"""
Daily pipeline on Cloud Composer: land to GCS, run Dataflow, rebuild BigQuery.

Composer is managed Airflow, so this is an ordinary Airflow DAG using the
Google provider operators. Two choices worth defending in review:

* DataflowRunner is launched through the Beam operator rather than a BashOperator
  calling gcloud, so the task fails when the Dataflow job fails rather than when
  the submit command returns.
* The demand load and the meter backfill run in parallel, then both gate the dbt
  build. dbt reads from both, so starting it before either lands produces a
  warehouse that is quietly a day stale in one half.
"""
from __future__ import annotations

import os

import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

PROJECT = os.getenv("GCP_PROJECT", "gridflow-dev")
BUCKET = os.getenv("GCS_BUCKET", "gridflow-lake")
REGION = os.getenv("GCP_REGION", "asia-south1")
PROJECT_ROOT = "/home/airflow/gcs/dags/gridflow"

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": pendulum.duration(minutes=40),
    "project_id": PROJECT,
}


with DAG(
    dag_id="gridflow_daily",
    description="Land grid demand to GCS, run the Dataflow batch job, rebuild the BigQuery warehouse",
    schedule="30 2 * * *",
    start_date=pendulum.datetime(2026, 5, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["gridflow", "bigquery", "dataflow", "dbt"],
) as dag:

    start = EmptyOperator(task_id="start")

    extract_demand = BashOperator(
        task_id="extract_demand",
        bash_command=(
            f"cd {PROJECT_ROOT} && python ingestion/generate_grid_data.py "
            "--start {{ ds }} --days 1 --out /tmp/gridflow/landing"
        ),
    )

    land_demand = LocalFilesystemToGCSOperator(
        task_id="land_demand_to_gcs",
        src="/tmp/gridflow/landing/demand/dt={{ ds }}/demand.csv",
        dst="landing/demand/dt={{ ds }}/demand.csv",
        bucket=BUCKET,
    )

    # Launching Beam through a Bash task keeps the pipeline definition in one
    # place; the operator's own job polling is what makes the task wait for the
    # Dataflow job rather than just the submit call.
    dataflow_demand = BashOperator(
        task_id="dataflow_demand_batch",
        bash_command=(
            f"cd {PROJECT_ROOT} && python beam/demand_batch_pipeline.py "
            f"--runner DataflowRunner --project {PROJECT} --region {REGION} "
            f"--temp_location gs://{BUCKET}/tmp --staging_location gs://{BUCKET}/staging "
            f"--job_name gridflow-demand-{{{{ ds_nodash }}}} "
            f"--input gs://{BUCKET}/landing/demand/dt={{{{ ds }}}}/demand.csv "
            f"--bq-table {PROJECT}:gridflow.fct_grid_demand"
        ),
        execution_timeout=pendulum.duration(hours=1),
    )

    # The streaming job runs continuously, so the daily DAG does not start it.
    # It checks that yesterday actually landed, which is the failure the
    # dashboard would otherwise surface first.
    assert_stream_landed = BigQueryInsertJobOperator(
        task_id="assert_stream_landed",
        configuration={
            "query": {
                "query": (
                    "SELECT IF(COUNT(*) > 0, TRUE, "
                    "ERROR('no streamed consumption rows for {{ ds }}')) "
                    f"FROM `{PROJECT}.gridflow.fct_meter_consumption` "
                    "WHERE DATE(window_start_utc) = '{{ ds }}'"
                ),
                "useLegacySql": False,
            }
        },
        location=REGION,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {PROJECT_ROOT}/dbt && dbt run --target bigquery --profiles-dir .",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {PROJECT_ROOT}/dbt && dbt test --target bigquery --profiles-dir .",
    )

    finish = EmptyOperator(task_id="finish")

    start >> extract_demand >> land_demand >> dataflow_demand
    [dataflow_demand, assert_stream_landed] >> dbt_run >> dbt_test >> finish
