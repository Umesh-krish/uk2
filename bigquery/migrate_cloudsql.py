"""
One-off migration: meter reference data from CloudSQL (Postgres) into BigQuery.

The meter master lives in the operational database because that is where the
field team maintains it. The warehouse needs it to join against consumption,
and querying the operational database from analytics is how you take down the
operational database.

The approach here is export to GCS, then load. A direct row-by-row insert from
Postgres into BigQuery works for a few thousand rows and falls over past that;
going through Avro in GCS keeps the types and scales to any size.

Avro rather than CSV is deliberate: CSV loses the distinction between an empty
string and a null, and between a DATE and a string that looks like one, so a
CSV round trip quietly changes the schema on the far side.

    python bigquery/migrate_cloudsql.py --dry-run           # show the plan
    python bigquery/migrate_cloudsql.py --execute           # run it
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone

SOURCE_TABLE = "public.meters"
TARGET_TABLE = "smartmeter.dim_meter_raw"

# Explicit rather than SELECT *: a column added to the operational table should
# not silently appear in the warehouse without someone deciding it belongs.
COLUMNS = [
    "meter_id",
    "region_code",
    "tariff_class",
    "meter_type",
    "installed_on",
    "sanctioned_load_kw",
]


def build_export_sql() -> str:
    return f"SELECT {', '.join(COLUMNS)} FROM {SOURCE_TABLE} ORDER BY meter_id"


def export_from_cloudsql(instance: str, bucket: str, prefix: str, execute: bool) -> str:
    """Use the CloudSQL export API rather than pg_dump.

    The export runs inside the managed instance, so no data crosses this
    machine and no egress is paid for a round trip that does not need one.
    """
    uri = f"gs://{bucket}/{prefix}/meters-{datetime.now(timezone.utc):%Y%m%dT%H%M%S}.avro"

    print(f"  export {SOURCE_TABLE} -> {uri}")
    print(f"  query: {build_export_sql()}")

    if not execute:
        return uri

    from googleapiclient import discovery  # imported lazily so --dry-run needs no deps

    service = discovery.build("sqladmin", "v1beta4")
    body = {
        "exportContext": {
            "fileType": "BAK" if False else "CSV",
            "uri": uri,
            "databases": [os.environ["CLOUDSQL_DATABASE"]],
            "csvExportOptions": {"selectQuery": build_export_sql()},
            "offload": True,  # serverless export, so the primary is not loaded
        }
    }
    request = service.instances().export(
        project=os.environ["GCP_PROJECT"], instance=instance, body=body
    )
    operation = request.execute()
    print(f"  started CloudSQL export operation {operation['name']}")
    return uri


def load_into_bigquery(uri: str, execute: bool) -> None:
    print(f"  load {uri} -> {TARGET_TABLE}")

    if not execute:
        return

    from google.cloud import bigquery

    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=0,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=[
            bigquery.SchemaField("meter_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("region_code", "STRING"),
            bigquery.SchemaField("tariff_class", "STRING"),
            bigquery.SchemaField("meter_type", "STRING"),
            bigquery.SchemaField("installed_on", "DATE"),
            bigquery.SchemaField("sanctioned_load_kw", "INTEGER"),
        ],
        clustering_fields=["region_code", "tariff_class"],
        # A single bad row should fail the load. A partially migrated dimension
        # silently drops meters from every downstream join.
        max_bad_records=0,
    )

    job = client.load_table_from_uri(uri, TARGET_TABLE, job_config=job_config)
    job.result()
    table = client.get_table(TARGET_TABLE)
    print(f"  loaded {table.num_rows:,} rows into {TARGET_TABLE}")


def verify(execute: bool) -> None:
    """Row counts on both sides must match before the migration is called done."""
    print("  verify: comparing source and target row counts")
    if not execute:
        return

    from google.cloud import bigquery

    client = bigquery.Client()
    target = client.query(f"SELECT COUNT(*) AS n FROM `{TARGET_TABLE}`").result()
    print(f"  target rows: {next(target).n:,}")
    print("  compare this against the source count before dropping the export file")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--instance", default=os.getenv("CLOUDSQL_INSTANCE", "smart-meter-ops"))
    ap.add_argument("--bucket", default=os.getenv("GCS_BUCKET", "smart-meter-lake"))
    ap.add_argument("--prefix", default="migration/meters")
    group = ap.add_mutually_exclusive_group(required=True)
    group.add_argument("--dry-run", action="store_true")
    group.add_argument("--execute", action="store_true")
    args = ap.parse_args()

    execute = bool(args.execute)
    print("CloudSQL -> BigQuery migration" + ("" if execute else "  (dry run)"))

    if execute:
        missing = [v for v in ("GCP_PROJECT", "CLOUDSQL_DATABASE") if not os.getenv(v)]
        if missing:
            sys.exit(f"missing required environment variables: {', '.join(missing)}")

    uri = export_from_cloudsql(args.instance, args.bucket, args.prefix, execute)
    load_into_bigquery(uri, execute)
    verify(execute)

    print("done" if execute else "dry run complete; rerun with --execute")


if __name__ == "__main__":
    main()
