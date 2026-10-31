# gridflow

A GCP-native data platform for electricity grid demand and smart meter telemetry, built with Apache Beam, BigQuery, Pub/Sub, Cloud Composer and dbt.

Two feeds, two shapes. National grid demand arrives as half-hourly settlement files and runs through a Beam batch pipeline. Smart meter readings arrive continuously over Pub/Sub and run through a Beam streaming pipeline that windows and differences cumulative registers into actual consumption. Both land in partitioned, clustered BigQuery tables that dbt turns into a tested warehouse.

The whole thing runs offline on the Beam DirectRunner and DuckDB, so you can build and inspect it with no GCP project and no credentials:

```bash
pip install -r requirements.txt
make demo
```

That generates the source feeds, runs both Beam pipelines, builds the models and runs the tests.

```
  Regional demand

  REGION  DAYS   AVG MWh/d   PEAK MW   RENEW %   ₹/MWh    SPREAD MW
  --------------------------------------------------------------------
  MH      120    284,025     16,528    25.4      2,554    5,529
  TN      120    210,363     12,240    25.3      2,554    4,078
  GJ      120    191,490     11,132    25.3      2,555    3,731
  KA      120    172,592     10,010    25.3      2,553    3,344
  DL      120    122,019     7,074     25.3      2,553    2,355

  Meter consumption by tariff

  TARIFF        METERS   TOTAL kWh     SPARSE   VOLT EVT  DUPES
  ------------------------------------------------------------------
  commercial    125      891,483       0        5754      2334
  domestic      206      840,525       0        9626      3708
  industrial    69       628,417       0        3180      1398
```

## Architecture

```
  BATCH                                      STREAMING

  grid demand CSV                            smart meters
        │                                          │
        ▼                                          ▼
  ┌─────────────┐                            ┌──────────┐
  │ Cloud       │                            │ Pub/Sub  │
  │ Storage     │                            └────┬─────┘
  └──────┬──────┘                                 │
         │                                        ▼
         ▼                                 ┌──────────────┐
  ┌──────────────┐                         │  Dataflow    │
  │  Dataflow    │                         │ (Beam)       │
  │  (Beam)      │                         │              │
  │              │                         │ fixed window │
  │ parse +      │                         │ + watermark  │
  │ validate     │                         │ + dedupe     │
  │ + derive     │                         │ + difference │
  └──────┬───────┘                         └──────┬───────┘
         │          dead-letter sink              │
         ▼                                        ▼
  ┌───────────────────────────────────────────────────────┐
  │                      BigQuery                         │
  │  fct_grid_demand          fct_meter_consumption       │
  │  PARTITION BY date        PARTITION BY window date    │
  │  CLUSTER BY region        CLUSTER BY meter_id         │
  └────────────────────────┬──────────────────────────────┘
                           │  dbt
                           ▼
              ┌────────────────────────┐
              │  staging → marts →     │
              │  agg_regional_daily    │
              └────────────────────────┘

  Cloud Composer runs the batch path daily and gates dbt on both feeds landing.
```

## What is in here

| Path | What it does |
| --- | --- |
| `ingestion/generate_grid_data.py` | Generates both source feeds, with the gaps and duplicates real telemetry has |
| `ingestion/publish_readings.py` | Publishes readings to Pub/Sub, emulator or real |
| `beam/demand_batch_pipeline.py` | Batch Beam: CSV to BigQuery, with a dead-letter path for bad rows |
| `beam/meter_stream_pipeline.py` | Streaming Beam: Pub/Sub to BigQuery, windowed, deduplicated, differenced |
| `bigquery/ddl/` | Partitioned and clustered native tables, external tables over GCS, a materialized view |
| `bigquery/migrate_cloudsql.py` | CloudSQL to BigQuery migration for the meter dimension |
| `dbt/models/` | Staging, two facts, a dimension and a gold regional aggregate |
| `dbt/tests/` | Three custom reconciliation tests |
| `composer/dags/gridflow_daily.py` | Cloud Composer DAG: land, Dataflow, dbt, test |

## The hard parts

**Meter readings are cumulative registers, not consumption.** A meter reports a running total, so usable numbers only come from differencing consecutive readings. That makes the pipeline stateful in a way a demand feed is not, and it makes duplicates dangerous: a duplicate that reaches the difference inflates a bill. The streaming pipeline deduplicates on `reading_id` before differencing and carries the dropped count into the output, so the duplicate rate stays observable rather than invisible. In the demo run above it catches about 7,400 of them.

**The window has to exceed the send interval.** Meters report four to twelve times a day. A sixty minute window usually contains a single reading per meter, which gives no interval to difference and silently produces nothing at all. The default window is six hours for exactly this reason. This was a real bug during development, found because the pipeline produced zero rows rather than wrong ones.

**A register that goes backwards is not negative consumption.** It is a meter reset or a replacement. The pipeline writes null and flags it, because zero would hide it.

**Partitioning is the whole cost model.** Grid demand is almost always queried as "this region, these dates". Partitioning by settlement date and clustering by region turns a full scan into a pruned one. `require_partition_filter` is set on the demand table so an unfiltered query fails rather than quietly billing for a full scan.

## Data quality

23 tests run on every build. Beyond uniqueness, not-null, referential integrity and accepted values, three custom tests catch what schema tests cannot:

**`assert_generation_balances_demand`** checks that the generation mix sums to demand within a megawatt. A persistent gap means a generation column is being dropped or double counted upstream, which is invisible in the headline demand figure but corrupts every renewable share number built on it.

**`assert_all_periods_present`** checks that every region has all 48 settlement periods for every completed day. A short day means the batch pipeline lost a file or ran before the feed finished publishing, and the daily aggregate then understates demand without any error appearing.

**`assert_consumption_within_sanctioned_load`** checks that no meter draws more than its connection is rated for, with a 25% margin. This one found a genuine bug: the data generator was producing industrial consumption above the physical capacity of the connection. The test was right and the generator was wrong.

## Running against real GCP

BigQuery's sandbox is free and needs no card, which is enough for everything except Dataflow.

```bash
cp .env.example .env            # set GCP_PROJECT, GCS_BUCKET
make bq-ddl | bq query --use_legacy_sql=false
DBT_TARGET=bigquery make warehouse test
```

For the streaming path with the emulator:

```bash
make pubsub                     # Pub/Sub emulator on :8085
make stream                     # publish readings into it
PUBSUB_EMULATOR_HOST=localhost:8085 python beam/meter_stream_pipeline.py \
    --subscription projects/gridflow-local/subscriptions/readings-sub \
    --output data/warehouse/consumption
```

## Notes on the design

**Bad rows are diverted, not dropped.** The batch pipeline uses Beam's tagged outputs to send unparseable rows to a dead-letter sink. Dropping them is how a pipeline reports success while quietly losing a region for a day.

**Schemas are declared, not inferred.** A reprocess of history has to produce identical types, and inference on CSV does not guarantee that.

**The dbt models run on both adapters from one file.** BigQuery wants `partition_by` as a dict and prefers `insert_overwrite`; DuckDB wants neither. Rather than maintaining two copies of each model, the config resolves on `target.type`.

**Avro, not CSV, for the CloudSQL migration.** CSV loses the distinction between an empty string and a null, and between a DATE and a string that looks like one, so a CSV round trip quietly changes the schema on the far side.

**The daily DAG gates dbt on both feeds.** dbt reads from the batch and the streaming tables, so starting it before either has landed produces a warehouse that is quietly a day stale in one half.

## How this maps to AWS

Included because the same design transfers, and the mapping is worth knowing.

| GCP | AWS |
| --- | --- |
| Cloud Storage | S3 |
| Pub/Sub | Kinesis or MSK |
| Dataflow (Beam) | Kinesis Data Analytics, or Spark on EMR |
| BigQuery | Redshift |
| BigQuery external tables | Redshift Spectrum, Glue Data Catalog |
| Cloud Composer | MWAA |
| CloudSQL | RDS, with DMS for the migration |

## License

MIT
