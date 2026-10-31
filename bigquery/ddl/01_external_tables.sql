-- External tables over Cloud Storage. These are the lake's read surface:
-- BigQuery queries the Parquet and CSV in GCS without loading it, so raw data
-- stays in one place and is never duplicated into native storage just to be
-- inspected.
--
-- Cost note: external tables cannot be partitioned by BigQuery's own
-- partitioning, so every query over them is a full scan. They are for
-- inspection and for the first hop only; anything queried repeatedly is
-- materialised into the native partitioned tables in 02_native_tables.sql.

CREATE SCHEMA IF NOT EXISTS `${PROJECT}.smartmeter_raw`
  OPTIONS (location = 'asia-south1');

CREATE OR REPLACE EXTERNAL TABLE `${PROJECT}.smartmeter_raw.ext_demand`
OPTIONS (
  format = 'CSV',
  skip_leading_rows = 1,
  uris = ['gs://${BUCKET}/landing/demand/*/demand.csv']
);

CREATE OR REPLACE EXTERNAL TABLE `${PROJECT}.smartmeter_raw.ext_readings`
OPTIONS (
  format = 'CSV',
  skip_leading_rows = 1,
  uris = ['gs://${BUCKET}/landing/readings/*/readings.csv']
);

CREATE OR REPLACE EXTERNAL TABLE `${PROJECT}.smartmeter_raw.ext_meters`
OPTIONS (
  format = 'CSV',
  skip_leading_rows = 1,
  uris = ['gs://${BUCKET}/landing/meters/meters.csv']
);
