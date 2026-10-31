-- Native tables: the data layer the warehouse actually serves from.
--
-- Partitioning and clustering are the whole point of this file. Grid demand is
-- almost always queried as "this region, this date range", so partitioning by
-- settlement_date prunes to the days asked for and clustering by region_code
-- prunes within them. On two years of half-hourly data across five regions
-- that is the difference between scanning megabytes and scanning the lot.
--
-- require_partition_filter is deliberate: it makes an unfiltered full-table
-- scan fail rather than quietly bill for it. In a sandbox that is a guardrail;
-- on a real project it is a cost control.

CREATE SCHEMA IF NOT EXISTS `${PROJECT}.gridflow`
  OPTIONS (location = 'asia-south1');

CREATE TABLE IF NOT EXISTS `${PROJECT}.gridflow.fct_grid_demand`
(
  settlement_date       DATE      NOT NULL,
  settlement_period     INT64     NOT NULL,
  interval_start_utc    TIMESTAMP NOT NULL,
  region_code           STRING    NOT NULL,
  demand_mw             FLOAT64,
  coal_mw               FLOAT64,
  solar_mw              FLOAT64,
  wind_mw               FLOAT64,
  hydro_mw              FLOAT64,
  nuclear_mw            FLOAT64,
  gas_mw                FLOAT64,
  price_inr_per_mwh     FLOAT64,
  renewable_mw          FLOAT64,
  renewable_share       FLOAT64,
  generation_balance_mw FLOAT64
)
PARTITION BY settlement_date
CLUSTER BY region_code
OPTIONS (
  require_partition_filter = TRUE,
  description = 'Half-hourly grid demand and generation mix by region. Written by beam/demand_batch_pipeline.py.'
);

CREATE TABLE IF NOT EXISTS `${PROJECT}.gridflow.fct_meter_consumption`
(
  meter_id           STRING    NOT NULL,
  window_start_utc   TIMESTAMP NOT NULL,
  window_end_utc     TIMESTAMP NOT NULL,
  first_register_kwh FLOAT64,
  last_register_kwh  FLOAT64,
  consumption_kwh    FLOAT64,
  reading_count      INT64,
  duplicates_dropped INT64,
  min_voltage_v      FLOAT64,
  max_voltage_v      FLOAT64,
  has_poor_signal    BOOL
)
PARTITION BY DATE(window_start_utc)
CLUSTER BY meter_id
OPTIONS (
  partition_expiration_days = 1095,
  description = 'Windowed consumption derived from cumulative meter registers. Written by beam/meter_stream_pipeline.py.'
);

CREATE TABLE IF NOT EXISTS `${PROJECT}.gridflow.dim_meter`
(
  meter_id           STRING NOT NULL,
  region_code        STRING,
  tariff_class       STRING,
  meter_type         STRING,
  installed_on       DATE,
  sanctioned_load_kw INT64
)
CLUSTER BY region_code, tariff_class
OPTIONS (description = 'Meter reference data, migrated from the operational CloudSQL instance.');
