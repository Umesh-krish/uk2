-- A materialized view for the one query the operations dashboard runs on every
-- page load. BigQuery keeps it incrementally fresh, so the dashboard reads a
-- small precomputed table instead of rescanning partitions all day.
--
-- Materialized views carry real restrictions: no window functions, no joins to
-- non-materialized sources, and the base table cannot be dropped underneath
-- them. Anything richer belongs in a dbt model that runs on a schedule.

CREATE MATERIALIZED VIEW IF NOT EXISTS `${PROJECT}.gridflow.mv_daily_regional_demand`
PARTITION BY settlement_date
CLUSTER BY region_code
AS
SELECT
  settlement_date,
  region_code,
  SUM(demand_mw)    / 2 AS demand_mwh,
  SUM(renewable_mw) / 2 AS renewable_mwh,
  MAX(demand_mw)        AS peak_demand_mw,
  AVG(price_inr_per_mwh) AS avg_price_inr_per_mwh,
  COUNT(*)              AS settlement_periods
FROM `${PROJECT}.gridflow.fct_grid_demand`
GROUP BY settlement_date, region_code;
