-- Demand fact, partitioned by settlement date and clustered by region.
--
-- The partition_by and cluster_by configs below are what make this model
-- produce a BigQuery table matching bigquery/ddl/02_native_tables.sql. On the
-- DuckDB dev target they are ignored, which is why both targets can share one
-- model file.

{#
  Adapters disagree on the partition and strategy settings below, so they are
  resolved per target rather than duplicated into two model files. BigQuery
  takes a partition spec as a dict and prefers insert_overwrite, which replaces
  whole partitions atomically; DuckDB takes neither and uses delete+insert.
#}

{{ config(
    materialized='incremental',
    unique_key=['settlement_date', 'settlement_period', 'region_code'],
    incremental_strategy='insert_overwrite' if target.type == 'bigquery' else 'delete+insert',
    partition_by={'field': 'settlement_date', 'data_type': 'date'} if target.type == 'bigquery' else none,
    cluster_by=['region_code'] if target.type == 'bigquery' else none
) }}

with demand as (

    select * from {{ ref('stg_grid_demand') }}

    {% if is_incremental() %}
      where settlement_date >= (select coalesce(max(settlement_date), '1900-01-01') from {{ this }})
    {% endif %}

),

final as (

    select
        settlement_date,
        settlement_period,
        interval_start_utc,
        region_code,

        demand_mw,
        demand_mw / 2                               as demand_mwh,
        coal_mw,
        solar_mw,
        wind_mw,
        hydro_mw,
        nuclear_mw,
        gas_mw,
        renewable_mw,
        renewable_share,
        generation_balance_mw,
        price_inr_per_mwh,

        -- Thermal is what has to be dispatched when renewables fall short, so
        -- it is the number the operations desk watches, not total demand.
        coal_mw + gas_mw                            as thermal_mw,

        case
            when settlement_period between 13 and 20 then 'morning_peak'
            when settlement_period between 37 and 44 then 'evening_peak'
            when settlement_period between 1 and 12  then 'overnight'
            else 'daytime'
        end                                         as period_band,

        extract(dow from settlement_date) in (0, 6) as is_weekend

    from demand

)

select * from final
