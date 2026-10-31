-- Consumption fact, one row per meter per window, joined to the meter dimension.
--
-- Windows built from too few readings are kept but flagged. Dropping them would
-- understate consumption for exactly the meters with the worst connectivity,
-- which is the opposite of what a billing system should do.

{{ config(
    materialized='incremental',
    unique_key=['meter_id', 'window_start_utc'],
    incremental_strategy='insert_overwrite' if target.type == 'bigquery' else 'delete+insert',
    partition_by={'field': 'window_date', 'data_type': 'date'} if target.type == 'bigquery' else none,
    cluster_by=['meter_id'] if target.type == 'bigquery' else none
) }}

with consumption as (

    select * from {{ ref('stg_meter_consumption') }}

    {% if is_incremental() %}
      where window_start_utc >= (select coalesce(max(window_start_utc), '1900-01-01') from {{ this }})
    {% endif %}

),

meters as (

    select * from {{ ref('dim_meter') }}

),

final as (

    select
        c.meter_id,
        c.window_start_utc,
        c.window_end_utc,
        cast(c.window_start_utc as date)  as window_date,

        m.region_code,
        m.tariff_class,
        m.load_band,
        m.sanctioned_load_kw,

        c.consumption_kwh,
        c.first_register_kwh,
        c.last_register_kwh,
        c.reading_count,
        c.duplicates_dropped,

        c.min_voltage_v,
        c.max_voltage_v,
        c.has_poor_signal,

        c.reading_count < {{ var('min_readings_per_window') }} as is_sparse_window,
        c.consumption_kwh is null                              as is_register_rollover,

        -- Indian LV supply is nominally 230V with a 6% tolerance band.
        c.min_voltage_v < 216.2 or c.max_voltage_v > 243.8     as is_voltage_excursion,

        case
            when c.consumption_kwh is null then null
            else round(
                c.consumption_kwh
                / nullif(date_diff('minute', c.window_start_utc, c.window_end_utc) / 60.0, 0),
                3)
        end                                                    as avg_load_kw

    from consumption c
    left join meters m on c.meter_id = m.meter_id

)

select * from final
