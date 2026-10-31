-- Typed consumption windows, one per meter per window.

with source as (

    select * from {{ source('raw', 'meter_consumption') }}

),

renamed as (

    select
        meter_id,
        cast(window_start_utc as timestamp) as window_start_utc,
        cast(window_end_utc as timestamp)   as window_end_utc,
        cast(consumption_kwh as double)     as consumption_kwh,
        cast(first_register_kwh as double)  as first_register_kwh,
        cast(last_register_kwh as double)   as last_register_kwh,
        cast(reading_count as integer)      as reading_count,
        cast(duplicates_dropped as integer) as duplicates_dropped,
        cast(min_voltage_v as double)       as min_voltage_v,
        cast(max_voltage_v as double)       as max_voltage_v,
        cast(has_poor_signal as boolean)    as has_poor_signal

    from source
    where meter_id is not null

)

select * from renamed
