-- Typed demand rows, one per region per settlement period.

with source as (

    select * from {{ source('raw', 'grid_demand') }}

),

renamed as (

    select
        cast(settlement_date as date)           as settlement_date,
        cast(settlement_period as integer)      as settlement_period,
        cast(interval_start_utc as timestamp)   as interval_start_utc,
        region_code,
        cast(demand_mw as double)               as demand_mw,
        cast(coal_mw as double)                 as coal_mw,
        cast(solar_mw as double)                as solar_mw,
        cast(wind_mw as double)                 as wind_mw,
        cast(hydro_mw as double)                as hydro_mw,
        cast(nuclear_mw as double)              as nuclear_mw,
        cast(gas_mw as double)                  as gas_mw,
        cast(price_inr_per_mwh as double)       as price_inr_per_mwh,
        cast(renewable_mw as double)            as renewable_mw,
        cast(renewable_share as double)         as renewable_share,
        cast(generation_balance_mw as double)   as generation_balance_mw

    from source
    where region_code is not null

)

select * from renamed
