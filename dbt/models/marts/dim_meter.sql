-- Meter dimension, clustered on the columns most queries filter by.

{{ config(
    materialized='table',
    cluster_by=['region_code', 'tariff_class'] if target.type == 'bigquery' else none
) }}

with meters as (

    select * from {{ ref('stg_meters') }}

),

final as (

    select
        meter_id,
        region_code,
        tariff_class,
        meter_type,
        installed_on,
        sanctioned_load_kw,

        case
            when sanctioned_load_kw <= 5  then 'small'
            when sanctioned_load_kw <= 15 then 'medium'
            else 'large'
        end as load_band,

        date_diff('day', installed_on, current_date) as days_since_install

    from meters

)

select * from final
