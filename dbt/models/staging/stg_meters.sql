-- Meter reference data.

select
    meter_id,
    region_code,
    tariff_class,
    meter_type,
    cast(installed_on as date)          as installed_on,
    cast(sanctioned_load_kw as integer) as sanctioned_load_kw

from {{ source('raw', 'meters') }}
where meter_id is not null
