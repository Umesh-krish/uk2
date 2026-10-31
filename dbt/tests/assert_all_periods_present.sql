-- A complete day has 48 settlement periods for every region. A short day means
-- the batch pipeline lost a file or ran before the feed finished publishing,
-- and the daily aggregate silently understates demand.
--
-- The most recent date is excluded: it may still be filling.

with per_day as (

    select
        settlement_date,
        region_code,
        count(distinct settlement_period) as periods

    from {{ ref('fct_grid_demand') }}
    group by settlement_date, region_code

)

select *
from per_day
where periods <> 48
  and settlement_date < (select max(settlement_date) from {{ ref('fct_grid_demand') }})
