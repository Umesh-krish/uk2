-- Gold aggregate: one row per region per day.
-- This is what the operations dashboard and the regulatory report both read.

with demand as (

    select * from {{ ref('fct_grid_demand') }}

),

daily as (

    select
        settlement_date,
        region_code,

        sum(demand_mwh)                                          as demand_mwh,
        sum(renewable_mw) / 2                                    as renewable_mwh,
        sum(thermal_mw) / 2                                      as thermal_mwh,
        sum(solar_mw) / 2                                        as solar_mwh,
        sum(wind_mw) / 2                                         as wind_mwh,

        max(demand_mw)                                           as peak_demand_mw,
        min(demand_mw)                                           as min_demand_mw,
        round(avg(price_inr_per_mwh), 2)                         as avg_price_inr_per_mwh,
        round(max(price_inr_per_mwh), 2)                         as max_price_inr_per_mwh,

        round(sum(renewable_mw) / nullif(sum(demand_mw), 0), 4)  as renewable_share,

        -- The spread between peak and trough is what storage and demand
        -- response are bought to flatten, so it is tracked as a headline number.
        round(max(demand_mw) - min(demand_mw), 1)                as peak_trough_spread_mw,

        max(case when period_band = 'evening_peak' then demand_mw end) as evening_peak_mw,
        count(*)                                                 as settlement_periods

    from demand
    group by settlement_date, region_code

),

with_trend as (

    select
        *,
        round(
            avg(demand_mwh) over (
                partition by region_code
                order by settlement_date
                rows between 6 preceding and current row
            ), 1
        ) as demand_mwh_7d_avg,

        round(
            demand_mwh - lag(demand_mwh, 7) over (
                partition by region_code order by settlement_date
            ), 1
        ) as demand_mwh_wow_change

    from daily

)

select * from with_trend
