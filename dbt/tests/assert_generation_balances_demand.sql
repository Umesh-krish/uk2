-- Generation should sum to demand within rounding. A persistent gap means one
-- of the generation columns is being dropped or double counted upstream, which
-- is invisible in the headline demand number but corrupts every renewable
-- share figure built on it.
--
-- One MW of tolerance on a load of several thousand is rounding; more is a bug.

select
    settlement_date,
    region_code,
    settlement_period,
    demand_mw,
    generation_balance_mw

from {{ ref('fct_grid_demand') }}
where abs(generation_balance_mw) > 1.0
