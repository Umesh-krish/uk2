"""Print the gold-layer regional summary, as a check that the run worked."""
from __future__ import annotations

import os
import sys
from pathlib import Path

import duckdb

DB = Path(os.getenv("METER_DATA", "data")) / "warehouse" / "smart_meter.duckdb"


def main() -> None:
    if not DB.exists():
        sys.exit(f"{DB} not found; run `make warehouse` first")

    con = duckdb.connect(str(DB), read_only=True)
    try:
        rows = con.execute(
            """
            select
                region_code,
                count(*)                        as days,
                round(avg(demand_mwh), 0)       as avg_daily_mwh,
                round(max(peak_demand_mw), 0)   as peak_mw,
                round(avg(renewable_share) * 100, 1) as renewable_pct,
                round(avg(avg_price_inr_per_mwh), 0) as avg_price,
                round(avg(peak_trough_spread_mw), 0) as avg_spread_mw
            from agg_regional_daily
            group by region_code
            order by avg_daily_mwh desc
            """
        ).fetchall()

        meters = con.execute(
            """
            select
                tariff_class,
                count(distinct meter_id)                as meters,
                round(sum(consumption_kwh), 0)          as total_kwh,
                sum(case when is_sparse_window then 1 else 0 end)      as sparse_windows,
                sum(case when is_voltage_excursion then 1 else 0 end)  as voltage_events,
                sum(duplicates_dropped)                 as dupes_dropped
            from fct_meter_consumption
            group by tariff_class
            order by total_kwh desc
            """
        ).fetchall()
    finally:
        con.close()

    print("\n  Regional demand\n")
    print(f"  {'REGION':<8}{'DAYS':<7}{'AVG MWh/d':<12}{'PEAK MW':<10}{'RENEW %':<10}{'₹/MWh':<9}{'SPREAD MW'}")
    print("  " + "-" * 68)
    for r in rows:
        print(f"  {r[0]:<8}{r[1]:<7}{r[2]:<12,.0f}{r[3]:<10,.0f}{r[4]:<10}{r[5]:<9,.0f}{r[6]:,.0f}")

    print("\n  Meter consumption by tariff\n")
    print(f"  {'TARIFF':<14}{'METERS':<9}{'TOTAL kWh':<14}{'SPARSE':<9}{'VOLT EVT':<10}{'DUPES'}")
    print("  " + "-" * 66)
    for r in meters:
        print(f"  {r[0]:<14}{r[1]:<9}{r[2]:<14,.0f}{r[3]:<9}{r[4]:<10}{r[5]}")
    print()


if __name__ == "__main__":
    main()
