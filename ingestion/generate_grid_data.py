"""
Generate the two source feeds this project is built around.

1. National half-hourly grid demand: the batch feed. Two years of settlement
   periods with the daily, weekly and seasonal shape real demand has, plus a
   generation mix per period.

2. Smart meter readings: the streaming feed. Per-meter cumulative kWh registers
   at irregular intervals, which is what meter telemetry actually looks like,
   including the gaps and occasional duplicate sends.

In production these come from a grid operator API and a meter data collector.
This generator exists so the project runs offline, with no GCP project and no
credentials.

Writes:
    data/landing/demand/dt=<date>/demand.csv
    data/landing/meters/meters.csv
    data/landing/readings/dt=<date>/readings.csv
"""
from __future__ import annotations

import argparse
import csv
import math
import random
from datetime import date, datetime, timedelta
from pathlib import Path

REGIONS = [
    ("TN", "Tamil Nadu", 1.00),
    ("KA", "Karnataka", 0.82),
    ("MH", "Maharashtra", 1.35),
    ("GJ", "Gujarat", 0.91),
    ("DL", "Delhi", 0.58),
]

TARIFFS = ["domestic", "domestic", "domestic", "commercial", "commercial", "industrial"]
METER_TYPES = ["single_phase", "single_phase", "single_phase", "three_phase"]

# Generation sources and roughly what share of the mix each carries.
SOURCES = ["coal", "solar", "wind", "hydro", "nuclear", "gas"]


def demand_shape(ts: datetime) -> float:
    """Multiplier on base load: daily double peak, weekday lift, summer lift."""
    hour = ts.hour + ts.minute / 60.0

    # Morning peak around 07:00, evening peak around 19:30, trough at 04:00.
    daily = (
        0.78
        + 0.17 * math.exp(-((hour - 7.0) ** 2) / 6.0)
        + 0.30 * math.exp(-((hour - 19.5) ** 2) / 7.0)
        - 0.10 * math.exp(-((hour - 4.0) ** 2) / 8.0)
    )

    weekday = 1.0 if ts.weekday() < 5 else 0.91

    # Peak demand in the Indian summer, trough in the monsoon months.
    seasonal = 1.0 + 0.16 * math.sin((ts.timetuple().tm_yday - 60) / 365.0 * 2 * math.pi)

    return daily * weekday * seasonal


def solar_share(ts: datetime) -> float:
    """Solar contributes only in daylight, peaking at solar noon."""
    hour = ts.hour + ts.minute / 60.0
    if hour < 6.2 or hour > 18.3:
        return 0.0
    return max(0.0, math.sin((hour - 6.2) / 12.1 * math.pi)) * 0.27


def generate_demand(start: date, days: int, rng: random.Random):
    rows = []
    for day_offset in range(days):
        day = start + timedelta(days=day_offset)
        for period in range(48):  # 48 half-hourly settlement periods
            ts = datetime.combine(day, datetime.min.time()) + timedelta(minutes=30 * period)

            for code, _name, weight in REGIONS:
                base_mw = 9500 * weight
                demand = base_mw * demand_shape(ts) * rng.uniform(0.97, 1.03)

                solar = demand * solar_share(ts) * rng.uniform(0.8, 1.2)
                wind = demand * rng.uniform(0.04, 0.14)
                hydro = demand * rng.uniform(0.05, 0.11)
                nuclear = demand * rng.uniform(0.02, 0.05)
                gas = demand * rng.uniform(0.03, 0.09)
                coal = max(0.0, demand - (solar + wind + hydro + nuclear + gas))

                rows.append(
                    {
                        "settlement_date": day.isoformat(),
                        "settlement_period": period + 1,
                        "interval_start_utc": ts.isoformat(timespec="seconds"),
                        "region_code": code,
                        "demand_mw": round(demand, 1),
                        "coal_mw": round(coal, 1),
                        "solar_mw": round(solar, 1),
                        "wind_mw": round(wind, 1),
                        "hydro_mw": round(hydro, 1),
                        "nuclear_mw": round(nuclear, 1),
                        "gas_mw": round(gas, 1),
                        # Price rises steeply when renewables cannot cover the peak.
                        "price_inr_per_mwh": round(
                            2400 + 3100 * max(0.0, (demand - solar - wind) / demand - 0.72) ** 1.5
                            + rng.uniform(-90, 90),
                            2,
                        ),
                    }
                )
    return rows


def generate_meters(count: int, rng: random.Random):
    rows = []
    for i in range(count):
        code, _name, _w = rng.choice(REGIONS)
        rows.append(
            {
                "meter_id": f"MTR{i:06d}",
                "region_code": code,
                "tariff_class": rng.choice(TARIFFS),
                "meter_type": rng.choice(METER_TYPES),
                "installed_on": (date(2021, 1, 1) + timedelta(days=rng.randint(0, 1400))).isoformat(),
                "sanctioned_load_kw": rng.choice([3, 5, 5, 8, 10, 15, 25, 50]),
            }
        )
    return rows


def generate_readings(meters: list[dict], start: date, days: int, rng: random.Random):
    """Cumulative kWh registers, reported at irregular intervals with gaps."""
    rows = []
    registers = {m["meter_id"]: rng.uniform(1000, 48000) for m in meters}

    # Utilisation factor: what fraction of the sanctioned load a connection
    # actually draws on average. Deriving consumption from this rather than from
    # a free-standing multiplier keeps it physically below the connection's
    # rating, which is what the dbt sanctioned-load test asserts.
    utilisation = {"domestic": (0.10, 0.28), "commercial": (0.22, 0.48), "industrial": (0.35, 0.65)}

    for day_offset in range(days):
        day = start + timedelta(days=day_offset)
        for meter in meters:
            # Roughly 6% of meter-days are missing entirely: comms failures are
            # normal in meter telemetry and the pipeline has to tolerate them.
            if rng.random() < 0.06:
                continue

            lo, hi = utilisation[meter["tariff_class"]]
            load = meter["sanctioned_load_kw"] * rng.uniform(lo, hi)
            sends = rng.choice([4, 6, 6, 8, 12])

            for s in range(sends):
                ts = datetime.combine(day, datetime.min.time()) + timedelta(
                    minutes=int(1440 / sends * s) + rng.randint(0, 9)
                )
                consumed = load * (24.0 / sends) * (demand_shape(ts) / 1.3) * rng.uniform(0.75, 1.25)
                registers[meter["meter_id"]] += consumed

                row = {
                    "reading_id": f"{meter['meter_id']}-{day.isoformat()}-{s}",
                    "meter_id": meter["meter_id"],
                    "reading_ts_utc": ts.isoformat(timespec="seconds"),
                    "cumulative_kwh": round(registers[meter["meter_id"]], 3),
                    "voltage_v": round(rng.gauss(233, 6), 1),
                    "signal_quality": rng.choice(["good", "good", "good", "fair", "poor"]),
                }
                rows.append(row)

                # About 1.5% of sends arrive twice. The streaming pipeline is
                # expected to deduplicate on reading_id rather than double-count.
                if rng.random() < 0.015:
                    rows.append(dict(row))

    return rows


def write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--days", type=int, default=120)
    ap.add_argument("--meters", type=int, default=400)
    ap.add_argument("--start", default="2026-05-01")
    ap.add_argument("--out", default="data/landing")
    ap.add_argument("--seed", type=int, default=11)
    args = ap.parse_args()

    rng = random.Random(args.seed)
    out = Path(args.out)
    start = date.fromisoformat(args.start)

    demand = generate_demand(start, args.days, rng)
    write_csv(out / "demand" / f"dt={start.isoformat()}" / "demand.csv", demand)
    print(f"demand    -> {len(demand):>8,} rows ({args.days} days x 48 periods x {len(REGIONS)} regions)")

    meters = generate_meters(args.meters, rng)
    write_csv(out / "meters" / "meters.csv", meters)
    print(f"meters    -> {len(meters):>8,} rows")

    readings = generate_readings(meters, start, args.days, rng)
    write_csv(out / "readings" / f"dt={start.isoformat()}" / "readings.csv", readings)
    print(f"readings  -> {len(readings):>8,} rows")


if __name__ == "__main__":
    main()
