"""
Publish meter readings into Pub/Sub, so the streaming pipeline has a live feed.

Works against the Pub/Sub emulator locally (set PUBSUB_EMULATOR_HOST) and
against real Pub/Sub when that variable is unset and credentials are present.
The client library picks between them on that variable alone, which is why no
flag here does it.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from pathlib import Path


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--readings", default="data/landing/readings/dt=2026-05-01/readings.csv")
    ap.add_argument("--project", default=os.getenv("PUBSUB_PROJECT_ID", "gridflow-local"))
    ap.add_argument("--topic", default=os.getenv("PUBSUB_TOPIC", "meter-readings"))
    ap.add_argument("--limit", type=int, default=20000)
    ap.add_argument("--rate", type=int, default=2000, help="Messages per second")
    ap.add_argument("--dry-run", action="store_true", help="Print instead of publishing")
    args = ap.parse_args()

    path = Path(args.readings)
    if not path.exists():
        sys.exit(f"{path} not found; run `make seed` first")

    with path.open(newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))[: args.limit]

    publisher = None
    topic_path = None
    if not args.dry_run:
        try:
            from google.cloud import pubsub_v1
        except ImportError:
            sys.exit("google-cloud-pubsub is not installed; run `pip install google-cloud-pubsub` or pass --dry-run")

        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(args.project, args.topic)
        try:
            publisher.create_topic(request={"name": topic_path})
            print(f"created topic {topic_path}")
        except Exception:
            pass  # already exists

    print(f"publishing {len(rows):,} readings to {args.topic} at ~{args.rate}/s")

    sent, started = 0, time.monotonic()
    futures = []
    for row in rows:
        payload = json.dumps(
            {
                "reading_id": row["reading_id"],
                "meter_id": row["meter_id"],
                "reading_ts_utc": row["reading_ts_utc"],
                "cumulative_kwh": float(row["cumulative_kwh"]),
                "voltage_v": float(row["voltage_v"]),
                "signal_quality": row["signal_quality"],
            }
        ).encode()

        if args.dry_run:
            if sent < 3:
                print(payload.decode())
        else:
            # Ordering key keeps one meter's readings in sequence, which matters
            # because consumption is differenced between consecutive registers.
            futures.append(publisher.publish(topic_path, payload, meter_id=row["meter_id"]))

        sent += 1
        if sent % args.rate == 0:
            elapsed = time.monotonic() - started
            if elapsed < sent / args.rate:
                time.sleep(sent / args.rate - elapsed)

    if publisher:
        for f in futures:
            f.result()

    print(f"published {sent:,} readings in {time.monotonic() - started:.1f}s")


if __name__ == "__main__":
    main()
