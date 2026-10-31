"""
Batch pipeline: grid demand CSV in Cloud Storage -> BigQuery.

Runs on the DirectRunner locally and on Dataflow in production; the only
difference is the --runner flag and the arguments Dataflow needs. Writing it
against Beam rather than a BigQuery load job buys the per-row validation and
derived columns below, which a bare load job cannot do.

Local:
    python beam/demand_batch_pipeline.py \
        --input data/landing/demand/dt=2026-05-01/demand.csv \
        --output data/warehouse/demand.jsonl

Dataflow:
    python beam/demand_batch_pipeline.py \
        --runner DataflowRunner --project $GCP_PROJECT --region asia-south1 \
        --temp_location gs://$BUCKET/tmp \
        --input gs://$BUCKET/landing/demand/dt=2026-05-01/demand.csv \
        --bq-table $GCP_PROJECT:gridflow.fct_grid_demand
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import logging

import apache_beam as beam
from apache_beam.metrics import Metrics
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

GENERATION_COLUMNS = ["coal_mw", "solar_mw", "wind_mw", "hydro_mw", "nuclear_mw", "gas_mw"]
RENEWABLE_COLUMNS = ["solar_mw", "wind_mw", "hydro_mw"]

# Declared rather than inferred, for the same reason the Spark jobs declare
# theirs: a reprocess of history must produce identical types.
BQ_SCHEMA = {
    "fields": [
        {"name": "settlement_date", "type": "DATE", "mode": "REQUIRED"},
        {"name": "settlement_period", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "interval_start_utc", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "region_code", "type": "STRING", "mode": "REQUIRED"},
        {"name": "demand_mw", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "coal_mw", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "solar_mw", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "wind_mw", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "hydro_mw", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "nuclear_mw", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "gas_mw", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "price_inr_per_mwh", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "renewable_mw", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "renewable_share", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "generation_balance_mw", "type": "FLOAT", "mode": "NULLABLE"},
    ]
}


class ParseDemandRow(beam.DoFn):
    """CSV line to a typed dict, with malformed rows diverted rather than dropped.

    Beam's multi-output lets the bad rows go to their own sink. Silently
    dropping them is how a pipeline reports success while quietly losing a
    region for a day.
    """

    DEAD_LETTER = "dead_letter"

    def __init__(self, header: list[str]):
        self.header = header
        self.parsed = Metrics.counter(self.__class__, "rows_parsed")
        self.rejected = Metrics.counter(self.__class__, "rows_rejected")

    def process(self, line: str):
        try:
            values = next(csv.reader(io.StringIO(line)))
            row = dict(zip(self.header, values))

            out = {
                "settlement_date": row["settlement_date"],
                "settlement_period": int(row["settlement_period"]),
                "interval_start_utc": row["interval_start_utc"],
                "region_code": row["region_code"],
            }
            for col in ["demand_mw", "price_inr_per_mwh", *GENERATION_COLUMNS]:
                out[col] = float(row[col])

            if not 1 <= out["settlement_period"] <= 48:
                raise ValueError(f"settlement_period {out['settlement_period']} outside 1-48")
            if out["demand_mw"] <= 0:
                raise ValueError(f"non-positive demand {out['demand_mw']}")

            self.parsed.inc()
            yield out

        except Exception as exc:
            self.rejected.inc()
            yield beam.pvalue.TaggedOutput(
                self.DEAD_LETTER, json.dumps({"raw": line, "error": str(exc)})
            )


class AddDerivedColumns(beam.DoFn):
    """Columns every downstream query would otherwise recompute by hand."""

    def process(self, row: dict):
        renewable = sum(row[c] for c in RENEWABLE_COLUMNS)
        generated = sum(row[c] for c in GENERATION_COLUMNS)

        row["renewable_mw"] = round(renewable, 1)
        row["renewable_share"] = round(renewable / row["demand_mw"], 4) if row["demand_mw"] else None
        # Generation minus demand. Persistent drift here means a feed is wrong,
        # which is what the dbt reconciliation test checks for.
        row["generation_balance_mw"] = round(generated - row["demand_mw"], 1)
        yield row


def run(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", help="Local JSONL sink; omit when writing to BigQuery")
    ap.add_argument("--bq-table", help="project:dataset.table")
    ap.add_argument("--dead-letter", default="data/warehouse/demand_rejected.jsonl")
    known, pipeline_argv = ap.parse_known_args(argv)

    options = PipelineOptions(pipeline_argv)
    options.view_as(SetupOptions).save_main_session = True

    with open(known.input, encoding="utf-8") as fh:
        header = next(csv.reader(fh))

    with beam.Pipeline(options=options) as p:
        parsed = (
            p
            | "ReadCsv" >> beam.io.ReadFromText(known.input, skip_header_lines=1)
            | "Parse" >> beam.ParDo(ParseDemandRow(header)).with_outputs(
                ParseDemandRow.DEAD_LETTER, main="rows"
            )
        )

        rows = parsed.rows | "Derive" >> beam.ParDo(AddDerivedColumns())

        if known.bq_table:
            rows | "WriteBigQuery" >> beam.io.WriteToBigQuery(
                known.bq_table,
                schema=BQ_SCHEMA,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                # Partitioning and clustering are declared here so a table
                # created by the pipeline matches the DDL in bigquery/ddl/.
                additional_bq_parameters={
                    "timePartitioning": {"type": "DAY", "field": "settlement_date"},
                    "clustering": {"fields": ["region_code"]},
                },
            )
        else:
            rows | "ToJson" >> beam.Map(json.dumps) | "WriteLocal" >> beam.io.WriteToText(
                known.output, shard_name_template=""
            )

        getattr(parsed, ParseDemandRow.DEAD_LETTER) | "WriteRejected" >> beam.io.WriteToText(
            known.dead_letter, shard_name_template=""
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.WARNING)
    run()
