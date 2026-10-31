.DEFAULT_GOAL := help
export METER_DATA := $(CURDIR)/data

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

install:  ## Install the Python dependencies
	pip install -r requirements.txt

seed:  ## Generate grid demand, meters and meter readings
	python ingestion/generate_grid_data.py
	@python -c "import csv, json, pathlib; \
rows=list(csv.DictReader(open('data/landing/readings/dt=2026-05-01/readings.csv'))); \
open('data/landing/readings/readings.jsonl','w').write(''.join(json.dumps(r)+chr(10) for r in rows)); \
print(f'readings.jsonl -> {len(rows):,} rows')"

beam:  ## Run both Beam pipelines on the DirectRunner
	@mkdir -p "$(METER_DATA)/warehouse"
	python beam/demand_batch_pipeline.py \
		--input data/landing/demand/dt=2026-05-01/demand.csv \
		--output data/warehouse/demand.jsonl
	python beam/meter_stream_pipeline.py \
		--input-file data/landing/readings/readings.jsonl \
		--output data/warehouse/consumption

warehouse:  ## Run the dbt models
	@mkdir -p "$(METER_DATA)/warehouse"
	cd dbt && dbt run --profiles-dir . --target $${DBT_TARGET:-dev}

test:  ## Run the dbt test suite
	cd dbt && dbt test --profiles-dir . --target $${DBT_TARGET:-dev}

query:  ## Print the regional summary from the gold layer
	@python scripts/show_grid_summary.py

demo: seed beam warehouse test query  ## Full offline run, start to finish
	@echo "\nPipeline complete."

pubsub:  ## Start the Pub/Sub emulator for the streaming path
	docker compose up -d pubsub

stream:  ## Publish readings into the Pub/Sub emulator
	python ingestion/publish_readings.py --limit 20000

bq-ddl:  ## Print the BigQuery DDL with your project and bucket substituted
	@sed -e "s/\$${PROJECT}/$${GCP_PROJECT:-your-project}/g" \
	     -e "s/\$${BUCKET}/$${GCS_BUCKET:-your-bucket}/g" bigquery/ddl/*.sql

clean:  ## Remove generated data
	rm -rf data dbt/target dbt/logs

.PHONY: help install seed beam warehouse test query demo pubsub stream bq-ddl clean
