# Common tasks. Run `make help` for the list.

VENV := .venv
PYTHON := $(VENV)/bin/python
RUFF := $(VENV)/bin/ruff
# dbt runs from risk_warehouse/, where profiles.yml lives.
DBT := cd risk_warehouse && ../$(VENV)/bin/dbt
PAYSIM_CSV := data/raw/paysim.csv
# The committed CI sample and where its pipeline writes (kept apart from the full dev build).
SAMPLE_CSV := data/sample/paysim_sample.csv
CI_GENERATED := data/raw/ci_generated
CI_WAREHOUSE := warehouse/ci.duckdb
# Prefect flows run locally with a temporary server; don't send anonymous usage statistics.
FLOW := PREFECT_SERVER_ANALYTICS_ENABLED=false $(PYTHON) flows/pipeline.py

.DEFAULT_GOAL := help
.PHONY: help setup data entities load ingest deps build docs lint format clean sample ci-build \
	results flow replay ci-replay monitoring-export monitoring-page triage triage-eval

help: ## Show this help
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "}; {printf "  %-17s %s\n", $$1, $$2}'

setup: ## Create .venv (Python 3.12) and install dependencies and dev tools
	uv venv --python 3.12
	uv pip install --python $(PYTHON) -r requirements-dev.txt

data: ## Download PaySim from Kaggle to data/raw/paysim.csv (needs a Kaggle API token)
	uv pip install --python $(PYTHON) kaggle six
	$(VENV)/bin/kaggle datasets download -d ealaxi/paysim1 -p data/raw --unzip
	mv data/raw/PS_*.csv $(PAYSIM_CSV)

entities: ## Generate synthetic accounts, devices, logins and disputes
	$(PYTHON) ingestion/generate_entities.py

load: ## Load PaySim and the generated entities into DuckDB's raw schema
	$(PYTHON) ingestion/load_raw.py

ingest: entities load ## Generate entities, then load everything into raw

deps: ## Install dbt packages
	$(DBT) deps

build: deps ## Run and test every dbt model
	$(DBT) build

docs: deps ## Generate and serve the dbt docs site (lineage, data dictionary)
	$(DBT) docs generate && ../$(VENV)/bin/dbt docs serve

results: ## Run the analyses on the full warehouse and print Markdown tables (README Results)
	$(PYTHON) scripts/run_analyses.py

monitoring-export: ## Export full-data monitoring results to docs/monitoring/monitoring.json (after build)
	$(PYTHON) scripts/export_monitoring.py

monitoring-page: ## Build the monitoring chart page locally from the committed export
	$(PYTHON) scripts/build_monitoring_page.py
	@echo "Open site/monitoring/index.html in a browser"

sample: ## Rebuild the committed CI sample from the full PaySim CSV
	$(PYTHON) ingestion/make_sample.py

ci-build: ## Full pipeline on the committed sample into warehouse/ci.duckdb (same as CI)
	$(PYTHON) ingestion/generate_entities.py --csv $(SAMPLE_CSV) --out $(CI_GENERATED)
	$(PYTHON) ingestion/load_raw.py --csv $(SAMPLE_CSV) --generated $(CI_GENERATED) \
		--warehouse $(CI_WAREHOUSE)
	$(DBT) deps
	$(DBT) build --target ci
	@# Check the analyses still compile and run; their output on the skewed sample is discarded.
	$(PYTHON) scripts/run_analyses.py --warehouse $(CI_WAREHOUSE) --target ci > /dev/null
	@# Re-check that the chart bands agree with every monitoring status, and that the page builds.
	$(PYTHON) scripts/export_monitoring.py --warehouse $(CI_WAREHOUSE) --out data/raw/ci_monitoring.json
	$(PYTHON) scripts/build_monitoring_page.py --data data/raw/ci_monitoring.json \
		--out site/ci-monitoring/index.html

flow: ## Full pipeline on the full data as a Prefect flow (generate, load, dbt build, analyses)
	$(FLOW) --mode full --dataset full

replay: ## Replay the full data day by day into warehouse/replay.duckdb; must equal `make build`
	$(FLOW) --mode replay --dataset full --batch-hours 24 --compare-to warehouse/risk.duckdb

ci-replay: ## Replay the sample week by week; must equal `make ci-build` (same as CI)
	$(FLOW) --mode replay --dataset sample --batch-hours 168 --compare-to $(CI_WAREHOUSE)

triage: ## Diagnose the last failed dbt build with Claude (read-only; needs ANTHROPIC_API_KEY)
	$(PYTHON) -m triage

triage-eval: ## Plant 9 known breakages and grade the triage agent on them (billed, about $1-2)
	$(PYTHON) -m triage.eval.run

lint: ## Ruff lint and format check (same as CI)
	$(RUFF) check .
	$(RUFF) format --check .

format: ## Fix lint issues and format Python code
	$(RUFF) check --fix .
	$(RUFF) format .

clean: ## Delete generated data, the warehouse file and dbt build output
	rm -rf data/raw/generated $(CI_GENERATED) warehouse/*.duckdb warehouse/*.duckdb.wal \
		risk_warehouse/target risk_warehouse/logs site data/raw/ci_monitoring.json
