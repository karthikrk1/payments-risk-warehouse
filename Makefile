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

.DEFAULT_GOAL := help
.PHONY: help setup data entities load ingest deps build docs lint format clean sample ci-build results

help: ## Show this help
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "}; {printf "  %-10s %s\n", $$1, $$2}'

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

lint: ## Ruff lint and format check (same as CI)
	$(RUFF) check .
	$(RUFF) format --check .

format: ## Fix lint issues and format Python code
	$(RUFF) check --fix .
	$(RUFF) format .

clean: ## Delete generated data, the warehouse file and dbt build output
	rm -rf data/raw/generated $(CI_GENERATED) warehouse/*.duckdb warehouse/*.duckdb.wal \
		risk_warehouse/target risk_warehouse/logs
