# Common tasks. Run `make help` for the list.

VENV := .venv
PYTHON := $(VENV)/bin/python
RUFF := $(VENV)/bin/ruff
# dbt runs from risk_warehouse/, where profiles.yml lives.
DBT := cd risk_warehouse && ../$(VENV)/bin/dbt
PAYSIM_CSV := data/raw/paysim.csv

.DEFAULT_GOAL := help
.PHONY: help setup data entities load ingest deps build docs lint format clean

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

lint: ## Ruff lint and format check (same as CI)
	$(RUFF) check .
	$(RUFF) format --check .

format: ## Fix lint issues and format Python code
	$(RUFF) check --fix .
	$(RUFF) format .

clean: ## Delete generated data, the warehouse file and dbt build output
	rm -rf data/raw/generated warehouse/risk.duckdb warehouse/risk.duckdb.wal \
		risk_warehouse/target risk_warehouse/logs
