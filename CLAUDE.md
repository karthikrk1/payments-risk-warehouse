# CLAUDE.md — payments-risk-warehouse

Guidance for Claude Code when working in this repository. Read this first in every session.

## What this project is

A portfolio data platform that models a peer-to-peer (P2P) payments company's **risk data foundation**:
transactions, accounts, devices, logins and disputes, transformed with dbt into a dimensional
warehouse and an ML-ready fraud feature table, with data quality and lineage monitoring.

The audience is hiring managers and senior data engineers reviewing the repo. Optimise for
**clarity, correctness and explainable design decisions** over feature count. The owner must be able
to explain every model and every line in an interview, so prefer simple, readable SQL to clever SQL.

## Stack

- Python 3.12, managed with **uv** (`uv venv`, `uv pip install`). Do not use the system or Homebrew
  Python, and do not call `pip` directly; use `uv pip`.
- **dbt Core 1.12** with the **dbt-duckdb** adapter. Local warehouse file: `warehouse/risk.duckdb`.
- Planned (phase 2): Prefect for orchestration, Elementary for data observability, Snowflake as an
  alternative target with Terraform, GitHub Actions CI, and a Claude-API triage agent for test failures.

## Repository layout

```
payments-risk-warehouse/
├── CLAUDE.md
├── README.md
├── requirements.txt
├── Makefile                  # make setup / make seed / make build / make docs / make demo
├── data/
│   └── raw/                  # PaySim CSV + generated files (git-ignored)
├── ingestion/
│   ├── generate_entities.py  # synthetic accounts, devices, logins, disputes (seeded, reproducible)
│   └── load_raw.py           # loads raw files into DuckDB schema `raw`
├── risk_warehouse/           # dbt project
│   ├── dbt_project.yml
│   ├── models/
│   │   ├── staging/          # stg_*  : 1:1 with raw sources, rename/cast/clean only
│   │   ├── intermediate/     # int_*  : joins, window features, business logic
│   │   └── marts/
│   │       ├── core/         # dim_* and fct_* (star schema)
│   │       └── ml/           # ML-ready feature tables
│   ├── tests/                # singular data tests
│   ├── macros/
│   └── seeds/                # small static reference data only
├── triage_agent/             # phase 2
└── docs/
    └── architecture.md       # diagrams and design decisions
```

## Source data

**PaySim** (Kaggle, synthetic mobile-money transactions) is the transaction source. Columns:
`step, type, amount, nameOrig, oldbalanceOrg, newbalanceOrig, nameDest, oldbalanceDest, newbalanceDest, isFraud, isFlaggedFraud`.

Known characteristics to handle explicitly and document:
- `step` is an hour index (1 hour per step, about 30 days). Derive a synthetic `transaction_ts` from a
  fixed anchor date; keep the anchor in a dbt var `paysim_start_date`.
- Fraud only occurs in `TRANSFER` and `CASH_OUT` types.
- Account IDs starting with `M` are merchants; they have no balance information.
- Balance fields are frequently inconsistent with `amount`. Do not drop these rows; flag them
  (`has_balance_mismatch`) and treat the flag as a candidate feature.
- `isFlaggedFraud` is the legacy rule-based flag; treat it as a baseline to compare against.
- Facts measured on load: 743 steps; 8,213 fraud rows (0.13%); `isFlaggedFraud` fires 16 times;
  99.85% of senders (`nameOrig`) appear once, so build velocity for receivers too; 16 rows have amount 0.
- The file lives at `data/raw/paysim.csv`. PaySim has no transaction ID: `source_row_number` (added by
  `ingestion/common.py`, the 1-based data row) becomes `transaction_id` in staging.

**Synthetic entities** from `ingestion/generate_entities.py`, written to `data/raw/generated/*.parquet`
and loaded by `ingestion/load_raw.py` as `raw.app_accounts`, `raw.app_devices`, `raw.app_logins`,
`raw.app_disputes`: accounts (signup time, KYC status, country), devices (OS, first seen), login
attempts (success/failure, device, IP country) and disputes (reason, status, filed after their
transaction). Every value is derived from MD5(seed | key), so output is byte-identical on re-run.
**Logins and devices carry a planted fraud signal on purpose** (`PLANTED_SIGNAL`): fraudulent senders
get more failed logins and shared "ring" devices. Always describe results on those features as
synthetic, never as evidence of real-world predictiveness. Nothing else in the generator depends on
the label, except disputes (which follow fraud, as in reality).

## Modelling conventions

- **Layers:** `stg_` → `int_` → `dim_` / `fct_` / `ml_`. Marts never select from raw sources;
  staging never joins.
- **Star schema (marts/core):**
  - `fct_transactions` — grain: one row per transaction. Incremental, keyed on `transaction_id`.
  - `fct_logins` — grain: one row per login attempt.
  - `fct_disputes` — grain: one row per dispute.
  - `dim_account`, `dim_device`, `dim_date`, `dim_transaction_type`.
- **Surrogate keys:** generate with `dbt_utils.generate_surrogate_key`; keep natural keys alongside.
- **Naming:** snake_case; booleans prefixed `is_` / `has_`; timestamps suffixed `_ts`, dates `_date`;
  amounts suffixed `_amount`.
- **Every model** has a YAML entry with a description, a stated grain, and column descriptions.
  These feed the dbt docs site and act as the data dictionary.
- **Risk features (intermediate):** transaction velocity per account (count and amount over
  1h / 24h / 7d windows), account age at transaction time, amount vs the account's trailing average,
  devices per account and accounts per device, failed logins before a transaction, prior disputes.
- **ML feature table (`ml_transaction_features`):** one row per transaction with point-in-time-correct
  features (only use information available **before** the transaction) and the `is_fraud` label.
  Avoiding label leakage is a core design point; call it out in model descriptions.

## Testing and data quality

- Every primary key: `unique` + `not_null`. Every foreign key: `relationships`.
- `accepted_values` on enumerations (transaction type, KYC status, dispute status).
- Use `dbt_expectations` or singular tests for ranges (amount > 0, timestamps within the data window).
- Add a singular test that asserts no ML feature uses data from after the transaction timestamp.
- Use `severity: warn` for known PaySim quirks (balance mismatches) and `error` for real integrity breaks.

## Commands

```bash
make help      # list targets
make setup     # .venv (Python 3.12) + requirements-dev.txt
make ingest    # generate_entities.py, then load_raw.py
make build     # dbt deps + dbt build (run and test everything)
make docs      # dbt docs generate + serve
make lint      # ruff, same as CI
```

## Working rules for Claude

- Make small, reviewable changes and explain the design reasoning in the reply. After changing models,
  run `dbt build --select <model>+` and fix failures before moving on.
- Do not invent results or metrics. The README results section is filled in only from real query output.
- Never commit data files, `.duckdb` files, `.env` or credentials.
- `main` is protected: no direct pushes, no force pushes, no deletion. Work on a branch and open a
  pull request; it merges only when the CI checks `lint` (ruff) and `dbt` pass. Run
  `ruff check . && ruff format --check .` and `dbt parse` (or `dbt build`) locally before pushing.
- New Python dependencies go in `requirements.txt` (runtime) or `requirements-dev.txt` (tools), pinned.
- Keep the README and `docs/architecture.md` in sync when the architecture changes.
- Prefer DuckDB-compatible ANSI SQL that would port to Snowflake with minimal changes; note any
  dialect-specific function in a comment.
