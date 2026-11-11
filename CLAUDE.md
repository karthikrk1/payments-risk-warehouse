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
- Balance fields are frequently inconsistent with `amount` (on outgoing transfers and cash-outs the
  sender's balance is consistent only 4.8% / 11.5% of the time). Do not drop these rows; flag them
  (`has_sender_balance_mismatch`, `has_receiver_balance_mismatch`, `has_balance_mismatch` in staging).
- **The after-balances and the mismatch flags are post-transaction and must never be ML features.**
  A real-time check would not know them, and in PaySim they almost reveal the label (on transfers and
  cash-outs the sender's balance adds up for 99.45% of fraud vs 9.9% of legitimate rows; fraud empties
  the sender's account exactly 97.7% of the time vs 0.0%).
- **No balance column may feed an ML feature, the `*_before_amount` ones included.** The dataset's own
  description says: "Transactions which are detected as fraud are cancelled, so for fraud detection
  these columns (oldbalanceOrg, newbalanceOrig, oldbalanceDest, newbalanceDest) must not be used."
  Use balances for analysis and data-quality reporting only.
- Do money arithmetic in `decimal(18, 2)`, never floating point: PaySim balances are often off by
  exactly one cent, which floating point turns into 0.0100000000093 and misreads as a mismatch.
- `isFlaggedFraud` is the legacy rule-based flag; treat it as a baseline to compare against. Kaggle
  documents it as "transfers over 200.000", but **that doesn't match the data**: 16 rows are flagged,
  out of 409,110 transfers over 200,000; flagged amounts are 353,874 to 10,000,000, and every flagged
  row has an unchanged sender balance. Describe it as "PaySim's legacy flag", never as the 200,000 rule.
- Fraud amounts are capped at exactly 10,000,000 (287 rows); large fraud is split into chunks.
- Facts measured on load: 743 steps; 8,213 fraud rows (0.13%); `isFlaggedFraud` fires 16 times;
  99.85% of senders (`nameOrig`) appear once, so build velocity for receivers too; 16 rows have amount 0.
- The file lives at `data/raw/paysim.csv`. PaySim has no transaction ID: `source_row_number` (added by
  `ingestion/common.py`, the 1-based data row) becomes `transaction_id` in staging.

**Synthetic entities** from `ingestion/generate_entities.py`, written to `data/raw/generated/*.parquet`
and loaded by `ingestion/load_raw.py` as `raw.app_accounts`, `raw.app_devices`, `raw.app_logins`,
`raw.app_disputes`: accounts (signup time, KYC status, country), devices (OS, first seen), login
attempts (success/failure, device, IP country) and disputes (reason, status, filed after their
transaction). Every value is derived from MD5(seed | key), so the data is identical on every re-run
(files are byte-identical on the same DuckDB version; Parquet records the writer version).
**Logins and devices carry a planted fraud signal on purpose** (`PLANTED_SIGNAL`): fraudulent senders
get more failed logins and shared "ring" devices. Always describe results on those features as
synthetic, never as evidence of real-world predictiveness. Nothing else in the generator depends on
the label, except disputes (which follow fraud, as in reality).

## Modelling conventions

- **Layers:** `stg_` → `int_` → `dim_` / `fct_` / `ml_`. Marts never select from raw sources;
  staging never joins.
- **Star schema (marts/core):**
  - `fct_transactions` — grain: one row per transaction. Incremental (`delete+insert`, unique key
    `transaction_id`), loading only `transaction_id > max(transaction_id)`. That is valid only because
    the PaySim file is in time order; if a source ever isn't, change the filter (e.g. to a loaded_at
    watermark) rather than keeping it.
  - `fct_logins` — grain: one row per login attempt (adds `is_foreign_ip`).
  - `fct_disputes` — grain: one row per dispute (adds `hours_to_file`).
  - `dim_account` (customers **and merchants**, so every receiver key resolves), `dim_device`,
    `dim_date` (range from vars `dim_date_start` / `dim_date_end`), `dim_transaction_type` (from the
    `transaction_types` seed).
  - Each fact has a `dbt_utils.equal_rowcount` test against its staging model, and every FK and date
    column has a `relationships` test to its dimension.
- **Surrogate keys:** generate with `dbt_utils.generate_surrogate_key`; keep natural keys alongside.
  Facts compute the same hash instead of joining to the dimension; the relationship tests prove it.
- **Naming:** snake_case; booleans prefixed `is_` / `has_`; timestamps suffixed `_ts`, dates `_date`;
  amounts suffixed `_amount`.
- **Every model** has a YAML entry with a description, a stated grain, and column descriptions.
  These feed the dbt docs site and act as the data dictionary.
- **Risk features (intermediate):** `int_account_hourly_activity` → `int_account_velocity` (activity in
  the 1 / 24 / 168 hours before each active hour, via `macros/prior_hours_window.sql`),
  `int_transaction_login_features` (logins and failed logins in the prior 24 h, foreign-IP latest login,
  other accounts that used that device *before* the transaction), `int_transaction_dispute_features`
  (disputes filed before the transaction, by the sender and on the receiver's received transactions).
  Sender history is almost always empty in PaySim (only 900 transactions' senders had received
  earlier), so receiver-side features carry the signal.
- **ML feature table (`ml_transaction_features`):** one row per transaction with point-in-time-correct
  features, the `is_fraud` label and `baseline_is_flagged_fraud`. Rules, each enforced by a test:
  - windows end at the **previous hour** (PaySim timestamps are hourly; same-hour order is unknown);
  - every feature group exposes an `audit_*` column with the latest source event it read, and
    `assert_ml_features_use_only_prior_data` checks each is before `as_of_ts`. **A new feature group
    must add its own audit column and a condition in that test**;
  - no column containing `balance` or `mismatch` (`assert_ml_features_exclude_balances`);
  - receiver velocity equals a brute-force recount (`assert_receiver_velocity_matches_recount`).
  When changing any of this, break it on purpose once and confirm the test fails.

## Testing and data quality

- Every primary key: `unique` + `not_null`. Every foreign key: `relationships`.
- `accepted_values` on enumerations (transaction type, KYC status, dispute status).
- Use `dbt_expectations` or singular tests for ranges (amount >= 0 as error, amount > 0 as warn because
  of the 16 zero rows; timestamps within the data window). Singular tests live in `risk_warehouse/tests/`.
- Add a singular test that asserts no ML feature uses data from after the transaction timestamp.
- Use `severity: warn` for known, rare PaySim quirks and `error` for real integrity breaks. Don't add a
  test that fires on millions of rows by design (balance mismatches are the norm, not a quirk): flag it
  as a column instead.
- Generic test parameters go under `arguments:` (dbt 1.10+); the old flat style is deprecated.
- Custom schemas are used as-is (`macros/generate_schema_name.sql`): models land in `staging`,
  `intermediate`, `core`, `ml`, not `main_staging`.

## Commands

```bash
make help      # list targets
make setup     # .venv (Python 3.12) + requirements-dev.txt
make ingest    # generate_entities.py, then load_raw.py
make build     # dbt deps + dbt build (run and test everything)
make docs      # dbt docs generate + serve
make results   # analyses -> Markdown tables; README Results is pasted from this, never typed by hand
make lint      # ruff, same as CI
make ci-build  # whole pipeline on data/sample/paysim_sample.csv -> warehouse/ci.duckdb, same as CI
make sample    # rebuild the committed sample from the full CSV (deterministic)
```

## Working rules for Claude

- Make small, reviewable changes and explain the design reasoning in the reply. After changing models,
  run `dbt build --select <model>+` and fix failures before moving on.
- Do not invent results or metrics. The README results section is filled in only from real query output.
- Never commit data files, `.duckdb` files, `.env` or credentials. The one exception is the CI sample
  `data/sample/paysim_sample.csv` (CC BY-SA 4.0, attribution in `data/sample/README.md`); regenerate it
  with `make sample`, never edit it by hand. Rates in the sample are skewed (12% fraud); never quote
  results from it.
- `main` is protected: no direct pushes, no force pushes, no deletion. Work on a branch and open a
  pull request; it merges only when the CI checks `lint` (ruff) and `dbt` (the full pipeline on the
  sample) pass. Run `make lint` and `make ci-build` locally before pushing.
- New Python dependencies go in `requirements.txt` (runtime) or `requirements-dev.txt` (tools), pinned.
- Keep the README and `docs/architecture.md` in sync when the architecture changes.
- Prefer DuckDB-compatible ANSI SQL that would port to Snowflake with minimal changes; note any
  dialect-specific function in a comment.
