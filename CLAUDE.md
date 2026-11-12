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
- GitHub Actions: CI (`lint`, `dbt`) on every PR; docs published to GitHub Pages on merge.
- Prefect 3 orchestration (`flows/pipeline.py`) with replay mode; custom data monitoring in dbt.
- Phase 2 next: a monitoring chart page, then a Claude-API triage agent for test failures. Snowflake +
  Terraform is documented as an extension, not built. Plan and status: `docs/phase-2-plan.md`.

## Repository layout

```
payments-risk-warehouse/
├── CLAUDE.md
├── README.md
├── Makefile                  # `make help` lists targets: setup, data, ingest, build, results, flow, replay, ...
├── requirements.txt          # runtime, pinned
├── requirements-dev.txt      # + ruff
├── ruff.toml
├── .github/
│   ├── workflows/ci.yml      # lint + dbt (full pipeline on the sample) on every PR
│   ├── workflows/docs.yml    # dbt docs → GitHub Pages on merge to main
│   └── dependabot.yml        # Actions + direct Python deps only
├── data/
│   ├── raw/                  # PaySim CSV + generated files (git-ignored)
│   └── sample/               # committed 4,904-row CI sample + attribution (CC BY-SA 4.0)
├── ingestion/
│   ├── common.py             # paths, PaySim reader (source_row_number), rnd() macro
│   ├── generate_entities.py  # synthetic accounts, devices, logins, disputes (MD5-seeded, reproducible)
│   ├── load_raw.py           # loads raw files into DuckDB schema `raw`
│   └── make_sample.py        # builds the CI sample
├── flows/
│   └── pipeline.py           # Prefect flow: full run or replay, with the dbt quality gate
├── scripts/
│   ├── run_analyses.py       # `make results`: analyses → Markdown tables
│   └── compare_warehouses.py # replay check: core/ml tables equal a full build, row for row
├── risk_warehouse/           # dbt project (profiles.yml in here; targets dev and ci)
│   ├── models/
│   │   ├── staging/          # stg_*  : 1:1 with raw sources, rename/cast/clean only
│   │   ├── intermediate/     # int_*  : joins, window features, business logic
│   │   ├── marts/
│   │   │   ├── core/         # dim_* and fct_* (star schema, contracts enforced)
│   │   │   └── ml/           # ML-ready feature tables (contract enforced)
│   │   └── monitoring/       # mon_*  : daily metrics and anomaly scores
│   ├── analyses/             # results queries (precision/recall via macros/rule_metrics.sql)
│   ├── tests/                # singular data tests, incl. the leakage tests
│   ├── macros/
│   └── seeds/                # small static reference data only
└── docs/
    ├── architecture.md       # diagrams: data flow, point-in-time checks, build/test/publish
    └── phase-2-plan.md       # Phase 2 plan and status
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
make flow      # full pipeline as a Prefect flow (flows/pipeline.py)
make replay    # replay the full data day by day into warehouse/replay.duckdb; must equal make build
make ci-replay # replay the sample week by week; must equal make ci-build, same as CI
make monitoring-export  # full-data monitoring -> docs/monitoring/monitoring.json (commit it)
make monitoring-page    # build the chart page locally -> site/monitoring/index.html
```

## Orchestration and replay

- `flows/pipeline.py` (Prefect 3, no server or account needed) runs ingestion and dbt as
  subprocesses and gates on dbt's `target/run_results.json`: any `error`/`fail` raises
  `DataQualityGateError`, naming the nodes. Loading retries; `dbt build` never does.
- **Replay:** `load_raw.py --as-of-hours H` loads only what existed before the cutoff. **A new raw
  source must define the time it becomes known** (`ENTITY_TIME_COLUMNS` in `load_raw.py`), or replay
  will leak future rows into early batches.
- **Replay must equal a full build** (`scripts/compare_warehouses.py` over `core` and `ml`). If a change
  breaks that, the change is wrong, not the check: incremental logic, point-in-time features and the
  data's "known at" times all have to agree.
- `WAREHOUSE_PATH` overrides the DuckDB file in `profiles.yml`. The flow sets it so replay never touches
  `warehouse/risk.duckdb`.
- Don't run two dbt commands at once in this project: they share `risk_warehouse/target/`, and the gate
  reads `run_results.json` from there.
- `make` sets `PREFECT_SERVER_ANALYTICS_ENABLED=false` for flow runs; keep it when adding targets.

## Monitoring

- `models/monitoring/`: `mon_daily_metrics` (computed from the data, so replay must still equal a full
  build; `scripts/compare_warehouses.py` covers `core`, `ml` and `monitoring`) and `mon_daily_anomalies`
  (each day vs the previous 7: modified z-score > 3.5 **and** ≥ 20% from the median; vars in
  `dbt_project.yml`). Don't tune those thresholds to make warnings go away.
- **`known_events` seed:** add a row only after verifying the cause in the data, with the reason in
  `description`. Never add one to silence a warning. The Jan 13-14 burst at the 10,000,000 cap is
  deliberately unexplained.
- Anomalies are **warnings** (`assert_no_unexplained_anomalies`); a monitor that was never scored once
  its source has history is an **error** (`assert_every_monitor_was_scored`). **A new monitor must be
  added to that test's `expected_monitors` list.**
- `ops.run_log` is written by the flow (`monitoring-check` task), not dbt. It records what each run saw,
  differs between replay and a full build by design, and is not compared.
- Model contracts are enforced on `fct_*` and `ml_transaction_features`: changing a column means
  updating its `data_type` in the YAML. Cast aggregates explicitly; DuckDB's `SUM` returns `HUGEINT` /
  `DECIMAL(38,2)`, which aren't portable.
- Elementary was evaluated and not adopted (its window ends at "now"; see `docs/phase-2-plan.md`).
  Don't add it back without solving that.
- **Chart page** (`docs/monitoring/template.html` + `scripts/build_monitoring_page.py`, published at
  `/monitoring/`) is built from `docs/monitoring/monitoring.json`, a committed export of the **full** data.
  After any change to monitoring logic, metrics or known events, run `make build && make monitoring-export`
  and commit the JSON in the same PR; CI can't regenerate it (it only has the sample). The export
  refuses to write if a band disagrees with a day's status. Render the page and look at it before
  shipping a template change.

## Working rules for Claude

- Make small, reviewable changes and explain the design reasoning in the reply. After changing models,
  run `dbt build --select <model>+` and fix failures before moving on.
- Do not invent results or metrics. The README results section is filled in only from real query output.
- Never commit data files, `.duckdb` files, `.env` or credentials. The one exception is the CI sample
  `data/sample/paysim_sample.csv` (CC BY-SA 4.0, attribution in `data/sample/README.md`); regenerate it
  with `make sample`, never edit it by hand. Rates in the sample are skewed (12% fraud); never quote
  results from it.
- `main` is protected: no direct pushes, no force pushes, no deletion. Work on a branch and open a
  pull request; it merges only when the CI checks `lint` (ruff) and `dbt` (the full pipeline and a
  weekly replay on the sample) pass. Run `make lint`, `make ci-build` and `make ci-replay` locally
  before pushing.
- Every merge to `main` republishes the dbt docs site (`.github/workflows/docs.yml`, GitHub Pages:
  https://karthikrk1.github.io/payments-risk-warehouse/), built from the sample. Model and column
  descriptions are public there, so keep them accurate and free of anything that isn't in the repo.
- Pin GitHub Actions by full version or commit SHA when the action has no moving major tag
  (setup-uv only publishes full tags); check that a tag exists before using it.
- New Python dependencies go in `requirements.txt` (runtime) or `requirements-dev.txt` (tools), pinned.
- Keep the README and `docs/architecture.md` in sync when the architecture changes.
- Prefer DuckDB-compatible ANSI SQL that would port to Snowflake with minimal changes; note any
  dialect-specific function in a comment.
