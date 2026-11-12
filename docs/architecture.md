# Architecture

How data flows through payments-risk-warehouse, and how the repo is built, tested and published.
For the reasoning behind each choice, see [Key design decisions](../README.md#key-design-decisions)
in the README; for every model and column, see the
[live dbt docs](https://karthikrk1.github.io/payments-risk-warehouse/).

## Data flow

```mermaid
flowchart LR
    subgraph sources["Sources (data/raw/, git-ignored)"]
        paysim["paysim.csv<br/>6.36M transactions<br/>(Kaggle, CC BY-SA 4.0)"]
    end

    subgraph ingestion["Ingestion (Python + DuckDB)"]
        gen["generate_entities.py<br/>accounts, devices, logins, disputes<br/>(MD5-seeded, reproducible)"]
        load["load_raw.py<br/>+ source_row_number, _loaded_at"]
    end

    subgraph warehouse["DuckDB: warehouse/risk.duckdb"]
        raw[("raw.*<br/>paysim_transactions<br/>app_accounts / devices / logins / disputes")]

        subgraph dbt["dbt project: risk_warehouse/"]
            stg["staging (views)<br/>stg_paysim__transactions<br/>stg_app__*"]
            int["intermediate (views)<br/>int_account_hourly_activity<br/>int_account_velocity<br/>int_transaction_login_features<br/>int_transaction_dispute_features"]
            core["marts/core (tables)<br/>fct_transactions (incremental)<br/>fct_logins, fct_disputes<br/>dim_account, dim_device,<br/>dim_date, dim_transaction_type"]
            ml["marts/ml (table)<br/>ml_transaction_features"]
            mon["monitoring (tables)<br/>mon_daily_metrics<br/>mon_daily_anomalies"]
            seed["seeds<br/>transaction_types, known_events"]
        end
    end

    analyses["analyses/<br/>make results → README Results"]

    paysim --> gen --> load
    paysim --> load
    load --> raw --> stg
    stg --> int --> ml
    stg --> core
    seed --> core
    core --> analyses
    ml --> analyses
    core --> mon
    ml --> mon
    seed --> mon
```

| Layer | Schema | Materialized | Rule |
|---|---|---|---|
| Raw | `raw` | tables (loaded by Python) | As delivered; only `source_row_number` and `_loaded_at` added |
| Staging | `staging` | views | One model per source table: rename, cast, clean. No joins |
| Intermediate | `intermediate` | views | Joins and point-in-time feature logic |
| Core | `core` | tables (`fct_transactions` incremental) | Star schema for analytics |
| ML | `ml` | table | One row per transaction: features, label, baseline, audit columns |
| Monitoring | `monitoring` | tables | Daily metrics from the data and their anomaly scores |
| Ops | `ops` | table written by the flow | `run_log`: what each run saw (freshness, rows, anomalies) |

Schemas are used as named (`macros/generate_schema_name.sql`), not prefixed with `main_`.

## Point-in-time features

```mermaid
flowchart LR
    tx["Transaction at hour h"]
    vel["Velocity windows<br/>hours h-1 back to h-168<br/>(current hour excluded)"]
    log["Logins and device sharing<br/>strictly before the transaction"]
    dis["Disputes filed<br/>strictly before the transaction"]
    feat["ml_transaction_features<br/>+ audit_* = latest source event read"]
    test{{"assert_ml_features_use_only_prior_data<br/>assert_ml_features_exclude_balances<br/>assert_receiver_velocity_matches_recount"}}

    vel --> feat
    log --> feat
    dis --> feat
    tx --> feat
    feat --> test
```

Every feature group records the latest source event it read in an `audit_*` column, and the tests
fail if any is not before the transaction, if a balance column appears, or if the window functions
disagree with a brute-force recount. Each test was checked by breaking the model on purpose.

## Orchestration and replay (Prefect)

```mermaid
flowchart LR
    gen["generate-entities"]
    deps["dbt-deps"]
    load["load-raw<br/>--as-of-hours H<br/>(retries: 2)"]
    build["dbt-build<br/>(no retries)"]
    gate{{"Quality gate<br/>run_results.json:<br/>any error/fail?"}}
    stop["Stop the flow<br/>name the failing tests"]
    triage["Triage agent (read-only)<br/>Claude diagnosis → triage artifact"]
    counts["warehouse-counts<br/>per-batch table artifact"]
    mon["monitoring-check<br/>this batch's anomalies (warnings)<br/>+ ops.run_log"]
    next{"More batches?"}
    compare["compare-warehouses<br/>replay = full build,<br/>row for row"]
    analyses["run-analyses<br/>results artifact"]

    gen --> deps --> load --> build --> gate
    gate -- yes --> triage --> stop
    gate -- no --> counts --> mon --> next
    next -- "yes: H += batch" --> load
    next -- no --> compare --> analyses
```

`flows/pipeline.py` runs in two modes:
- **full** (`make flow`): one batch with no cutoff, the same as `make ingest` + `make build` +
  `make results`, orchestrated.
- **replay** (`make replay`, `make ci-replay`): start from an empty warehouse and load one batch of
  simulated time at a time (a day locally, a week in CI), with `dbt build` and the gate after each, until
  the last late dispute has arrived. The incremental `fct_transactions` grows batch by batch. At the end
  every `core` and `ml` table must equal a one-shot full build.

When the gate fails, the flow first asks the triage agent ([`triage/`](../triage/README.md)) for a
diagnosis: context gathered from dbt's artifacts and git, one Claude call, a structured answer
logged as a `triage` artifact. It is best effort: with no API key or any error it is skipped, and the
run always fails with the gate's own error.

`load_raw.py --as-of-hours H` decides what existed at the cutoff: transactions by `step`, and each
entity by the time it became known (`signup_ts`, `first_seen_ts`, `login_ts`, `filed_ts`). Each run
builds its own warehouse file (`WAREHOUSE_PATH`), so replay never touches the full build.

## Monitoring

```mermaid
flowchart LR
    facts["fct_* and ml_transaction_features"]
    metrics["mon_daily_metrics<br/>per source, metric, dimension, day"]
    score["mon_daily_anomalies<br/>vs previous 7 days: median + MAD<br/>modified z > 3.5 and change ≥ 20%"]
    known["known_events seed<br/>verified, explained events"]
    status{{"status"}}
    warn["assert_no_unexplained_anomalies<br/>(warning)"]
    guard["assert_every_monitor_was_scored<br/>(error: no silent empty windows)"]

    facts --> metrics --> score --> status
    known --> status
    status -- "anomaly" --> warn
    score --> guard
```

Metrics come from the data, not from when the pipeline ran, so `monitoring` is part of the
replay-equals-full-build check. `ops.run_log`, which records what each run saw at the time, is not.
Model contracts on the facts and the ML table fail the build on a dropped or retyped column.

## Build, test and publish

```mermaid
flowchart LR
    pr["Pull request into main"]
    lint["CI: lint<br/>ruff check + format"]
    cibuild["CI: dbt<br/>make ci-build, then make ci-replay<br/>(full build + weekly replay on data/sample/,<br/>replay must equal the build)"]
    merge["Merge to main<br/>(protected: PR + both checks)"]
    docs["Docs workflow<br/>make ci-build → dbt docs generate --static<br/>+ monitoring page from the committed export"]
    pages["GitHub Pages<br/>/ (dbt docs) and /monitoring/ (charts)"]
    dependabot["Dependabot (weekly)<br/>Actions + direct Python deps"]

    pr --> lint
    pr --> cibuild
    lint --> merge
    cibuild --> merge
    merge --> docs --> pages
    dependabot --> pr
```

- **The sample** (`data/sample/paysim_sample.csv`, 4,904 rows, `make sample`) lets CI run the whole
  pipeline on real data in about 25 seconds, so model, test and dependency changes are exercised, not
  just parsed. Its fraud rate is skewed on purpose; results always come from the full data.
- **Separate warehouses, never mixed:** `warehouse/risk.duckdb` (full data, dbt target `dev`),
  `warehouse/ci.duckdb` (sample, target `ci`), and the replay copies `warehouse/replay.duckdb` and
  `warehouse/ci_replay.duckdb`.

## Commands

| Command | What it does |
|---|---|
| `make setup` | `.venv` with Python 3.12, dependencies and dev tools |
| `make data` | Download PaySim from Kaggle to `data/raw/paysim.csv` |
| `make ingest` | Generate entities, load everything into `raw` (full data) |
| `make build` | `dbt deps` + `dbt build` on the full data |
| `make results` | Run the analyses and print the README Results tables |
| `make ci-build` | The whole pipeline on the sample (what CI runs) |
| `make flow` | The full pipeline on the full data as a Prefect flow |
| `make replay` | Replay the full data day by day; must equal `make build` |
| `make ci-replay` | Replay the sample week by week; must equal `make ci-build` (what CI runs) |
| `make docs` | Generate and serve the dbt docs locally |
| `make lint` / `make format` | Ruff |
| `make sample` | Rebuild the committed CI sample from the full CSV |
| `make monitoring-export` | Export full-data monitoring results for the chart page |
| `make monitoring-page` | Build the chart page locally (`site/monitoring/index.html`) |
| `make triage` | Diagnose the last failed build with Claude (read-only) |
| `make triage-eval` | Grade the triage agent on 9 planted breakages (billed) |
| `make clean` | Delete generated data, warehouse files and dbt output |
