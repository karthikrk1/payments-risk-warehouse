# Phase 2 plan

Phase 1 (ingestion, staging, star schema, point-in-time features, tests, results, CI and hosted docs) is
complete. Phase 2 turns the warehouse from a pipeline that runs on demand into a system that runs on
its own, watches itself and explains its failures.

**Order:** 1 → 2 → 3, with 4 optional. Each step feeds the next: orchestration produces run history,
observability watches it, and the triage agent explains what fails.

| # | Item | Status | Cost | Size |
|---|---|---|---|---|
| 1 | Orchestration with Prefect | Next | None | 1–2 PRs |
| 2 | Data observability with Elementary | Planned | None | 1 PR |
| 3 | AI triage agent (Claude API) | Planned | Cents per run (API key) | 1–2 PRs |
| 4 | Snowflake + Terraform | Optional | Snowflake credits | 2+ PRs |
| 5 | Update the karthikrk.com case study | After 1–3 | None | 1 PR |

## 1. Orchestration with Prefect

**Goal:** the pipeline runs as an orchestrated flow with retries, a data-quality gate and run history,
instead of a chain of `make` targets.

- `flows/pipeline.py`: generate → load → `dbt build` → analyses, each a Prefect task.
- **Retries only where they help:** loading yes; a failing dbt test no (a data problem, not a glitch).
- **Quality gate:** if `dbt build` fails, nothing downstream runs, and the run reports which tests
  failed, from dbt's structured results rather than log scraping.
- **Run summary** (row counts, tests passed/failed, duration) as a Prefect artifact; timeouts as a
  simple SLA.
- Runs locally with no account (`make flow`); `prefect server start` gives the UI. CI runs the flow on
  the sample.
- **Proposed, to confirm: replay mode.** Process PaySim one simulated day at a time (31 runs) instead of
  all at once. That exercises the incremental `fct_transactions` the way production would and gives
  item 2 a real time series to learn from.

**Done when:** `make flow` runs the full pipeline with the gate and summary, CI runs it on the sample,
and the README documents it.

## 2. Data observability with Elementary

**Goal:** catch what tests don't (volume drops, stale data, odd distributions) and keep a history of
test results.

- **First, verify Elementary works with DuckDB.** Don't build on it until that's confirmed; if it
  doesn't, choose an alternative before writing code.
- Anomaly tests (volume, freshness, column distributions) on the facts.
- The Elementary report published next to the dbt docs on GitHub Pages.

**Done when:** anomaly tests run in CI and the report is published.

## 3. AI triage agent (Claude API)

**Goal:** when dbt tests fail, an agent reads the failing test, sample failing rows, lineage and recent
code changes, then classifies the failure (data issue, code bug, expected) and suggests a fix, written
to the Prefect run or as a PR comment.

- **Evaluation built in:** the deliberate breakages already used to prove the leakage tests (a window
  that includes the current hour, a balance column in the ML table) become an evaluation set for whether
  the diagnoses are right.
- Needs an Anthropic API key, stored as a GitHub secret; never committed.
- Before building, check the current Claude models and API (don't rely on memory).

**Done when:** the agent diagnoses the seeded failures correctly and its output appears where failures
are reviewed.

## 4. Snowflake + Terraform (optional)

**Goal:** show the same project on a cloud warehouse, provisioned as code.

- Terraform creates the database, warehouse, roles and user; dbt gets a `snowflake` target.
- Port the DuckDB-specific pieces (`isodow` in `dim_date`, integer `RANGE` window frames in
  `macros/prior_hours_window.sql`); everything else uses dbt's cross-database macros.
- **Costs money** (a trial, then pay-as-you-go). Worth it only if Snowflake matters for target roles.

## Rules that carry over from Phase 1

- Results only from real output (`make results` or the equivalent); never typed by hand.
- No balance column as an ML feature; every new feature group adds an `audit_*` column and a condition
  in the leakage test.
- Verify documentation against the data, and check that an action, package or tag exists before
  pinning it.
- Every change through a PR with `lint` and `dbt` passing.
