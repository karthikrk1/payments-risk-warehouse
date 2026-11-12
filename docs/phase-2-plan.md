# Phase 2 plan

Phase 1 (ingestion, staging, star schema, point-in-time features, tests, results, CI and hosted docs) is
complete. Phase 2 turns the warehouse from a pipeline that runs on demand into a system that runs on
its own, watches itself and explains its failures.

**Order:** 1 → 2 → 3, then the wrap-up. Each step feeds the next: orchestration produces run history,
monitoring watches it, and the triage agent explains what fails. Snowflake is documented as an
extension rather than built (item 4).

| # | Item | Status | Cost | Size |
|---|---|---|---|---|
| 1 | Orchestration with Prefect, with replay | Done (#17) | None | 1 PR |
| 2 | Data monitoring: custom, in dbt (Elementary evaluated, not adopted) | Done (#18); chart page in review (`feat/monitoring-page`) | None | 2 PRs |
| 3 | AI triage agent (Claude API) | Planned | Cents per run (API key) | 1–2 PRs |
| 4 | Snowflake + Terraform | Out of scope: documented as an extension | — | docs only |
| 5 | Wrap-up: Snowflake extension note, karthikrk.com case study (one update for all of Phase 2) | After 1–3 | None | 1–2 PRs |

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
- **Replay mode (confirmed).** Process PaySim one simulated day at a time instead of all at once. That
  exercises the incremental `fct_transactions` the way production would and gives item 2 a real time
  series to learn from.

**Done when:** `make flow` runs the full pipeline with the gate and summary, CI runs it on the sample,
and the README documents it.

**As built:**
- `ingestion/load_raw.py --as-of-hours H` loads only what existed before the cutoff: transactions with
  `step <= H`, and accounts, devices, logins and disputes by their signup / first-seen / login / filed
  time. Transaction IDs are assigned over the whole file first, so they match a full load.
- Replay runs until the last *late* dispute has arrived (about 45 days, not 31): disputes keep coming for
  up to 14 days after the last transaction.
- **Correctness check:** at the end of a replay, every `core` and `ml` table must equal a one-shot full
  build row for row (`scripts/compare_warehouses.py`). CI replays the sample weekly and checks this on
  every pull request; `make replay` does it daily on the full data.
- **Gate check:** with the velocity window deliberately broken, the flow stopped at the first batch,
  named both failing tests with their failing row counts, and ran nothing downstream.
- **Found by replay:** the entity generator placed 2,407 failed logins before their account existed.
  Fixed (attempts can't precede signup) and guarded by `assert_logins_after_account_signup`; features
  and `make results` are byte-identical after the fix.
- Prefect's temporary server reports anonymous usage statistics by default; the `make` targets turn that
  off (`PREFECT_SERVER_ANALYTICS_ENABLED=false`).

## 2. Data monitoring

**Goal:** catch what tests don't (volume drops, stale data, odd distributions) and keep a history of
what each run saw.

### Evaluated first: Elementary (not adopted)

Tried in a throwaway copy of the project on the CI sample (Elementary 0.26.1):
- **It works with DuckDB:** dbt-duckdb is a listed adapter, the CLI has a `duckdb` extra, its 32 dbt
  models build, anomaly tests run, and `edr report` works once its own `elementary` profile is added.
- **It needs a dependency downgrade:** `networkx < 3` (we pinned 3.7). dbt accepts 2.x, so it resolves,
  plus 31 more packages.
- **Its anomaly window ends at "now".** On PaySim's January 2024 data, run in November 2026, a volume
  test measured 14 empty days, found "0 rows, average 0" and passed without checking anything. The only
  override, `custom_run_started_at`, is marked "for test purposes only" in the package's source.
- **Volume alerts would fire on PaySim's own shape:** daily volume is ~400-575k for most of the first 17
  days, near zero on Jan 3-5, and 10-55k a day from Jan 18. On the noisy sample, a deliberate drop of the
  last day to zero wasn't flagged at all.

So monitoring is built in dbt instead, designed around the data.

### As built (custom)

- **`monitoring.mon_daily_metrics`:** daily metrics computed from the data itself (so replay still
  equals a full build): row counts per source (empty days count as 0), median and 95th-percentile amount
  per type, fraud share per type, null shares of nullable-by-design columns, failed-login share.
- **`monitoring.mon_daily_anomalies`:** each day vs the previous 7 days. Anomaly when the modified
  z-score (Iglewicz & Hoaglin; 0.6745 × (value − median) / MAD) exceeds **3.5** (the published rule, not
  tuned) **and** the value is at least **20%** from the median, with at least 5 days of history. The 20%
  floor exists because on millions of rows MAD is tiny and trivial moves (5.71% → 5.77%) score as
  extreme; on the full data the flag count goes 65 → 59 → 52 → 51 for floors of 0/10/20/25-30%.
- **`known_events` seed:** verified events that explain outliers (`expected` instead of `anomaly`):
  the Jan 3-7 volume lull, the Jan 1-14 dispute ramp-up, the Jan 18-25 volume collapse, the Jan 31 final
  day. Each row says why; rows are only added after checking the cause in the data.
- **Tests:** `assert_no_unexplained_anomalies` (**warning**: statistical, reported not blocking) and
  `assert_every_monitor_was_scored` (**error**: once a source has enough history, every monitor must have
  been scored against it, the empty-window lesson). Its first version demanded scoring from day one and
  failed the first batch of the daily replay, where nothing can be scored yet; the weekly sample replay
  had missed it because its first batch already holds 7 days.
- **Schema:** dbt model contracts (enforced) on the three facts and the ML table. Adding them exposed
  accidental `HUGEINT` / `DECIMAL(38,2)` types from DuckDB's `SUM`, now cast explicitly (values
  unchanged; verified row for row).
- **`ops.run_log`:** appended by the Prefect flow after each batch: rows seen per source, latest event,
  freshness at the cutoff, anomalies on that batch's days. It records what each run saw at the time, so it
  differs between replay and a full build by design and is excluded from the comparison (which now also
  covers `monitoring`).

**Findings on the full data:** 658 metric-days measured, 553 scored; 38 outliers explained by known events, 14 left
as warnings, including a burst nobody planned for: 2,125 transfers at exactly the 10,000,000 cap on
Jan 13 (only 12 fraud) and 727 more on Jan 14 (none fraud), against ~10 a day, all fraud, normally.
Monitoring also showed that PaySim injects fraud at a near-constant ~262 a day whatever the legitimate
volume, so the fraud *share* explodes when volume drops (Jan 31: 272 transactions, all fraud).

**Chart page (as built):** <https://karthikrk1.github.io/payments-risk-warehouse/monitoring/>, one
self-contained HTML file (no external scripts): headline counts, the unexplained anomalies, the known
events, and a small line chart per monitor (18; three always-zero fraud shares are listed, not drawn) with
the not-flagged band shaded, unexplained anomalies as red diamonds and explained days as grey rings,
hover/keyboard tooltips and a table view per chart. Built from `docs/monitoring/monitoring.json`, a
committed export of the full data (`make monitoring-export`), because charts of the skewed CI sample
would mislead. The export recomputes each band with the SQL rule and refuses to write if any band
disagrees with that day's status; CI runs the same export on the sample. Colours validated with the
data-viz palette checks in light and dark mode; rendered and inspected in a headless browser (light,
dark, 390 px phone width) before shipping.

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

## 4. Snowflake + Terraform (documented, not built)

Decided out of scope: the project already shows the data-engineering work end to end on DuckDB, and a
cloud warehouse would add cost without new design. Instead, `docs/snowflake-extension.md` (in the
wrap-up) will describe exactly how it would be added: Terraform for the database, warehouse, roles,
service user and grants; a `snowflake` dbt target; loading via a stage and `COPY INTO`; the DuckDB-specific
SQL to port; and a cheap CI setup. Each portability claim is checked against Snowflake's documentation
before it's written.

## Rules that carry over from Phase 1

- Results only from real output (`make results` or the equivalent); never typed by hand.
- No balance column as an ML feature; every new feature group adds an `audit_*` column and a condition
  in the leakage test.
- Verify documentation against the data, and check that an action, package or tag exists before
  pinning it.
- Every change through a PR with `lint` and `dbt` passing.
