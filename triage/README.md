# Triage agent

When `dbt build` fails, the triage agent explains why: it classifies the failure, names the cause
and the file, quotes the evidence, and suggests the smallest fix plus one check that would confirm
it. It is **read-only**: it never edits files, never writes to the warehouse and never retries
anything. An engineer reads the diagnosis and decides.

```bash
make triage                        # diagnose the last failed build (risk_warehouse/target/)
python -m triage --dry-run         # print exactly what would be sent; no API call
make triage-eval                   # grade it on 9 planted breakages (billed, about $1-2)
```

The Prefect flow runs it automatically when the data-quality gate fails. The diagnosis appears in
the run log and as a `triage` artifact, and is saved to `risk_warehouse/target/triage.md`. The
run still fails with the gate's own error; triage only adds an explanation.

## How it works

One model call, no tools, no loop. Everything the model reads is collected deterministically by
[`context.py`](context.py) from dbt's own artifacts:

| Section | Source |
|---|---|
| What failed, dbt's message, failing-row count, compiled SQL | `target/run_results.json` |
| Up to 20 of the rows the failing test returned | the test's own query, run on a **read-only** DuckDB connection |
| Upstream lineage: every ancestor model's SQL and description, nearest first, plus the project macros they use | `target/manifest.json` |
| Project configuration (vars such as the PaySim anchor date) | `dbt_project.yml` |
| What changed | `git diff` against `origin/main`, committed and uncommitted |
| Project rules | `CLAUDE.md` |

Each section is size-capped (requests in the evaluation were 11–25K input tokens), and the request says when something
was cut. The output is a fixed schema ([`diagnose.py`](diagnose.py)), validated by the SDK:

`category` (`code_bug` · `data_problem` · `test_problem` · `environment`) · `confidence` · `summary`
· `root_cause` · `evidence` · `suspect_files` · `suggested_fix` · `confirming_check`

**Why one call rather than an agent with tools?** The useful context is predictable: it's in dbt's
artifacts. Collecting it in code makes every diagnosis reproducible from the saved context, keeps
the cost bounded, and leaves the model nothing it could change. If the evaluation showed diagnoses
failing for lack of context the code can't anticipate, read-only query tools would be the next step.

**Sample rows are data, not instructions.** They are fenced in the request and the system prompt
says to ignore any instruction-like text inside them. The data here is synthetic (PaySim plus
generated entities), so nothing personal is sent.

## Model choice and what happens when a model is retired

The model is a setting: `TRIAGE_MODEL`, default `claude-opus-5-5` (effort `high`). When a model is
retired, triage reports it and exits; the pipeline is unaffected:

```
Triage unavailable: Model 'claude-…' was not found (retired?). Set TRIAGE_MODEL to a current model
and re-run the evaluation (make triage-eval).
```

Switching is one setting change. The evaluation is what makes the switch safe: run it on the new
model before trusting it. Requests use Anthropic's server-side fallback (`fallbacks="default"`),
which covers a model declining a request, not a retired model.

With no API key, an invalid key, a rate limit or any API error, triage prints why and exits with
code 2. In the flow it logs a warning and the run reports the gate failure as usual.

## Evaluation

[`eval/cases.py`](eval/cases.py) defines 9 realistic breakages, each planted in a scratch copy of the
project built on the committed CI sample. The runner checks that each change really makes the
build fail (and on which node), then has each model diagnose it:

| Case | Planted change | Fails on | Expected |
|---|---|---|---|
| window_includes_current_hour | window macro ends at `current row`, not `1 preceding` | leakage test + velocity recount | code_bug |
| balance_feature_added | a balance column added to the ML table and its contract | `assert_ml_features_exclude_balances` | code_bug |
| contract_cast_removed | a `SUM` loses its cast, widening to `DECIMAL(38,2)` | ML model contract (error) | code_bug |
| fraud_flag_inverted | staging maps `isFraud = 0` to `is_fraud` | `assert_fraud_only_in_transfer_and_cash_out` | code_bug |
| failed_share_monitor_removed | the failed-login monitor deleted | `assert_every_monitor_was_scored` | code_bug |
| duplicate_raw_transaction | one PaySim row loaded twice | `unique` on `transaction_id` | data_problem |
| login_before_signup | 5 raw logins dated 3 years early | `assert_logins_after_account_signup` | data_problem |
| window_test_off_by_one_day | the window test allows 30 days, not 31 | `assert_transaction_ts_within_paysim_window` | test_problem |
| disputes_table_missing | `raw.app_disputes` never loaded | `stg_app__disputes` (error) | environment |

**Grading:** the category must match exactly, and the diagnosis must name the cause, not just the
failing test: each case lists term groups (for example the macro's name, and `current row` or
`1 preceding`), and at least one term from every group must appear in the summary, root cause,
suspect files or fix. The data-problem cases have no code diff, as on a real branch.

### Results (November 2026)

| Model | Passed | Category right | Cost per diagnosis | Time |
|---|---|---|---|---|
| Opus 5.5 (`high` effort) | **9/9** | 9/9 | ~$0.09 | ~15 s |
| Sonnet 5.5 | 8/9 | 8/9 | ~$0.05 | ~10 s |

**What the evaluation found.** The first run (2 repeats per case) passed 16/18 on both models, and
both failed `login_before_signup` every time. The logins had been moved to January 2021, but the
context didn't include PaySim's anchor date (January 2024), so both models took 2021 as the real
window. Opus kept the right category (data) but blamed the accounts table; Sonnet blamed the
generator code. The fix was a context gap, not the prompt or the grader: the request now includes
`dbt_project.yml`. On the rerun Opus named `raw.app_logins` and the 3-year shift exactly. Sonnet found
the same facts but still attributed them to the generator (code_bug, low confidence), the same
call it made in all 3 runs of that case.

The two runs cost $3.69 in total.

**Why Opus is the default:** it is the only one that got the hardest case right, and at about $0.09
a diagnosis the difference in cost doesn't matter for an event that happens a few times a month.
Sonnet is a reasonable choice if cost or speed ever does matter (`TRIAGE_MODEL=claude-sonnet-5-5`).

**Limits:** 9 cases is small, and they were written alongside the prompt; `login_before_signup`
was fixed after its failure was seen, so it is no longer an unseen case. Term matching checks that
the cause is named, not that the reasoning is sound; every diagnosis is saved to read, in
[`eval/results.json`](eval/results.json) (the rerun) and
[`eval/results-first-run.json`](eval/results-first-run.json). Each run is billed, so the evaluation isn't in CI.
