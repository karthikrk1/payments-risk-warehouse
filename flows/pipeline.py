"""Prefect flow for the payments risk warehouse.

Two modes:

- full:   generate entities -> load raw -> dbt build -> analyses, in one go (what `make ingest`,
          `make build` and `make results` do, orchestrated).
- replay: feed the warehouse one batch of simulated time at a time (a day by default), the way it
          would run in production: each batch loads only what had happened by then and runs
          `dbt build`, so the incremental fct_transactions grows batch by batch. It keeps going
          after the last transaction until the last late dispute has arrived. With --compare-to,
          the end result is checked row for row against a full build.

Every batch passes a data-quality gate: if any dbt model or test errors or fails, the flow stops and
reports which ones (from dbt's run_results.json). Loading is retried; failing tests are not, because
they mean the data is wrong, not that something glitched.

Runs without a Prefect server or account: Prefect starts a temporary local API for the run. For the
UI with run history, run `prefect server start` in another terminal first.

Usage:
    python flows/pipeline.py --mode full   [--dataset full|sample]
    python flows/pipeline.py --mode replay [--dataset full|sample] [--batch-hours 24]
                             [--compare-to PATH]
"""

import argparse
import json
import math
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import duckdb
from prefect import flow, get_run_logger, task
from prefect.artifacts import create_markdown_artifact, create_table_artifact

REPO_ROOT = Path(__file__).resolve().parents[1]
DBT_DIR = REPO_ROOT / "risk_warehouse"
BIN = Path(sys.executable).parent
PYTHON = str(BIN / "python")
DBT = str(BIN / "dbt")


@dataclass(frozen=True)
class Dataset:
    """Where a dataset's inputs live and which dbt target builds it."""

    csv: Path
    generated: Path
    target: str


DATASETS = {
    "full": Dataset(REPO_ROOT / "data/raw/paysim.csv", REPO_ROOT / "data/raw/generated", "dev"),
    "sample": Dataset(
        REPO_ROOT / "data/sample/paysim_sample.csv", REPO_ROOT / "data/raw/ci_generated", "ci"
    ),
}

DEFAULT_WAREHOUSES = {
    ("full", "full"): REPO_ROOT / "warehouse/risk.duckdb",
    ("sample", "full"): REPO_ROOT / "warehouse/ci.duckdb",
    ("full", "replay"): REPO_ROOT / "warehouse/replay.duckdb",
    ("sample", "replay"): REPO_ROOT / "warehouse/ci_replay.duckdb",
}


COUNTED_TABLES = [
    "core.fct_transactions",
    "core.fct_logins",
    "core.fct_disputes",
    "core.dim_account",
    "ml.ml_transaction_features",
]


class DataQualityGateError(RuntimeError):
    """dbt reported failing tests or erroring models; nothing downstream should run."""


def run_command(args: list[str], cwd: Path = REPO_ROOT, env: dict | None = None) -> str:
    """Run a command, log its output, and raise with the tail of the output if it fails."""
    logger = get_run_logger()
    result = subprocess.run(
        args, cwd=cwd, env={**os.environ, **(env or {})}, capture_output=True, text=True
    )
    output = (result.stdout + result.stderr).strip()
    if result.returncode != 0:
        tail = "\n".join(output.splitlines()[-25:])
        raise RuntimeError(f"{' '.join(args[:3])} failed (exit {result.returncode}):\n{tail}")
    # Enough to show every line of the short commands this flow runs (e.g. all tables compared).
    for line in output.splitlines()[-40:]:
        logger.info(line)
    return output


@task(name="generate-entities")
def generate_entities(dataset: Dataset) -> None:
    run_command(
        [
            PYTHON,
            "ingestion/generate_entities.py",
            "--csv",
            str(dataset.csv),
            "--out",
            str(dataset.generated),
        ]
    )


@task(name="load-raw", retries=2, retry_delay_seconds=5, timeout_seconds=600)
def load_raw(dataset: Dataset, warehouse: Path, as_of_hours: int | None = None) -> None:
    args = [
        PYTHON,
        "ingestion/load_raw.py",
        "--csv",
        str(dataset.csv),
        "--generated",
        str(dataset.generated),
        "--warehouse",
        str(warehouse),
    ]
    if as_of_hours is not None:
        args += ["--as-of-hours", str(as_of_hours)]
    run_command(args)


@task(name="dbt-deps", retries=2, retry_delay_seconds=10)
def dbt_deps() -> None:
    run_command([DBT, "deps"], cwd=DBT_DIR)


@task(name="dbt-build", timeout_seconds=1800)
def dbt_build(target: str, warehouse: Path, full_refresh: bool = False) -> dict:
    """Run dbt build and enforce the data-quality gate from dbt's own run_results.json."""
    args = [DBT, "build", "--target", target] + (["--full-refresh"] if full_refresh else [])
    results_file = DBT_DIR / "target" / "run_results.json"
    # A stale file from an earlier run must never be read as this run's result.
    results_file.unlink(missing_ok=True)
    result = subprocess.run(
        args,
        cwd=DBT_DIR,
        env={**os.environ, "WAREHOUSE_PATH": str(warehouse)},
        capture_output=True,
        text=True,
    )
    if not results_file.exists():
        raise RuntimeError(f"dbt build produced no run_results.json:\n{result.stdout[-2000:]}")

    results = json.loads(results_file.read_text())["results"]
    by_status: dict[str, int] = {}
    for r in results:
        by_status[r["status"]] = by_status.get(r["status"], 0) + 1
    failed = [r for r in results if r["status"] in ("error", "fail")]
    summary = {
        "nodes": len(results),
        "passed": by_status.get("pass", 0) + by_status.get("success", 0),
        "warned": by_status.get("warn", 0),
        "failed": len(failed),
    }

    if failed:
        lines = [
            f"- `{r['unique_id']}`: {r['status']}"
            + (f", {r['failures']} failing rows" if r.get("failures") else "")
            + (f" ({(r.get('message') or '').splitlines()[0][:200]})" if r.get("message") else "")
            for r in failed
        ]
        raise DataQualityGateError(
            f"dbt build: {len(failed)} of {len(results)} nodes failed the gate:\n"
            + "\n".join(lines)
        )
    if result.returncode != 0:
        raise RuntimeError(f"dbt build exited {result.returncode}:\n{result.stdout[-2000:]}")
    get_run_logger().info(f"dbt build passed the gate: {summary}")
    return summary


@task(name="warehouse-counts")
def warehouse_counts(warehouse: Path) -> dict:
    with duckdb.connect(str(warehouse), read_only=True) as con:
        return {
            table: con.execute(f"select count(*) from {table}").fetchone()[0]
            for table in COUNTED_TABLES
        }


@task(name="run-analyses")
def run_analyses(dataset_name: str, warehouse: Path) -> str:
    target = DATASETS[dataset_name].target
    output = run_command(
        [PYTHON, "scripts/run_analyses.py", "--warehouse", str(warehouse), "--target", target],
        env={"WAREHOUSE_PATH": str(warehouse)},
    )
    note = (
        "Computed on the **full** dataset."
        if dataset_name == "full"
        else "Computed on the **CI sample**: fraud is over-represented on purpose, so these rates "
        "are not meaningful. Results come from the full dataset."
    )
    create_markdown_artifact(key=f"results-{dataset_name}", markdown=f"{note}\n\n{output}")
    return output


@task(name="compare-warehouses")
def compare_warehouses(expected: Path, actual: Path) -> None:
    run_command([PYTHON, "scripts/compare_warehouses.py", str(expected), str(actual)])


def replay_horizon_hours(dataset: Dataset) -> int:
    """Hours after paysim_start_date needed for every source event to have happened."""
    # Reuse ingestion's PaySim reader and anchor date so the horizon matches what load_raw loads.
    sys.path.insert(0, str(REPO_ROOT / "ingestion"))
    from common import paysim_select_sql, paysim_start_date

    with duckdb.connect() as con:
        max_step = con.execute(
            f"select max(step) from ({paysim_select_sql(dataset.csv)})"
        ).fetchone()[0]
        latest = con.execute(f"""
            select greatest(
                (select max(signup_ts) from read_parquet('{dataset.generated}/accounts.parquet')),
                (select max(first_seen_ts)
                    from read_parquet('{dataset.generated}/devices.parquet')),
                (select max(login_ts) from read_parquet('{dataset.generated}/logins.parquet')),
                (select max(filed_ts) from read_parquet('{dataset.generated}/disputes.parquet'))
            ) - timestamp '{paysim_start_date()}'
        """).fetchone()[0]
    entity_hours = math.floor(latest.total_seconds() / 3600) + 1  # cutoff is exclusive
    return max(max_step, entity_hours)


def fresh_warehouse(path: Path) -> None:
    for p in (path, path.with_name(path.name + ".wal")):
        p.unlink(missing_ok=True)


@flow(name="payments-risk-pipeline", log_prints=True, timeout_seconds=4 * 3600)
def pipeline(
    mode: str = "full",
    dataset_name: str = "full",
    batch_hours: int = 24,
    warehouse: str | None = None,
    compare_to: str | None = None,
) -> dict:
    """Run the warehouse pipeline end to end (mode="full") or replay it batch by batch."""
    if mode not in ("full", "replay") or dataset_name not in DATASETS:
        raise ValueError("mode must be full|replay and dataset_name full|sample")
    dataset = DATASETS[dataset_name]
    target_path = Path(warehouse) if warehouse else DEFAULT_WAREHOUSES[(dataset_name, mode)]
    started = time.perf_counter()

    generate_entities(dataset)
    dbt_deps()

    if mode == "full":
        load_raw(dataset, target_path)
        build = dbt_build(dataset.target, target_path)
        counts = warehouse_counts(target_path)
        run_analyses(dataset_name, target_path)
        create_markdown_artifact(
            key=f"run-summary-{dataset_name}",
            markdown=(
                f"## Full build ({dataset_name} dataset)\n\n"
                f"dbt: {build['passed']} passed, {build['warned']} warned, "
                f"{build['failed']} failed of {build['nodes']} nodes.\n\n"
                + "\n".join(f"- `{t}`: {n:,} rows" for t, n in counts.items())
                + f"\n\nTook {time.perf_counter() - started:.0f} s."
            ),
        )
        return {"mode": mode, "dbt": build, "counts": counts}

    # Replay: start from an empty warehouse and feed it one batch of simulated time at a time.
    fresh_warehouse(target_path)
    horizon = replay_horizon_hours(dataset)
    batches = math.ceil(horizon / batch_hours)
    print(f"Replaying {horizon} hours in {batches} batches of {batch_hours} hours")

    rows = []
    previous = 0
    for batch in range(1, batches + 1):
        as_of = batch * batch_hours
        batch_started = time.perf_counter()
        load_raw(dataset, target_path, as_of_hours=as_of)
        build = dbt_build(dataset.target, target_path, full_refresh=(batch == 1))
        counts = warehouse_counts(target_path)
        transactions = counts["core.fct_transactions"]
        rows.append(
            {
                "batch": batch,
                "as_of_hour": as_of,
                "new_transactions": transactions - previous,
                "total_transactions": transactions,
                "disputes": counts["core.fct_disputes"],
                "tests_passed": build["passed"],
                "tests_warned": build["warned"],
                "seconds": round(time.perf_counter() - batch_started, 1),
            }
        )
        previous = transactions
        print(
            f"Batch {batch}/{batches} (as of hour {as_of}): +{rows[-1]['new_transactions']:,} "
            f"transactions, {counts['core.fct_disputes']:,} disputes"
        )

    create_table_artifact(
        key=f"replay-{dataset_name}",
        table=rows,
        description=f"Replay of the {dataset_name} dataset in {batch_hours}-hour batches.",
    )
    if compare_to:
        compare_warehouses(Path(compare_to), target_path)
    run_analyses(dataset_name, target_path)
    return {"mode": mode, "batches": batches, "counts": warehouse_counts(target_path)}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--mode", choices=["full", "replay"], default="full")
    parser.add_argument("--dataset", choices=sorted(DATASETS), default="full")
    parser.add_argument("--batch-hours", type=int, default=24)
    parser.add_argument("--warehouse", help="DuckDB file to build (default depends on mode)")
    parser.add_argument("--compare-to", help="after replay, require equality with this warehouse")
    args = parser.parse_args()
    pipeline(
        mode=args.mode,
        dataset_name=args.dataset,
        batch_hours=args.batch_hours,
        warehouse=args.warehouse,
        compare_to=args.compare_to,
    )


if __name__ == "__main__":
    main()
