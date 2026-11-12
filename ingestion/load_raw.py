"""Load PaySim and the generated entities into the DuckDB warehouse's `raw` schema.

Tables are replaced on every run, so loading is idempotent. Each gets a _loaded_at timestamp,
which dbt source freshness checks can use.

    raw.paysim_transactions   from data/raw/paysim.csv (+ source_row_number)
    raw.app_accounts          from data/raw/generated/accounts.parquet
    raw.app_devices           from data/raw/generated/devices.parquet
    raw.app_logins            from data/raw/generated/logins.parquet
    raw.app_disputes          from data/raw/generated/disputes.parquet

With --as-of-hours H, only what had happened before paysim_start_date + H hours is loaded, as if the
source systems were read at that moment: transactions with step <= H, and accounts, devices, logins
and disputes whose signup / first-seen / login / filed time is before that cutoff. This is what
replay mode (flows/pipeline.py) uses to feed the warehouse one simulated day at a time.
source_row_number is assigned over the whole file before filtering, so a transaction has the same
transaction_id whether it is loaded on its own day or in a full load.

Run generate_entities.py first.

Usage:
    python ingestion/load_raw.py [--csv PATH] [--generated DIR] [--warehouse PATH] [--as-of-hours H]
"""

import argparse
import time
from pathlib import Path

from common import (
    GENERATED_DIR,
    PAYSIM_CSV,
    WAREHOUSE_PATH,
    connect,
    paysim_select_sql,
    paysim_start_date,
    sql_literal,
)

ENTITY_TABLES = ["accounts", "devices", "logins", "disputes"]

# The time column that says when each entity became known to the source system.
ENTITY_TIME_COLUMNS = {
    "accounts": "signup_ts",
    "devices": "first_seen_ts",
    "logins": "login_ts",
    "disputes": "filed_ts",
}


def count_data_rows(csv_path: Path) -> int:
    """Lines in the file minus the header, to check nothing was dropped on load."""
    with csv_path.open("rb") as f:
        return sum(1 for _ in f) - 1


def source_queries(csv: Path, generated: Path, as_of_hours: int | None) -> dict[str, str]:
    """SELECT for each raw table, limited to what existed before the cutoff when one is given."""
    paysim = f"select * from ({paysim_select_sql(csv)})"
    if as_of_hours is not None:
        paysim += f" where step <= {int(as_of_hours)}"
    queries = {"paysim_transactions": paysim}

    cutoff = f"(timestamp {sql_literal(paysim_start_date())} + to_hours({int(as_of_hours or 0)}))"
    for table in ENTITY_TABLES:
        query = f"select * from read_parquet({sql_literal(generated / f'{table}.parquet')})"
        if as_of_hours is not None:
            query += f" where {ENTITY_TIME_COLUMNS[table]} < {cutoff}"
        queries[f"app_{table}"] = query
    return queries


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--csv", type=Path, default=PAYSIM_CSV, help="PaySim CSV path")
    parser.add_argument("--generated", type=Path, default=GENERATED_DIR, help="entity Parquet dir")
    parser.add_argument("--warehouse", type=Path, default=WAREHOUSE_PATH, help="DuckDB file")
    parser.add_argument(
        "--as-of-hours",
        type=int,
        default=None,
        help="load only data from before paysim_start_date + this many hours (replay)",
    )
    args = parser.parse_args()

    if not args.csv.exists():
        raise SystemExit(f"PaySim CSV not found at {args.csv}. See the README Quickstart.")
    missing = [t for t in ENTITY_TABLES if not (args.generated / f"{t}.parquet").exists()]
    if missing:
        raise SystemExit(
            f"Generated files missing: {', '.join(missing)}. "
            "Run python ingestion/generate_entities.py first."
        )
    if args.as_of_hours is not None and args.as_of_hours < 1:
        raise SystemExit("--as-of-hours must be at least 1")

    started = time.perf_counter()
    args.warehouse.parent.mkdir(parents=True, exist_ok=True)
    con = connect(args.warehouse)
    con.execute("create schema if not exists raw")

    for table, query in source_queries(args.csv, args.generated, args.as_of_hours).items():
        con.execute(f"""
            create or replace table raw.{table} as
            select *, current_timestamp as _loaded_at
            from ({query})
        """)
        rows = con.execute(f"select count(*) from raw.{table}").fetchone()[0]
        print(f"  raw.{table:<20} {rows:>11,} rows")

    # A full load must contain every data row of the file.
    if args.as_of_hours is None:
        expected = count_data_rows(args.csv)
        loaded = con.execute("select count(*) from raw.paysim_transactions").fetchone()[0]
        if loaded != expected:
            raise SystemExit(f"PaySim row count mismatch: file has {expected:,}, loaded {loaded:,}")

    con.close()
    as_of = "" if args.as_of_hours is None else f" (as of hour {args.as_of_hours})"
    print(
        f"Loaded {len(ENTITY_TABLES) + 1} tables into {args.warehouse.name}{as_of} in "
        f"{time.perf_counter() - started:.1f}s"
    )


if __name__ == "__main__":
    main()
