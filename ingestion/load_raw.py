"""Load PaySim and the generated entities into the DuckDB warehouse's `raw` schema.

Tables are replaced on every run, so loading is idempotent. Each gets a _loaded_at timestamp,
which dbt source freshness checks can use.

    raw.paysim_transactions   from data/raw/paysim.csv (+ source_row_number)
    raw.app_accounts          from data/raw/generated/accounts.parquet
    raw.app_devices           from data/raw/generated/devices.parquet
    raw.app_logins            from data/raw/generated/logins.parquet
    raw.app_disputes          from data/raw/generated/disputes.parquet

Run generate_entities.py first.

Usage:
    python ingestion/load_raw.py [--csv PATH] [--generated DIR] [--warehouse PATH]
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
    sql_literal,
)

ENTITY_TABLES = ["accounts", "devices", "logins", "disputes"]


def count_data_rows(csv_path: Path) -> int:
    """Lines in the file minus the header, to check nothing was dropped on load."""
    with csv_path.open("rb") as f:
        return sum(1 for _ in f) - 1


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--csv", type=Path, default=PAYSIM_CSV, help="PaySim CSV path")
    parser.add_argument("--generated", type=Path, default=GENERATED_DIR, help="entity Parquet dir")
    parser.add_argument("--warehouse", type=Path, default=WAREHOUSE_PATH, help="DuckDB file")
    args = parser.parse_args()

    if not args.csv.exists():
        raise SystemExit(f"PaySim CSV not found at {args.csv}. See the README Quickstart.")
    missing = [t for t in ENTITY_TABLES if not (args.generated / f"{t}.parquet").exists()]
    if missing:
        raise SystemExit(
            f"Generated files missing: {', '.join(missing)}. "
            "Run python ingestion/generate_entities.py first."
        )

    started = time.perf_counter()
    args.warehouse.parent.mkdir(parents=True, exist_ok=True)
    con = connect(args.warehouse)
    con.execute("create schema if not exists raw")

    sources = {"paysim_transactions": f"({paysim_select_sql(args.csv)})"}
    for table in ENTITY_TABLES:
        parquet = args.generated / f"{table}.parquet"
        sources[f"app_{table}"] = f"read_parquet({sql_literal(parquet)})"

    for table, source in sources.items():
        con.execute(f"""
            create or replace table raw.{table} as
            select *, current_timestamp as _loaded_at
            from {source}
        """)
        rows = con.execute(f"select count(*) from raw.{table}").fetchone()[0]
        print(f"  raw.{table:<20} {rows:>11,} rows")

    expected = count_data_rows(args.csv)
    loaded = con.execute("select count(*) from raw.paysim_transactions").fetchone()[0]
    if loaded != expected:
        raise SystemExit(f"PaySim row count mismatch: file has {expected:,}, loaded {loaded:,}")

    con.close()
    print(
        f"Loaded {len(sources)} tables into {args.warehouse.name} in "
        f"{time.perf_counter() - started:.1f}s"
    )


if __name__ == "__main__":
    main()
