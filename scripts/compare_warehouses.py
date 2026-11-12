"""Check that two DuckDB warehouses hold identical data in their core and ml tables.

Used to prove replay mode is correct: after replaying PaySim one batch at a time, every core and
ml table must equal a one-shot full build, row for row. Exits non-zero, listing the differences,
if not.

Usage:
    python scripts/compare_warehouses.py EXPECTED.duckdb ACTUAL.duckdb [--schemas core ml]
"""

import argparse
import sys
from pathlib import Path

import duckdb


def tables_in(con: duckdb.DuckDBPyConnection, catalog: str, schemas: list[str]) -> set[str]:
    rows = con.execute(
        """
        select table_schema || '.' || table_name
        from information_schema.tables
        where table_catalog = ? and table_schema in (select unnest(?)) and table_type = 'BASE TABLE'
        """,
        [catalog, schemas],
    ).fetchall()
    return {r[0] for r in rows}


def compare(expected: Path, actual: Path, schemas: list[str]) -> list[str]:
    """Differences between the two warehouses, as human-readable lines (empty if identical)."""
    con = duckdb.connect()
    con.execute(f"attach '{expected}' as expected (read_only)")
    con.execute(f"attach '{actual}' as actual (read_only)")

    problems = []
    expected_tables = tables_in(con, "expected", schemas)
    actual_tables = tables_in(con, "actual", schemas)
    for name in sorted(expected_tables ^ actual_tables):
        side = "expected" if name in expected_tables else "actual"
        problems.append(f"{name}: only in {side}")

    for name in sorted(expected_tables & actual_tables):
        counts = con.execute(
            f"select (select count(*) from expected.{name}), (select count(*) from actual.{name})"
        ).fetchone()
        missing = con.execute(
            f"select count(*) from (select * from expected.{name} except all "
            f"select * from actual.{name})"
        ).fetchone()[0]
        extra = con.execute(
            f"select count(*) from (select * from actual.{name} except all "
            f"select * from expected.{name})"
        ).fetchone()[0]
        status = "identical" if missing == extra == 0 else "DIFFERENT"
        print(f"  {name:<34} {counts[0]:>11,} vs {counts[1]:>11,} rows  {status}")
        if missing or extra:
            problems.append(f"{name}: {missing:,} rows missing, {extra:,} unexpected rows")
    return problems


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("expected", type=Path, help="reference warehouse (e.g. a full build)")
    parser.add_argument("actual", type=Path, help="warehouse to check (e.g. a replay)")
    parser.add_argument("--schemas", nargs="+", default=["core", "ml"])
    args = parser.parse_args()

    problems = compare(args.expected, args.actual, args.schemas)
    if problems:
        print("Warehouses differ:\n" + "\n".join(f"  - {p}" for p in problems))
        sys.exit(1)
    print(f"Identical: every {'/'.join(args.schemas)} table matches row for row.")


if __name__ == "__main__":
    main()
