"""Run the dbt analyses in risk_warehouse/analyses/ and print each result as a Markdown table.

The README's Results section is pasted from this output, so every number there can be
regenerated with `make results`. Runs against the full warehouse (warehouse/risk.duckdb) by
default; results from the CI sample are not meaningful (its fraud rate is skewed).

Usage:
    python scripts/run_analyses.py [--warehouse PATH] [--target TARGET]
"""

import argparse
import subprocess
import sys
from pathlib import Path

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[1]
DBT_DIR = REPO_ROOT / "risk_warehouse"
COMPILED_DIR = DBT_DIR / "target" / "compiled" / "risk_warehouse" / "analyses"

# Order matches the README's Results section.
ANALYSES = [
    "fraud_rate_by_type",
    "legacy_rule_baseline",
    "balance_leakage_trap",
    "feature_rules",
]


def markdown_table(columns: list[str], rows: list[tuple]) -> str:
    def cell(value: object) -> str:
        if isinstance(value, int) and not isinstance(value, bool):
            return f"{value:,}"
        return "" if value is None else str(value)

    lines = ["| " + " | ".join(columns) + " |", "|" + "---|" * len(columns)]
    lines += ["| " + " | ".join(cell(v) for v in row) + " |" for row in rows]
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--warehouse", type=Path, default=REPO_ROOT / "warehouse" / "risk.duckdb")
    parser.add_argument("--target", default="dev", help="dbt target to compile with")
    args = parser.parse_args()

    dbt = REPO_ROOT / ".venv" / "bin" / "dbt"
    compiled = subprocess.run(
        [str(dbt), "compile", "--quiet", "--target", args.target, "--select", *ANALYSES],
        cwd=DBT_DIR,
        capture_output=True,
        text=True,
    )
    if compiled.returncode != 0:
        sys.exit(f"dbt compile failed:\n{compiled.stdout}{compiled.stderr}")

    con = duckdb.connect(str(args.warehouse), read_only=True)
    for name in ANALYSES:
        sql = (COMPILED_DIR / f"{name}.sql").read_text()
        cursor = con.execute(sql)
        columns = [d[0] for d in cursor.description]
        print(f"### {name}\n\n{markdown_table(columns, cursor.fetchall())}\n")


if __name__ == "__main__":
    main()
