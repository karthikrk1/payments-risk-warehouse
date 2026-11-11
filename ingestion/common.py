"""Shared paths, settings and SQL for the ingestion scripts.

Both generate_entities.py and load_raw.py read PaySim through paysim_select_sql(), so a
transaction's source_row_number (transaction_id downstream) is the same in both.
"""

import os
from pathlib import Path

import duckdb
import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = REPO_ROOT / "data" / "raw"
GENERATED_DIR = RAW_DIR / "generated"
WAREHOUSE_PATH = REPO_ROOT / "warehouse" / "risk.duckdb"
DBT_PROJECT_FILE = REPO_ROOT / "risk_warehouse" / "dbt_project.yml"

# The Kaggle file is renamed to paysim.csv on download; override with the PAYSIM_CSV env var.
PAYSIM_CSV = Path(os.environ.get("PAYSIM_CSV", RAW_DIR / "paysim.csv"))

# Changing the seed changes every generated value; keep it fixed so output is reproducible.
DEFAULT_SEED = "prw-v1"

# Declared rather than sniffed, so a changed or truncated file fails loudly instead of loading
# with different types.
PAYSIM_COLUMNS = {
    "step": "INTEGER",
    "type": "VARCHAR",
    "amount": "DOUBLE",
    "nameOrig": "VARCHAR",
    "oldbalanceOrg": "DOUBLE",
    "newbalanceOrig": "DOUBLE",
    "nameDest": "VARCHAR",
    "oldbalanceDest": "DOUBLE",
    "newbalanceDest": "DOUBLE",
    "isFraud": "INTEGER",
    "isFlaggedFraud": "INTEGER",
}


def sql_literal(value: object) -> str:
    """Quote a value as a SQL string literal."""
    return "'" + str(value).replace("'", "''") + "'"


def paysim_start_date() -> str:
    """The PaySim anchor date, read from dbt_project.yml so dbt and Python share one value."""
    project = yaml.safe_load(DBT_PROJECT_FILE.read_text())
    return str(project["vars"]["paysim_start_date"])


def connect(database: Path | str = ":memory:") -> duckdb.DuckDBPyConnection:
    """Open DuckDB with file order preserved, which source_row_number depends on."""
    con = duckdb.connect(str(database))
    con.execute("set preserve_insertion_order = true")
    return con


def register_rnd(con: duckdb.DuckDBPyConnection, seed: str = DEFAULT_SEED) -> None:
    """Define the SQL macro rnd(key): a uniform number in [0, 1) from MD5(seed | key).

    The same seed and key always give the same number, whatever the thread order. A seeded
    random-number generator running inside parallel SQL would not guarantee that.
    Callers build keys like account_id || '|kyc' so each decision draws its own number.
    """
    con.execute(
        "create or replace macro rnd(key) as "
        f"cast(md5_number({sql_literal(seed)} || '|' || key) % 1000000 as double) / 1000000"
    )


def paysim_select_sql(csv_path: Path = PAYSIM_CSV) -> str:
    """SELECT over the PaySim CSV, adding source_row_number (the 1-based data row in the file)."""
    columns = ", ".join(f"'{name}': '{dtype}'" for name, dtype in PAYSIM_COLUMNS.items())
    return f"""
        select
            row_number() over () as source_row_number,
            *
        from read_csv({sql_literal(csv_path)}, header = true, columns = {{{columns}}})
    """


def transaction_ts_sql(step_column: str = "step") -> str:
    """SQL for PaySim's step (an hour index) as a timestamp: anchor + (step - 1) hours."""
    return f"(timestamp {sql_literal(paysim_start_date())} + to_hours({step_column} - 1))"
