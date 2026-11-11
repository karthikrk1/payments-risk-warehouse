"""Carve a small, deterministic sample out of the full PaySim CSV for CI.

CI can't download the 470 MB Kaggle file, so it builds the whole pipeline on this sample instead
(see `make ci-build`). The sample keeps what the tests and features need:

- every row PaySim's legacy rule flagged (isFlaggedFraud = 1);
- every zero-amount row (they trigger an expected warning test);
- a share of fraudulent rows;
- every transaction of a set of receiving accounts, so receivers repeat as they do in the full
  data (velocity features need repeats);
- a random share of everything else.

Selection uses MD5 of row numbers and account IDs, so the same input always gives the same sample.
Lines are copied verbatim from the source file, in their original order.

Usage:
    python ingestion/make_sample.py [--csv PATH] [--out PATH]
"""

import argparse
from pathlib import Path

from common import PAYSIM_CSV, REPO_ROOT, connect, paysim_select_sql

SAMPLE_CSV = REPO_ROOT / "data" / "sample" / "paysim_sample.csv"

# Shares are applied to md5(key) mapped to [0, 1); see bucket_sql().
FRAUD_SHARE = 0.07  # about 575 of 8,213 fraudulent rows
RECEIVER_SHARE = 0.0003  # about 170 of 572k customer receivers, with all their transactions
OTHER_SHARE = 0.0005  # about 3,200 of the remaining rows


def bucket_sql(key: str) -> str:
    """SQL for a stable number in [0, 1) from md5(key), independent of the generator's seed."""
    return f"cast(md5_number('sample|' || {key}) % 1000000 as double) / 1000000"


def select_rows(csv_path: Path) -> set[int]:
    """Row numbers (1-based data rows) to keep."""
    con = connect()
    con.execute(f"create table tx as {paysim_select_sql(csv_path)}")
    rows = con.execute(f"""
        select source_row_number
        from tx
        where isFlaggedFraud = 1
           or amount = 0
           or (isFraud = 1 and {bucket_sql("cast(source_row_number as varchar)")} < {FRAUD_SHARE})
           or (nameDest like 'C%' and {bucket_sql("nameDest")} < {RECEIVER_SHARE})
           or {bucket_sql("'other|' || cast(source_row_number as varchar)")} < {OTHER_SHARE}
    """).fetchall()
    return {r[0] for r in rows}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--csv", type=Path, default=PAYSIM_CSV, help="full PaySim CSV")
    parser.add_argument("--out", type=Path, default=SAMPLE_CSV, help="sample CSV to write")
    args = parser.parse_args()

    if not args.csv.exists():
        raise SystemExit(f"PaySim CSV not found at {args.csv}. See the README Quickstart.")

    keep = select_rows(args.csv)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.csv.open() as src, args.out.open("w") as dst:
        dst.write(next(src))  # header
        for row_number, line in enumerate(src, start=1):
            if row_number in keep:
                dst.write(line)

    print(f"Wrote {len(keep):,} rows to {args.out.relative_to(REPO_ROOT)}")


if __name__ == "__main__":
    main()
