"""Diagnose the most recent failed dbt build.

    python -m triage [--target-dir risk_warehouse/target] [--warehouse warehouse/risk.duckdb]
                     [--model claude-opus-5-5] [--changes-file DIFF] [--dry-run]

--dry-run prints the context that would be sent (and its size) without calling the API. The
diagnosis is printed and saved to <target-dir>/triage.md. Exits 0 when nothing failed or the
diagnosis was produced, 2 when triage was unavailable (the build's own failure is unaffected).
"""

import argparse
import os
import sys
from pathlib import Path

from triage.context import REPO_ROOT, build_context
from triage.diagnose import TriageUnavailable, diagnose, to_markdown


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--target-dir", type=Path, default=REPO_ROOT / "risk_warehouse" / "target")
    parser.add_argument(
        "--warehouse",
        type=Path,
        default=Path(os.environ.get("WAREHOUSE_PATH", REPO_ROOT / "warehouse" / "risk.duckdb")),
    )
    parser.add_argument(
        "--model", default=None, help="defaults to $TRIAGE_MODEL or claude-opus-5-5"
    )
    parser.add_argument(
        "--changes-file",
        type=Path,
        default=None,
        help="use this diff as 'what changed' instead of git diff against origin/main",
    )
    parser.add_argument("--dry-run", action="store_true", help="print the context; no API call")
    args = parser.parse_args()

    changes = args.changes_file.read_text() if args.changes_file else None
    context = build_context(args.target_dir, args.warehouse, changes=changes)
    if context is None:
        print("The last dbt build has no failed nodes: nothing to triage.")
        return
    text = context.render()
    if args.dry_run:
        print(text)
        print(f"\n[dry run] {len(text):,} characters, roughly {len(text) // 4:,} tokens; not sent.")
        return

    try:
        diagnosis, meta = diagnose(text, model=args.model)
    except TriageUnavailable as error:
        print(f"Triage unavailable: {error}", file=sys.stderr)
        sys.exit(2)
    report = to_markdown(diagnosis, meta)
    (args.target_dir / "triage.md").write_text(report + "\n")
    print(report)


if __name__ == "__main__":
    main()
