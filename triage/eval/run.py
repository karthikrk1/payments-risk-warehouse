"""Run the triage evaluation: plant each case's breakage, build, diagnose, grade.

    python -m triage.eval.run [--models claude-opus-5-5 claude-sonnet-5-5] [--cases NAME ...]
                              [--repeats 1] [--build-only] [--workdir DIR]

1. Loads the committed CI sample into a scratch raw warehouse once.
2. For each case: copies the dbt project and that warehouse, applies the case, runs
   `dbt build --target ci`, and builds the triage context (the "what changed" section is the diff
   between the clean and broken project, as git would show it on a branch). A case whose change
   does not make the build fail is reported as invalid and not sent to any model.
3. Diagnoses every valid case with every model (each call is billed: about $0.05-0.15 on Opus 5.5)
   and grades it against cases.py. Writes triage/eval/results.json and prints a summary table.

--build-only stops after step 2 (no API calls), to check the cases themselves.
"""

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import duckdb

from triage.context import REPO_ROOT, build_context
from triage.diagnose import DEFAULT_MODEL, TriageUnavailable, diagnose
from triage.eval.cases import CASES, Case

PROJECT = REPO_ROOT / "risk_warehouse"
VENV_BIN = Path(sys.executable).parent
SAMPLE_CSV = REPO_ROOT / "data" / "sample" / "paysim_sample.csv"
RESULTS = Path(__file__).with_name("results.json")  # overwritten by each run
COPY_IGNORE = shutil.ignore_patterns("target", "logs", ".user.yml")


def run(cmd: list, cwd: Path, env: dict | None = None) -> subprocess.CompletedProcess:
    return subprocess.run(
        [str(c) for c in cmd], cwd=cwd, env=env, capture_output=True, text=True, check=False
    )


def prepare_base(workdir: Path) -> tuple[Path, Path]:
    """A clean project copy (with packages) and a warehouse holding only the raw sample."""
    base_project = workdir / "base" / "risk_warehouse"
    base_db = workdir / "base" / "base.duckdb"
    if base_project.exists() and base_db.exists():
        return base_project, base_db
    shutil.copytree(PROJECT, base_project, ignore=COPY_IGNORE, dirs_exist_ok=True)
    generated = workdir / "base" / "generated"
    for cmd in (
        [VENV_BIN / "python", "ingestion/generate_entities.py", "--csv", SAMPLE_CSV,
         "--out", generated],
        [VENV_BIN / "python", "ingestion/load_raw.py", "--csv", SAMPLE_CSV,
         "--generated", generated, "--warehouse", base_db],
    ):  # fmt: skip
        result = run(cmd, REPO_ROOT)
        if result.returncode != 0:
            raise SystemExit(f"Base setup failed: {' '.join(map(str, cmd))}\n{result.stderr}")
    return base_project, base_db


def project_diff(base_project: Path, case_project: Path) -> str:
    """The diff between the clean and broken project, with paths as they are in the repo."""
    # Exit code 1 means "differences found", so it is not checked.
    result = run(["git", "diff", "--no-index", "--", base_project, case_project], REPO_ROOT)
    diff = result.stdout
    for root in (base_project, case_project):
        diff = diff.replace(str(root).lstrip("/"), "risk_warehouse")
    return diff.strip() or "(no changes to project code since origin/main)"


def build_case(case: Case, workdir: Path, base_project: Path, base_db: Path) -> dict:
    case_dir = workdir / "cases" / case.name
    if case_dir.exists():
        shutil.rmtree(case_dir)
    project = case_dir / "risk_warehouse"
    shutil.copytree(base_project, project)
    db = case_dir / "ci.duckdb"
    shutil.copy(base_db, db)

    for edit in case.edits:
        path = project / edit.path
        text = path.read_text()
        if text.count(edit.old) != 1:
            raise SystemExit(f"{case.name}: text to replace not found once in {edit.path}")
        path.write_text(text.replace(edit.old, edit.new))
    if case.raw_sql:
        with duckdb.connect(str(db)) as con:
            for statement in case.raw_sql:
                con.execute(statement)

    # Diff before building, so dbt's own logs and target/ files are not part of it.
    changes = project_diff(base_project, project)
    env = {**os.environ, "WAREHOUSE_PATH": str(db), "DBT_SEND_ANONYMOUS_USAGE_STATS": "false"}
    build = run([VENV_BIN / "dbt", "build", "--target", "ci"], project, env)
    (case_dir / "build.log").write_text(build.stdout + build.stderr)
    target = project / "target"

    record = {"case": case.name, "expected_category": case.expected_category}
    if not (target / "run_results.json").exists():
        record["invalid"] = "dbt produced no run_results.json (see build.log)"
        return record
    context = build_context(target, db, changes=changes)
    if context is None:
        record["invalid"] = "the planted change did not make the build fail"
        return record
    text = context.render()
    (case_dir / "context.md").write_text(text)
    record["failed_nodes"] = [n.unique_id for n in context.failed]
    record["context_chars"] = len(text)
    return record


def grade(case: Case, diagnosis: dict) -> dict:
    haystack = " ".join(
        [diagnosis["summary"], diagnosis["root_cause"], diagnosis["suggested_fix"],
         *diagnosis["suspect_files"]]
    ).lower()  # fmt: skip
    haystack = re.sub(r"\s+", " ", haystack)
    missing = [
        group for group in case.must_mention if not any(t.lower() in haystack for t in group)
    ]
    category_ok = diagnosis["category"] == case.expected_category
    return {"category_ok": category_ok, "missing": missing, "passed": category_ok and not missing}


def diagnose_case(case: Case, model: str, repeat: int, workdir: Path) -> dict:
    text = (workdir / "cases" / case.name / "context.md").read_text()
    started = time.monotonic()
    try:
        diagnosis, meta = diagnose(text, model=model)
    except TriageUnavailable as error:
        return {"case": case.name, "model": model, "repeat": repeat, "error": str(error)}
    d = diagnosis.model_dump()
    return {
        "case": case.name,
        "model": model,
        "repeat": repeat,
        "seconds": round(time.monotonic() - started, 1),
        **{
            k: meta[k]
            for k in ("model_served", "input_tokens", "output_tokens", "estimated_cost_usd")
        },  # fmt: skip
        "diagnosis": d,
        **grade(case, d),
    }


def summarise(runs: list[dict], models: list[str]) -> str:
    lines = [
        "| Model | Passed | Category right | Avg cost | Avg seconds |",
        "|---|---|---|---|---|",
    ]
    for model in models:
        rs = [r for r in runs if r["model"] == model and "error" not in r]
        errors = sum(1 for r in runs if r["model"] == model and "error" in r)
        if not rs:
            lines.append(f"| {model} | - | - | - | - ({errors} errors) |")
            continue
        n = len(rs)
        lines.append(
            f"| {model} | {sum(r['passed'] for r in rs)}/{n + errors} "
            f"| {sum(r['category_ok'] for r in rs)}/{n + errors} "
            f"| ${sum(r['estimated_cost_usd'] for r in rs) / n:.3f} "
            f"| {sum(r['seconds'] for r in rs) / n:.0f} |"
        )
    lines += ["", "| Case | " + " | ".join(models) + " |", "|---|" + "---|" * len(models)]
    for case in dict.fromkeys(r["case"] for r in runs):
        cells = []
        for model in models:
            rs = [r for r in runs if r["case"] == case and r["model"] == model]
            cells.append(
                " ".join(
                    "error"
                    if "error" in r
                    else "pass"
                    if r["passed"]
                    else f"FAIL ({r['diagnosis']['category']}"
                    + (f"; missing {r['missing']}" if r["missing"] else "")
                    + ")"
                    for r in rs
                )  # fmt: skip
            )
        lines.append(f"| {case} | " + " | ".join(cells) + " |")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--models", nargs="+", default=[DEFAULT_MODEL, "claude-sonnet-5-5"])
    parser.add_argument("--cases", nargs="+", default=None, help="case names (default: all)")
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--build-only", action="store_true", help="build cases; no API calls")
    parser.add_argument("--workdir", type=Path, default=None)
    args = parser.parse_args()

    cases = [c for c in CASES if args.cases is None or c.name in args.cases]
    workdir = args.workdir or Path(tempfile.mkdtemp(prefix="triage-eval-"))
    workdir.mkdir(parents=True, exist_ok=True)
    print(f"Working in {workdir}")

    base_project, base_db = prepare_base(workdir)
    print(f"Building {len(cases)} broken copies of the project...")
    with ThreadPoolExecutor(max_workers=3) as pool:
        built = list(pool.map(lambda c: build_case(c, workdir, base_project, base_db), cases))
    for record in built:
        status = record.get("invalid") or f"fails: {', '.join(record['failed_nodes'])}"
        print(f"  {record['case']:<32} {status}")
    valid = [c for c, r in zip(cases, built, strict=True) if "invalid" not in r]
    if args.build_only:
        return

    jobs = [(c, m, i) for c in valid for m in args.models for i in range(args.repeats)]
    print(f"Diagnosing: {len(jobs)} calls...")
    with ThreadPoolExecutor(max_workers=4) as pool:
        runs = list(pool.map(lambda j: diagnose_case(*j, workdir), jobs))

    RESULTS.write_text(json.dumps({"cases": built, "runs": runs}, indent=2, default=str) + "\n")
    print(summarise(runs, args.models))
    print(f"\nTotal estimated cost: ${sum(r.get('estimated_cost_usd', 0) for r in runs):.2f}")
    print(f"Full results: {RESULTS.relative_to(REPO_ROOT)}")


if __name__ == "__main__":
    main()
