"""Export the monitoring results from the full warehouse to docs/monitoring/monitoring.json.

The chart page (scripts/build_monitoring_page.py) is built from this file in CI, where only the
CI sample exists; charts of the sample would be noisy and misleading, so the page shows the full
dataset instead, from this committed export. It holds aggregated daily metrics and their scores
only (no transactions). Regenerate it with `make monitoring-export` after `make build`, the same
way the README results come from `make results`.

For each scored day the export also computes the band of values that would *not* be flagged, using
the same rule as mon_daily_anomalies:
    flagged  <=>  |z| > threshold  and  (median = 0  or  |value - median| >= floor * |median|)
    with z = 0.6745 * (value - median) / MAD, and any change flagged when MAD = 0.
So the not-flagged band is  |value - median| <= max(threshold * MAD / 0.6745, floor * |median|)
(the floor term drops when median = 0; the band collapses to the median when MAD = 0). The script
checks that every flagged day falls outside its band and every ok day inside, so the picture can
never contradict the data.

Usage:
    python scripts/export_monitoring.py [--warehouse PATH] [--out PATH]
"""

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
DBT_PROJECT = REPO_ROOT / "risk_warehouse" / "dbt_project.yml"
OUT = REPO_ROOT / "docs" / "monitoring" / "monitoring.json"


def band(median: float, mad: float, threshold: float, floor: float) -> tuple[float, float]:
    """Values that the monitoring rule would not flag, as (low, high)."""
    if mad == 0:
        half = 0.0
    else:
        half = threshold * mad / 0.6745
        if median != 0:
            half = max(half, floor * abs(median))
    return median - half, median + half


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--warehouse", type=Path, default=REPO_ROOT / "warehouse" / "risk.duckdb")
    parser.add_argument("--out", type=Path, default=OUT)
    args = parser.parse_args()

    project_vars = yaml.safe_load(DBT_PROJECT.read_text())["vars"]
    threshold = float(project_vars["monitoring_anomaly_threshold"])
    floor = float(project_vars["monitoring_min_relative_change"])

    con = duckdb.connect(str(args.warehouse), read_only=True)
    transactions = con.execute("select count(*) from core.fct_transactions").fetchone()[0]
    rows = con.execute("""
        select source, metric_name, dimension, metric_date, metric_value, baseline_days,
               baseline_median, baseline_mad, modified_z_score, status, known_event_id
        from monitoring.mon_daily_anomalies
        order by source, metric_name, dimension, metric_date
    """).fetchall()
    events = {
        r[0]: r[1]
        for r in con.execute(
            "select event_id, min(description) from seeds.known_events group by event_id"
        ).fetchall()
    }

    monitors: dict[tuple, dict] = {}
    for src, metric, dim, day, value, days, median, mad, z, status, event in rows:
        key = (src, metric, dim)
        monitor = monitors.setdefault(
            key, {"source": src, "metric": metric, "dimension": dim, "days": []}
        )
        point = {"date": day.isoformat(), "value": value, "status": status}
        if status != "insufficient_baseline":
            low, high = band(median, mad, threshold, floor)
            inside = low - 1e-9 <= value <= high + 1e-9
            if (status == "ok") != inside:
                raise SystemExit(
                    f"Band disagrees with status for {src}.{metric}[{dim}] {day}: "
                    f"value {value}, band {low}..{high}, status {status}"
                )
            point.update(median=median, low=low, high=high, z=z, baseline_days=days, event=event)
        monitor["days"].append(point)

    always_zero = [m for m in monitors.values() if all(p["value"] == 0 for p in m["days"])]
    shown = [m for m in monitors.values() if m not in always_zero]
    counts = {
        s: sum(1 for r in rows if r[9] == s)
        for s in ("ok", "expected", "anomaly", "insufficient_baseline")
    }

    export = {
        "exported_at": datetime.now(UTC).strftime("%Y-%m-%d"),
        "dataset": {"name": "PaySim, full dataset", "transactions": transactions},
        "rule": {
            "baseline_days": project_vars["monitoring_baseline_days"],
            "min_baseline_days": project_vars["monitoring_min_baseline_days"],
            "threshold": threshold,
            "min_relative_change": floor,
        },
        "counts": {
            "measured": len(rows),
            "scored": len(rows) - counts["insufficient_baseline"],
            **counts,
        },  # fmt: skip
        "known_events": events,
        "always_zero": [f"{m['source']}.{m['metric']}[{m['dimension']}]" for m in always_zero],
        "monitors": shown,
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(export, indent=1) + "\n")
    print(
        f"Wrote {args.out.resolve().relative_to(REPO_ROOT)}: {len(shown)} monitors charted, "
        f"{len(always_zero)} always zero, {counts['anomaly']} unexplained anomalies, "
        f"{counts['expected']} expected; bands agree with every day's status."
    )


if __name__ == "__main__":
    main()
