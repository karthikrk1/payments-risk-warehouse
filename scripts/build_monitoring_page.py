"""Build the monitoring chart page from docs/monitoring/monitoring.json.

Inlines the committed export into docs/monitoring/template.html and writes one self-contained HTML
file (no external scripts, styles or fonts), which the docs workflow publishes at
<pages>/monitoring/. Run locally with `make monitoring-page` and open the printed path.

Usage:
    python scripts/build_monitoring_page.py [--data PATH] [--out PATH]
"""

import argparse
import json
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
TEMPLATE = REPO_ROOT / "docs" / "monitoring" / "template.html"
DATA = REPO_ROOT / "docs" / "monitoring" / "monitoring.json"
OUT = REPO_ROOT / "site" / "monitoring" / "index.html"
PLACEHOLDER = "/*__MONITORING_DATA__*/"
REQUIRED_KEYS = {
    "exported_at",
    "dataset",
    "rule",
    "counts",
    "known_events",
    "always_zero",
    "monitors",
}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--data", type=Path, default=DATA)
    parser.add_argument("--out", type=Path, default=OUT)
    args = parser.parse_args()

    data = json.loads(args.data.read_text())
    missing = REQUIRED_KEYS - data.keys()
    if missing:
        raise SystemExit(f"{args.data} is missing keys: {sorted(missing)}")
    if not data["monitors"]:
        raise SystemExit(f"{args.data} has no monitors")

    template = TEMPLATE.read_text()
    if template.count(PLACEHOLDER) != 1:
        raise SystemExit(f"{TEMPLATE} must contain {PLACEHOLDER} exactly once")
    # Inside <script>, "</" could end the element early; JSON allows "<\/" as an escape for it.
    payload = json.dumps(data, separators=(",", ":")).replace("</", "<\\/")
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(template.replace(PLACEHOLDER, payload))
    print(f"Wrote {args.out} ({len(data['monitors'])} charts, exported {data['exported_at']})")


if __name__ == "__main__":
    main()
