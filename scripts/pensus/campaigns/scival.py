#!/usr/bin/env python3
"""P-SCIVAL: science validation checklist gates for a pensus draft view."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

GATES = [
    "sed_coverage_by_footprint",
    "qso_loci_midir_uv",
    "stellar_cmd_gaia_ps1",
    "pristine_feh_idjoin_null_rate",
    "null_rate_ceilings",
]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--view", required=True)
    parser.add_argument("--draft", type=Path, required=True)
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args(argv)

    report = {
        "view": args.view,
        "draft": str(args.draft),
        "gates": [
            {"name": g, "status": "pending", "threshold": "set in pilot/spec"} for g in GATES
        ],
        "ok": False,
        "note": "Run numeric gates on CANFAR once draft view parquet exists.",
    }
    text = json.dumps(report, indent=2, sort_keys=True) + "\n"
    print(text)
    if args.write:
        args.draft.mkdir(parents=True, exist_ok=True)
        (args.draft / "scival-report.json").write_text(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
