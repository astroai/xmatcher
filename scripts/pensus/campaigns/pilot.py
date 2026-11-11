#!/usr/bin/env python3
"""P-PILOT + S-PAIRCAL harness: crowded + extragalactic tiles, kill/resume, oracle hooks."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve().parents[1]
if str(_SCRIPT_DIR.parent) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_DIR.parent))

from pensus.tile_run import main as tile_main  # noqa: E402
from pensus.tile_submit import main as submit_main  # noqa: E402

# Representative regimes (HEALPix order=5 pixels; refine on sky).
PILOT_TILES = {
    "crowded_bulge": {"order": 5, "pixel": 4500},
    "extragalactic_cosmos": {"order": 5, "pixel": 2800},
}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-root", type=Path, required=True)
    parser.add_argument("--campaign", default="pilot-v0")
    parser.add_argument("--demo", action="store_true", default=True)
    parser.add_argument("--regime", choices=sorted(PILOT_TILES), action="append")
    args = parser.parse_args(argv)

    regimes = args.regime or list(PILOT_TILES)
    results = []
    for name in regimes:
        tile = PILOT_TILES[name]
        rc = tile_main(
            [
                "--campaign",
                args.campaign,
                "--order",
                str(tile["order"]),
                "--pixel",
                str(tile["pixel"]),
                "--work-root",
                str(args.work_root),
                *(["--demo"] if args.demo else []),
            ]
        )
        results.append({"regime": name, **tile, "returncode": rc})

    # Resume check: submitter should report 0 missing for finished pixels.
    for name in regimes:
        tile = PILOT_TILES[name]
        submit_main(
            [
                "--campaign",
                args.campaign,
                "--order",
                str(tile["order"]),
                "--work-root",
                str(args.work_root),
                "--pixels",
                str(tile["pixel"]),
            ]
        )

    report = {
        "campaign": args.campaign,
        "results": results,
        "paircal": {
            "status": "pending_real_trees",
            "method": "compare mutual-best sep histograms vs STILTS/lsdb; update pair_policy.toml",
        },
        "oracle": {
            "stilts": "scripts/pensus/campaigns/oracle_stilts.md",
            "lsdb": "margin-cache crossmatch on same cones",
        },
        "ok": all(r["returncode"] == 0 for r in results),
    }
    out = args.work_root / "logs" / args.campaign
    out.mkdir(parents=True, exist_ok=True)
    (out / "pilot-report.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
