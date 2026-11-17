#!/usr/bin/env python3
"""P-SPIKE: one-tile amplification / session cost go-no-go harness.

Offline mode (--demo) exercises the tile path without HATS mounts and records
a structured report. On CANFAR, omit --demo and point --left/--right at pinned
trees to measure real read amplification.
"""

from __future__ import annotations

import argparse
import json
import resource
import sys
import time
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve().parent
_SCRIPTS_ROOT = str(_SCRIPT_DIR.parent)
if _SCRIPTS_ROOT not in sys.path:
    sys.path.insert(0, _SCRIPTS_ROOT)

from pensus.tile_run import main as tile_main  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--campaign", default="spike")
    parser.add_argument("--order", type=int, default=4)
    parser.add_argument("--pixel", type=int, default=0)
    parser.add_argument("--work-root", type=Path, required=True)
    parser.add_argument("--demo", action="store_true", default=True)
    parser.add_argument("--no-demo", action="store_true")
    args = parser.parse_args(argv)

    demo = not args.no_demo
    t0 = time.perf_counter()
    ru0 = resource.getrusage(resource.RUSAGE_SELF)
    rc = tile_main(
        [
            "--campaign",
            args.campaign,
            "--order",
            str(args.order),
            "--pixel",
            str(args.pixel),
            "--work-root",
            str(args.work_root),
            *(["--demo"] if demo else []),
        ]
    )
    elapsed = time.perf_counter() - t0
    ru1 = resource.getrusage(resource.RUSAGE_SELF)
    report = {
        "status": "ok" if rc == 0 else "fail",
        "returncode": rc,
        "demo": demo,
        "elapsed_s": round(elapsed, 4),
        "max_rss_kib_delta": ru1.ru_maxrss - ru0.ru_maxrss,
        "go_no_go": {
            "rule": "offline demo must finish < 30s; real-tree spike sets tile order from amplification",
            "pass": rc == 0 and elapsed < 30.0,
        },
        "work_root": str(args.work_root),
        "tile": {"order": args.order, "pixel": args.pixel},
    }
    out = args.work_root / "logs" / args.campaign
    out.mkdir(parents=True, exist_ok=True)
    (out / "spike-report.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps(report, indent=2, sort_keys=True))
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
