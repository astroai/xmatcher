#!/usr/bin/env python3
"""I-GALEX: GUVcat HATS pin (CDS) with fallback build trigger."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-root", type=Path, default=Path("/arc/projects/k-pop/pensus"))
    parser.add_argument("--cds-down", action="store_true")
    parser.add_argument("--print-commands", action="store_true")
    args = parser.parse_args(argv)
    dest = args.work_root / "inputs" / "galex_guvcat"
    if args.cds_down:
        plan = {
            "mode": "fallback_build",
            "dest": str(args.work_root / "hats_build" / "galex_guvcat"),
            "steps": [
                "chunked archive pull → /scratch (≤250 GiB peak)",
                "torchfits stream FITS→parquet",
                "hats-import → k-pop hats_build/galex_guvcat",
                "wipe scratch",
            ],
        }
    else:
        plan = {
            "mode": "cds_hats_sync",
            "alias": "galex_hats",
            "dest": str(dest),
            "commands": [f"pixi run xmatcher sync galex_hats --output {dest}"],
        }
    print(json.dumps(plan, indent=2, sort_keys=True))
    if args.print_commands and "commands" in plan:
        for cmd in plan["commands"]:
            print(cmd)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
