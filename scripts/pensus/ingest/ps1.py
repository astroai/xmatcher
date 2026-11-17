#!/usr/bin/env python3
"""I-PS1: pin Pan-STARRS product for pensus v0.1 interim northern backbone."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-root", type=Path, default=Path("/arc/projects/k-pop/pensus"))
    parser.add_argument(
        "--product",
        choices=("mean_object", "dr1_hats"),
        default="dr1_hats",
        help="mean_object when CDS/local mean tree available; else registry ps1_dr1",
    )
    parser.add_argument("--print-commands", action="store_true")
    args = parser.parse_args(argv)
    alias = "ps1_dr1" if args.product == "dr1_hats" else "ps1_mean_object"
    dest = args.work_root / "inputs" / "ps1" / args.product
    plan = {
        "product": args.product,
        "alias": alias,
        "dest": str(dest),
        "note": "D-PS1: prefer mean object when pinned; dr1_hats is interim registry path",
        "commands": [
            f"pixi run xmatcher sync {alias} --output {dest}",
            f"export XMATCHER_PS1_HATS={dest}",
        ],
    }
    print(json.dumps(plan, indent=2, sort_keys=True))
    if args.print_commands:
        for cmd in plan["commands"]:
            print(cmd)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
