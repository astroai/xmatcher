#!/usr/bin/env python3
"""Enumerate missing pensus tiles and emit headless submit commands."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve().parent
_SCRIPTS_ROOT = str(_SCRIPT_DIR.parent)
if _SCRIPTS_ROOT not in sys.path:
    sys.path.insert(0, _SCRIPTS_ROOT)

from pensus.tile_geometry import tile_ids  # noqa: E402
from pensus.tile_markers import missing_tiles  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--campaign", required=True)
    parser.add_argument("--order", type=int, required=True)
    parser.add_argument("--work-root", type=Path, default=Path("/arc/projects/k-pop/pensus"))
    parser.add_argument("--limit", type=int, default=0, help="Max tiles to emit (0 = all missing)")
    parser.add_argument("--pixels", type=int, nargs="*", default=None)
    parser.add_argument("--demo", action="store_true")
    parser.add_argument("--print-commands", action="store_true")
    args = parser.parse_args(argv)

    markers_dir = args.work_root / "tiles" / args.campaign / "markers"
    pixels = list(args.pixels) if args.pixels is not None else list(tile_ids(args.order))
    todo = missing_tiles(markers_dir, args.order, pixels)
    if args.limit > 0:
        todo = todo[: args.limit]

    commands = []
    for pixel in todo:
        cmd = [
            "python",
            "scripts/pensus/tile_run.py",
            "--campaign",
            args.campaign,
            "--order",
            str(args.order),
            "--pixel",
            str(pixel),
            "--work-root",
            str(args.work_root),
        ]
        if args.demo:
            cmd.append("--demo")
        commands.append(cmd)

    report = {
        "campaign": args.campaign,
        "order": args.order,
        "missing": len(todo),
        "pixels": todo,
    }
    print(json.dumps(report, sort_keys=True))
    if args.print_commands:
        for cmd in commands:
            print(" ".join(cmd))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
