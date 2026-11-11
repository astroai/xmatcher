#!/usr/bin/env python3
"""Pin LSDB / LIneA public HATS trees into k-pop inputs (I-LSDB).

Uses xmatcher sync/mirror with archive politeness. Production later reads with
--no-sync from the pinned path.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

# Default v0.1 remote pins (aliases resolve via xmatcher.yaml).
V01_LSDB = [
    "gaia_lsdb_hats",
    "skymapper_dr4_hats",
    "twomass_hats",
]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--work-root",
        type=Path,
        default=Path("/arc/projects/k-pop/pensus"),
    )
    parser.add_argument("--alias", action="append", default=[])
    parser.add_argument("--print-commands", action="store_true")
    args = parser.parse_args(argv)
    aliases = args.alias or V01_LSDB
    dest = args.work_root / "inputs"
    commands = []
    for alias in aliases:
        out = dest / alias
        commands.append(
            [
                "pixi",
                "run",
                "xmatcher",
                "sync",
                alias,
                "--output",
                str(out),
            ]
        )
    report = {"aliases": aliases, "dest": str(dest), "n": len(aliases)}
    print(json.dumps(report, indent=2, sort_keys=True))
    if args.print_commands:
        for cmd in commands:
            print(" ".join(cmd))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
