#!/usr/bin/env python3
"""P-V01 campaign skeleton: full-sky tile submit + sidecar promote checklist."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

SURVEYS = [
    "gaia_dr3",
    "catwise2020",
    "twomass",
    "galex_guvcat",
    "ps1",
    "skymapper_dr4",
    "pristine_dr2_idjoin",
]
SIDECARS = ["gaia_gspc", "gaia_xp", "pristine"]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-root", type=Path, default=Path("/arc/projects/k-pop/pensus"))
    parser.add_argument("--public-root", type=Path, default=Path("/arc/projects/hats/pensus"))
    parser.add_argument("--order", type=int, default=5)
    parser.add_argument("--print-commands", action="store_true")
    args = parser.parse_args(argv)

    campaign = "v0.1"
    plan = {
        "campaign": campaign,
        "surveys": SURVEYS,
        "sidecars": SIDECARS,
        "order": args.order,
        "work_tiles": str(args.work_root / "tiles" / campaign),
        "draft_view": str(args.work_root / "drafts" / "views" / "v0.1"),
        "public_view": str(args.public_root / "views" / "v0.1"),
        "steps": [
            "preflight dual budgets (k-pop + hats)",
            "confirm pinned inputs under k-pop/inputs (--no-sync)",
            "tile_submit missing markers → CANFAR headless tile_run",
            "tile_audit all markers",
            "assemble draft view + science validation gates",
            "checksum promote draft → hats/pensus/views/v0.1 + sidecars",
        ],
        "commands": [
            f"python scripts/pensus/tile_submit.py --campaign {campaign} --order {args.order} "
            f"--work-root {args.work_root} --print-commands",
            f"python scripts/pensus/tile_audit.py --campaign {campaign} --work-root {args.work_root}",
            f"python scripts/pensus/campaigns/scival.py --view v0.1 --draft "
            f"{args.work_root / 'drafts' / 'views' / 'v0.1'}",
        ],
    }
    print(json.dumps(plan, indent=2, sort_keys=True))
    if args.print_commands:
        for cmd in plan["commands"]:
            print(cmd)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
