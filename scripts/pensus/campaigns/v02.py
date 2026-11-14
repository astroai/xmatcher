#!/usr/bin/env python3
"""P-V02 and later: Legacy/UHS/VHS/AllWISE + P1/P2 + Euclid + replace drill."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-root", type=Path, default=Path("/arc/projects/k-pop/pensus"))
    parser.add_argument("--public-root", type=Path, default=Path("/arc/projects/hats/pensus"))
    args = parser.parse_args(argv)

    plan = {
        "v0.2": {
            "add": ["legacy_dr11", "uhs_dr3", "vhs_dr5", "allwise_w3w4"],
            "backbone": "Legacy Tractor replaces PS1/SkyMapper as L2 spine",
            "replace_drill": "optional DR10.1→DR11 non-bijective lineage (S-REPLACE)",
        },
        "later_public": {
            "surveys": [
                "sdss_u",
                "splus_dr4",
                "decaps",
                "nsc",
                "hsc",
                "kids",
                "atlas",
                "virac2",
                "jpas",
                "euclid_dr1_foundation",
            ],
            "euclid": "public footprint-limited P2 (~1900 deg2)",
        },
        "access_gated": ["lsst_dp2", "unions", "pristine_dr2.1_or_dr3"],
        "gaia_dr4": {
            "timing": "~Dec 2026",
            "protocol": "non-bijective replace after v0.1 on DR3; member-equivalence alone is insufficient",
            "drill": "P-DRILL / S-REPLACE",
        },
        "paths": {
            "draft": str(args.work_root / "drafts" / "views" / "v0.2"),
            "public": str(args.public_root / "views" / "v0.2"),
        },
    }
    print(json.dumps(plan, indent=2, sort_keys=True))
    docs = args.work_root / "docs"
    try:
        docs.mkdir(parents=True, exist_ok=True)
        (docs / "v02-plan.json").write_text(json.dumps(plan, indent=2, sort_keys=True) + "\n")
    except OSError:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
