#!/usr/bin/env python3
"""S-REPLACE / P-DRILL: non-bijective lineage replace (Legacy DR10.1→11 or Gaia DR4 prep).

member-equivalence is bijective-only; splits/merges must use an explicit replace
protocol that leaves prior view `vN` untouched.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from-view", required=True)
    parser.add_argument("--to-view", required=True)
    parser.add_argument("--survey", required=True)
    parser.add_argument("--work-root", type=Path, default=Path("/arc/projects/k-pop/pensus"))
    args = parser.parse_args(argv)

    plan = {
        "from_view": args.from_view,
        "to_view": args.to_view,
        "survey": args.survey,
        "invariants": [
            f"views/{args.from_view} remains immutable and readable",
            "new namespace for replacement survey release",
            "splits/merges recorded explicitly (not bijective member-equivalence alone)",
            "tile campaign re-run only for affected cascade levels",
        ],
        "artifacts": {
            "lineage": str(args.work_root / "drafts" / "views" / args.to_view / "lineage.json"),
            "equivalence_partial": str(
                args.work_root / "drafts" / "views" / args.to_view / "member_links.parquet"
            ),
        },
    }
    print(json.dumps(plan, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
