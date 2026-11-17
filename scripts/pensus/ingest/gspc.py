#!/usr/bin/env python3
"""I-GSPC: Gaia GSPC/XP continuous spectra — ECSV → parquet → HATS (not torchfits)."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-root", type=Path, default=Path("/arc/projects/k-pop/pensus"))
    parser.add_argument("--ecsv", type=Path, help="Input ECSV (or directory of shards)")
    parser.add_argument("--dry-run", action="store_true", default=True)
    args = parser.parse_args(argv)
    build = args.work_root / "hats_build" / "gaia_gspc"
    plan = {
        "input": str(args.ecsv) if args.ecsv else None,
        "convert": "polars/pyarrow ECSV/CSV stream (NOT torchfits)",
        "hats_build": str(build),
        "env": "XMATCHER_GAIA_GSPC_HATS",
        "sidecar_public": "/arc/projects/hats/pensus/sidecars/gaia_gspc",
        "steps": [
            "stream ECSV → parquet partitions under /scratch then k-pop staging if needed",
            "hats-import → hats_build/gaia_gspc",
            "promote sidecar under hats/pensus/sidecars after view release",
        ],
    }
    print(json.dumps(plan, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
