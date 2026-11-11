#!/usr/bin/env python3
"""Build Pristine DR2 HATS under k-pop (cheap smoke), then optional public promote.

Pristine enters pensus views via Gaia source_id ID-join only — this HATS tree
is a pipeline smoke test / sidecar source, not a positional match input.

Typical CANFAR flow (scratch ≤ 290 GiB):

  1. Download/stage FITS or parquet into /scratch/pensus/<run>/pristine
  2. Stream convert with torchfits if FITS; else polars/pyarrow
  3. hats-import into /arc/projects/k-pop/pensus/hats_build/pristine_dr2
  4. Optional promote to /arc/projects/hats/pristine_dr2 after checksum
  5. Wipe scratch
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve().parents[1]
if str(_SCRIPT_DIR.parent) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_DIR.parent))

from pensus.paths import SCRATCH_HEADROOM_GIB, default_paths  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", default="pristine-dr2")
    parser.add_argument("--source", type=Path, help="Input FITS/parquet/dir")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--promote-public", action="store_true")
    args = parser.parse_args(argv)
    paths = default_paths(args.run_id)
    build = paths.work_root / "hats_build" / "pristine_dr2"
    scratch = paths.scratch_root / "pristine"
    plan = {
        "source": str(args.source) if args.source else None,
        "scratch": str(scratch),
        "scratch_headroom_gib": SCRATCH_HEADROOM_GIB,
        "hats_build": str(build),
        "public_promote": str(paths.hats_survey_root / "pristine_dr2"),
        "id_join_only": True,
        "steps": [
            "stage source → scratch",
            "torchfits stream or polars → parquet partitions",
            "hats-import → k-pop hats_build/pristine_dr2",
            "set XMATCHER_PRISTINE_DR2_HATS to hats_build path",
            "optional checksum promote → /arc/projects/hats/pristine_dr2",
            "wipe scratch",
        ],
    }
    print(json.dumps(plan, indent=2, sort_keys=True))
    if args.dry_run:
        return 0
    if args.source is None:
        print("provide --source or --dry-run", file=sys.stderr)
        return 2
    if not Path("/scratch").exists() and not paths.work_root.exists():
        print(
            "CANFAR mounts absent; recipe printed only. Re-run on Skaha with --source.",
            file=sys.stderr,
        )
        return 0
    scratch.mkdir(parents=True, exist_ok=True)
    build.mkdir(parents=True, exist_ok=True)
    # Copy/stage only; hats-import is invoked by the operator with current hats-import CLI.
    staged = scratch / "input"
    if args.source.is_dir():
        if staged.exists():
            shutil.rmtree(staged)
        shutil.copytree(args.source, staged)
    else:
        staged.mkdir(parents=True, exist_ok=True)
        shutil.copy2(args.source, staged / args.source.name)
    meta = build / "PENSUS_BUILD.json"
    meta.write_text(
        json.dumps(
            {
                "survey": "pristine_dr2",
                "staged_from": str(args.source),
                "scratch": str(scratch),
                "status": "staged_awaiting_hats_import",
                "positional_match": False,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n"
    )
    print(f"staged → {scratch}; build marker → {meta}")
    if args.promote_public:
        print(
            "promote-public deferred until hats-import checksum validation completes",
            file=sys.stderr,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
