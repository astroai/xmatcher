#!/usr/bin/env python3
"""Create the k-pop pensus work tree (never writes under hats/pensus)."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve().parent
_SCRIPTS_ROOT = str(_SCRIPT_DIR.parent)
if _SCRIPTS_ROOT not in sys.path:
    sys.path.insert(0, _SCRIPTS_ROOT)

from pensus.paths import default_paths, ensure_work_layout, public_layout, work_layout  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", default=None)
    parser.add_argument(
        "--also-public-dirs",
        action="store_true",
        help="Create empty public views/sidecars/docs dirs (no content).",
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    paths = default_paths(args.run_id)
    if args.dry_run:
        layout = work_layout(paths.work_root)
    else:
        layout = ensure_work_layout(paths.work_root)
        if args.also_public_dirs:
            for path in public_layout(paths.public_root).values():
                path.mkdir(parents=True, exist_ok=True)
    report = {
        "public_root": str(paths.public_root),
        "work_root": str(paths.work_root),
        "scratch_root": str(paths.scratch_root),
        "work_layout": {k: str(v) for k, v in layout.items()},
        "dry_run": bool(args.dry_run),
    }
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
