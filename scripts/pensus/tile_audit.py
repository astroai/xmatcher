#!/usr/bin/env python3
"""Audit tile markers vs outputs (rows/bytes/sha256)."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve().parent
_SCRIPTS_ROOT = str(_SCRIPT_DIR.parent)
if _SCRIPTS_ROOT not in sys.path:
    sys.path.insert(0, _SCRIPTS_ROOT)

from pensus.tile_markers import file_sha256, read_marker  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--campaign", required=True)
    parser.add_argument("--work-root", type=Path, default=Path("/arc/projects/k-pop/pensus"))
    args = parser.parse_args(argv)

    markers_dir = args.work_root / "tiles" / args.campaign / "markers"
    if not markers_dir.is_dir():
        print(json.dumps({"ok": False, "error": f"no markers dir {markers_dir}"}))
        return 2

    failures: list[dict] = []
    checked = 0
    for path in sorted(markers_dir.glob("o*_p*.json")):
        checked += 1
        marker = read_marker(path)
        out = (
            args.work_root
            / "tiles"
            / args.campaign
            / f"o{marker.order}"
            / f"p{marker.pixel}"
            / "edges_mutual_best.parquet"
        )
        if not out.is_file():
            failures.append({"marker": path.name, "error": "missing output"})
            continue
        size = out.stat().st_size
        digest = file_sha256(out)
        if size != marker.bytes or digest != marker.sha256:
            failures.append(
                {
                    "marker": path.name,
                    "error": "sha/bytes mismatch",
                    "expected_bytes": marker.bytes,
                    "actual_bytes": size,
                    "expected_sha256": marker.sha256,
                    "actual_sha256": digest,
                }
            )
    report = {"ok": not failures, "checked": checked, "failures": failures}
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
