#!/usr/bin/env python3
"""T-VIEW: assemble draft pensus view metadata from finished tile markers."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve().parent
_SCRIPTS_ROOT = str(_SCRIPT_DIR.parent)
if _SCRIPTS_ROOT not in sys.path:
    sys.path.insert(0, _SCRIPTS_ROOT)

from pensus.tile_markers import read_marker  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--campaign", required=True)
    parser.add_argument("--view", required=True)
    parser.add_argument("--work-root", type=Path, required=True)
    args = parser.parse_args(argv)

    markers_dir = args.work_root / "tiles" / args.campaign / "markers"
    markers = sorted(markers_dir.glob("o*_p*.json")) if markers_dir.is_dir() else []
    rows = 0
    bytes_total = 0
    for path in markers:
        m = read_marker(path)
        rows += m.rows
        bytes_total += m.bytes
    draft = args.work_root / "drafts" / "views" / args.view
    draft.mkdir(parents=True, exist_ok=True)
    manifest = {
        "view": args.view,
        "campaign": args.campaign,
        "tiles": len(markers),
        "edge_rows": rows,
        "edge_bytes": bytes_total,
        "status": "draft",
        "promote_to": f"/arc/projects/hats/pensus/views/{args.view}",
        "note": "Full columnar view materialization runs on CANFAR after tile campaign completes.",
    }
    (draft / "VIEW_MANIFEST.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    print(json.dumps(manifest, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
