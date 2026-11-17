#!/usr/bin/env python3
"""Run one pensus HEALPix tile: spine×survey matches → mutual-best → marker.

Production path: engine=fast, fallback_policy=error, --no-sync pinned trees.
This driver is intentionally thin; heavy I/O uses xmatcher CrossMatch / HATS APIs.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import polars as pl

_SCRIPT_DIR = Path(__file__).resolve().parent
_SCRIPTS_ROOT = str(_SCRIPT_DIR.parent)
if _SCRIPTS_ROOT not in sys.path:
    sys.path.insert(0, _SCRIPTS_ROOT)

from pensus.mutual_best import mutual_best_edges  # noqa: E402
from pensus.route_guard import guard_match_route  # noqa: E402
from pensus.tile_geometry import TileSpec, margin_arcsec  # noqa: E402
from pensus.tile_markers import TileMarker, file_sha256, write_marker  # noqa: E402


def _synthetic_demo(order: int, pixel: int, out_parquet: Path) -> pl.DataFrame:
    """Deterministic synthetic edges for offline CI / dry runs without HATS mounts."""
    n = 32
    spine_id = [f"s{pixel}_{i}" for i in range(n)]
    survey_id = [f"t{pixel}_{i}" for i in range(n)]
    # Slight asymmetry so mutual-best drops a few non-reciprocal pairs.
    sep = [0.1 + (i % 7) * 0.01 + (0.05 if i % 11 == 0 else 0.0) for i in range(n)]
    raw = pl.DataFrame({"spine_id": spine_id, "survey_id": survey_id, "sep_arcsec": sep})
    # Add competitors that lose mutual-best.
    extras = pl.DataFrame(
        {
            "spine_id": [f"s{pixel}_0", f"s{pixel}_1"],
            "survey_id": [f"t{pixel}_99", f"t{pixel}_98"],
            "sep_arcsec": [0.5, 0.6],
        }
    )
    edges = mutual_best_edges(pl.concat([raw, extras]))
    out_parquet.parent.mkdir(parents=True, exist_ok=True)
    edges.write_parquet(out_parquet)
    return edges


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--campaign", required=True)
    parser.add_argument("--order", type=int, required=True)
    parser.add_argument("--pixel", type=int, required=True)
    parser.add_argument("--work-root", type=Path, default=Path("/arc/projects/k-pop/pensus"))
    parser.add_argument("--engine", default="fast")
    parser.add_argument("--fallback-policy", default="error")
    parser.add_argument("--r-max-arcsec", type=float, default=3.0)
    parser.add_argument("--cascade-levels", type=int, default=3)
    parser.add_argument("--pm-halo-arcsec", type=float, default=17.0)
    parser.add_argument(
        "--demo",
        action="store_true",
        help="Synthetic tile path (no HATS). Used for CI and spike harness offline.",
    )
    args = parser.parse_args(argv)

    guard_match_route(engine=args.engine, fallback_policy=args.fallback_policy)
    margin = margin_arcsec(
        r_max_arcsec=args.r_max_arcsec,
        cascade_levels=args.cascade_levels,
        pm_halo_arcsec=args.pm_halo_arcsec,
    )
    spec = TileSpec(order=args.order, pixel=args.pixel, margin_arcsec=margin)
    tile_dir = args.work_root / "tiles" / args.campaign / f"o{spec.order}" / f"p{spec.pixel}"
    out_parquet = tile_dir / "edges_mutual_best.parquet"
    markers_dir = args.work_root / "tiles" / args.campaign / "markers"

    if not args.demo:
        print(
            json.dumps(
                {
                    "error": "full HATS tile path requires CANFAR mounts; pass --demo offline",
                    "tile": {"order": spec.order, "pixel": spec.pixel, "margin_arcsec": margin},
                }
            ),
            file=sys.stderr,
        )
        return 3

    edges = _synthetic_demo(spec.order, spec.pixel, out_parquet)
    digest = file_sha256(out_parquet)
    marker = TileMarker(
        campaign=args.campaign,
        order=spec.order,
        pixel=spec.pixel,
        rows=edges.height,
        bytes=out_parquet.stat().st_size,
        sha256=digest,
    )
    write_marker(markers_dir, marker)
    print(
        json.dumps(
            {
                "status": "done",
                "marker": marker.name,
                "rows": marker.rows,
                "sha256": marker.sha256,
                "margin_arcsec": margin,
                "output": str(out_parquet),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
