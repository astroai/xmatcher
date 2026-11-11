"""Offline pensus tile / mutual-best / route-guard / margin tests."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import polars as pl
import pytest

ROOT = Path(__file__).resolve().parents[1]
PENSUS = ROOT / "scripts" / "pensus"
sys.path.insert(0, str(PENSUS.parent))

from pensus.mutual_best import mutual_best_edges  # noqa: E402
from pensus.route_guard import PensusRouteError, guard_match_route  # noqa: E402
from pensus.tile_geometry import assert_margin_covers_cascade, margin_arcsec  # noqa: E402


def test_margin_rule():
    m = margin_arcsec(r_max_arcsec=3.0, cascade_levels=3, pm_halo_arcsec=17.0)
    assert m == pytest.approx(2 * 3.0 * 3 + 17.0)
    assert_margin_covers_cascade(margin=m, r_max_arcsec=3.0, cascade_levels=3, pm_halo_arcsec=17.0)


def test_route_guard_rejects_auto_and_ray_union():
    with pytest.raises(PensusRouteError):
        guard_match_route(engine="auto", fallback_policy="error")
    with pytest.raises(PensusRouteError):
        guard_match_route(engine="ray-union", fallback_policy="error")
    with pytest.raises(PensusRouteError):
        guard_match_route(engine="fast", fallback_policy="warn")
    guard_match_route(engine="fast", fallback_policy="error")


def test_mutual_best_reciprocal_only():
    edges = pl.DataFrame(
        {
            "spine_id": ["a", "a", "b"],
            "survey_id": ["1", "2", "1"],
            "sep_arcsec": [0.1, 0.2, 0.15],
        }
    )
    # a→1 is best for a; but 1's best is b (0.15 < 0.1? no 0.15>0.1 so 1→a).
    # Actually sep(a,1)=0.1 is best for both a and 1; b→1 loses on right side.
    out = mutual_best_edges(edges)
    assert out.select(["spine_id", "survey_id"]).to_dicts() == [{"spine_id": "a", "survey_id": "1"}]


def test_tile_demo_spike_and_audit(tmp_path: Path):
    spike = subprocess.run(
        [
            sys.executable,
            str(PENSUS / "spike.py"),
            "--work-root",
            str(tmp_path),
            "--campaign",
            "ci-spike",
            "--order",
            "2",
            "--pixel",
            "3",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert spike.returncode == 0, spike.stderr + spike.stdout
    audit = subprocess.run(
        [
            sys.executable,
            str(PENSUS / "tile_audit.py"),
            "--campaign",
            "ci-spike",
            "--work-root",
            str(tmp_path),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert audit.returncode == 0, audit.stderr + audit.stdout
    payload = json.loads(audit.stdout)
    assert payload["ok"] is True
    assert payload["checked"] == 1
