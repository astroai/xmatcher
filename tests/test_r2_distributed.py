"""Round-2 regressions for the distributed and HATS engines."""

from __future__ import annotations

import numpy as np
import polars as pl
import pytest
from scipy.spatial import cKDTree

from xmatcher import matchers, ray_anchor
from xmatcher.ray_union import CataloguePlan


def _plan(sep_arcsec: float) -> tuple[ray_anchor.AnchorPlan, CataloguePlan]:
    anchor = CataloguePlan("anchor", "", "", "ra", "dec", dtypes={"ra": pl.Float64})
    survey = CataloguePlan(
        "s", "", "", "ra_s", "dec_s", dtypes={"ra_s": pl.Float64, "dec_s": pl.Float64}
    )
    plan = ray_anchor.AnchorPlan(
        anchor=anchor,
        surveys=[survey],
        survey_tags=["s"],
        sep_arcsec=sep_arcsec,
        delta_deg=0.0,
        out_dir="",
        anchor_id="source_id",
        out_columns=[],
    )
    return plan, survey


def test_anchor_counts_agree_with_separations_at_the_radius(monkeypatch) -> None:
    """``n_within`` and ``sep_arcsec``/``sep_second`` must use one radius rule.

    cKDTree's two-neighbour query excludes a candidate exactly on
    ``distance_upper_bound`` while ``query_ball_point`` counts it, so a row
    reported ``n_within=3`` with a null ``sep_second``.  Exact binary
    distances put two candidates exactly on the radius.
    """
    xyz = {
        "anchor": np.array([[1.0, 0.0, 0.0]]),
        "survey": np.array([[1.0, 0.125, 0.0], [1.0, 0.5, 0.0], [1.0, -0.5, 0.0]]),
    }
    monkeypatch.setattr(
        matchers, "_radec_to_xyz", lambda ra, dec: xyz["anchor" if len(ra) == 1 else "survey"]
    )
    monkeypatch.setattr(matchers, "_arcsec_to_chord", lambda arcsec: 0.5)
    tree = cKDTree(xyz["survey"])
    assert np.isfinite(tree.query(xyz["anchor"], k=3, distance_upper_bound=0.5)[0]).sum() == 1
    assert tree.query_ball_point(xyz["anchor"], 0.5, return_length=True)[0] == 3

    anchor = pl.DataFrame({"ra": [0.0], "dec": [0.0]})
    survey = pl.DataFrame({"ra_s": [0.0, 0.0, 0.0], "dec_s": [0.0, 0.0, 0.0]})
    plan, cat = _plan(1.0)
    out = ray_anchor._match_survey(anchor, survey, plan, cat, "s")
    assert out["s_n_within"][0] == 3
    assert out["s_crowded"][0]
    assert out["s_sep_arcsec"][0] == matchers._chord_to_arcsec(0.125)
    assert out["s_sep_second"][0] == matchers._chord_to_arcsec(0.5)


def _fake_ray(monkeypatch, *, initialized: bool) -> list[str]:
    ray = pytest.importorskip("ray")
    calls: list[str] = []
    monkeypatch.delenv("RAY_ADDRESS", raising=False)
    monkeypatch.setattr(ray, "is_initialized", lambda: initialized)
    monkeypatch.setattr(ray, "init", lambda **kw: calls.append("init"))
    monkeypatch.setattr(ray, "shutdown", lambda *a, **kw: calls.append("shutdown"))
    return calls


def test_ray_zone_match_stops_only_the_cluster_it_started(monkeypatch, initialized) -> None:
    """engine='ray' started a private cluster per call and never stopped it."""
    from xmatcher import ray_engine
    from xmatcher.sources import CatalogueSource

    calls = _fake_ray(monkeypatch, initialized=initialized)
    source = CatalogueSource(name="t", is_local=True, ra_column="ra", dec_column="dec")
    empty = pl.DataFrame(
        {"ra": pl.Series([], dtype=pl.Float64), "dec": pl.Series([], dtype=pl.Float64)}
    )
    ray_engine.ray_zone_match(empty, empty, source, source, matchers.MatchSpec())
    assert calls == ([] if initialized else ["init", "shutdown"])


def test_hats_native_ray_stops_only_the_cluster_it_started(
    monkeypatch, tmp_path, initialized
) -> None:
    ray = pytest.importorskip("ray")

    from xmatcher import MatchSpec, hats_native
    from xmatcher.sources import CatalogueSource

    calls = _fake_ray(monkeypatch, initialized=initialized)

    def no_remote(fn):
        raise RuntimeError("no workers in this test")

    monkeypatch.setattr(ray, "remote", no_remote)
    sources = []
    for name in ("L", "R"):
        root = tmp_path / name
        part = root / "dataset" / "Norder=0" / "Dir=0" / "Npix=0"
        part.mkdir(parents=True)
        (root / "properties").write_text("hats_col_ra=ra\nhats_col_dec=dec\n")
        pl.DataFrame({"ra": [10.0], "dec": [5.0]}).write_parquet(part / "catalog.parquet")
        sources.append(
            CatalogueSource(
                name=name,
                is_local=False,
                access_method="hats",
                access_identifier=str(root),
                ra_column="ra",
                dec_column="dec",
            )
        )
    out = hats_native.hats_native_crossmatch(
        *sources, MatchSpec(radius_arcsec=1.0, join_type="1and2"), engine="ray"
    )
    assert out.height == 1
    assert calls == ([] if initialized else ["init", "shutdown"])
