"""Round-2 regressions for the distributed and HATS engines."""

from __future__ import annotations

import polars as pl
import pytest

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


def test_anchor_counts_agree_with_separations_at_the_radius() -> None:
    """``n_within`` and ``sep_arcsec``/``sep_second`` must use one radius rule.

    Two candidates sit exactly on the radius (the radius is their computed
    separation): both must be counted and the second one reported, as in
    torchsky's inclusive ``sep <= radius`` rule for counts and neighbours.
    """
    import torch
    from torchsky.catalogs import match_within

    ra_s = [10.0001, 10.0, 10.0]
    dec_s = [5.0, 5.0004, 4.9996]  # the last two are symmetric about the anchor
    anchor = pl.DataFrame({"ra": [10.0], "dec": [5.0]})
    survey = pl.DataFrame({"ra_s": ra_s, "dec_s": dec_s})
    seps = match_within(
        torch.tensor([10.0], dtype=torch.float64),
        torch.tensor([5.0], dtype=torch.float64),
        torch.tensor(ra_s, dtype=torch.float64),
        torch.tensor(dec_s, dtype=torch.float64),
        10.0,
    ).separation_arcsec
    radius = float(seps.max())
    plan, cat = _plan(radius)
    out = ray_anchor._match_survey(anchor, survey, plan, cat, "s")
    assert out["s_n_within"][0] == 3
    assert out["s_crowded"][0]
    assert out["s_sep_arcsec"][0] == pytest.approx(float(seps.min()), abs=1e-9)
    assert out["s_sep_second"][0] == pytest.approx(radius, abs=1e-9)


def test_anchor_refuses_motion_beyond_the_candidate_halo(tmp_path) -> None:
    """--target-epoch moved rows further than the survey candidate cone reaches.

    Survey partitions are chosen from catalogue-epoch positions; a row moved
    beyond the cone's margin could pair with sources no task read.
    """
    from xmatcher.exceptions import CrossMatchError
    from xmatcher.ray_union import PartitionPlan

    pl.DataFrame(
        {"ra": [10.0, 10.0], "dec": [5.0, 5.0], "pmra": [0.0, 50_000.0], "pmdec": [0.0, 0.0]}
    ).write_parquet(tmp_path / "p.parquet")
    cols = ["ra", "dec", "pmra", "pmdec"]
    cat = CataloguePlan(
        "fast",
        str(tmp_path),
        "",
        "ra",
        "dec",
        partitions=[PartitionPlan(0, 0, "p.parquet")],
        cols=cols,
        final_cols=cols,
        dtypes=dict.fromkeys(cols, pl.Float64),
        epoch=2016.0,
        pm_ra_column="pmra",
        pm_dec_column="pmdec",
    )
    plan, _ = _plan(1.0)
    plan.delta_deg = 1.0 / 60.0  # halo = (1" + 60") / 2
    plan.target_epoch = 2017.0
    with pytest.raises(CrossMatchError, match="candidate halo"):
        ray_anchor._read_frame(cat, [0], plan)
    plan.target_epoch = 2016.5  # 25" < 30.5"
    assert ray_anchor._read_frame(cat, [0], plan).height == 2


def _fake_ray(monkeypatch, *, initialized: bool) -> list[str]:
    ray = pytest.importorskip("ray")
    calls: list[str] = []
    monkeypatch.delenv("RAY_ADDRESS", raising=False)
    monkeypatch.setattr(ray, "is_initialized", lambda: initialized)
    monkeypatch.setattr(ray, "init", lambda **kw: calls.append("init"))
    monkeypatch.setattr(ray, "shutdown", lambda *a, **kw: calls.append("shutdown"))
    return calls


@pytest.mark.parametrize("initialized", [False, True])
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


@pytest.mark.parametrize("initialized", [False, True])
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
