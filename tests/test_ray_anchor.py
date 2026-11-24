from __future__ import annotations

import functools
import sys
import threading
from dataclasses import replace
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import polars as pl
import pytest

from xmatcher.crossmatch import CrossMatch
from xmatcher.exceptions import CrossMatchError
from xmatcher.mirror import _write_hats_native
from xmatcher.ray_anchor import build_anchor_plan, ray_anchor_match, run_anchor_partition
from xmatcher.sources import CatalogueSource


def _write_cat(path: Path, frame: pl.DataFrame) -> None:
    _write_hats_native(frame, path, ra_column="ra", dec_column="dec")


def _source(name: str, path: Path, id_column: str = "id", **kwargs) -> CatalogueSource:
    return CatalogueSource(
        name=name,
        is_local=True,
        path=path,
        access_method="hats",
        ra_column="ra",
        dec_column="dec",
        id_column=id_column,
        **kwargs,
    )


@pytest.fixture()
def survey_tree(tmp_path: Path):
    anchor = pl.DataFrame(
        {
            "id": [1, 2, 3, 4, 5, 6],
            "ra": [10.0, 10.001, 20.0, 30.0, 40.0, 50.0],
            "dec": [0.0] * 6,
            "pmra": [1000.0] * 6,
            "pmdec": [0.0] * 6,
            "parallax": [1.0] * 6,
            "gmag": [12.0, 13.0, 14.0, 15.0, 16.0, 17.0],
        }
    )
    survey_a = pl.DataFrame(
        {
            "sid": [101, 102, 103],
            "ra": [10.0, 10.0002, 10.0004],
            "dec": [0.0, 0.0, 0.0],
            "mag": [12.0, 13.0, 14.0],
        }
    )
    survey_b = pl.DataFrame({"sid": [201], "ra": [20.0], "dec": [0.0], "mag": [15.0]})
    for name, frame in (("anchor", anchor), ("a", survey_a), ("b", survey_b)):
        _write_cat(tmp_path / name, frame)
    return tmp_path


def test_anchor_keeps_every_anchor_row_and_ambiguity(survey_tree: Path) -> None:
    anchor = _source(
        "anchor",
        survey_tree / "anchor",
        epoch=2016.0,
        pm_ra_column="pmra",
        pm_dec_column="pmdec",
        parallax_column="parallax",
    )
    survey_a = _source("a", survey_tree / "a", id_column="sid", epoch=2000.0)
    survey_b = _source("b", survey_tree / "b", id_column="sid")
    plan = build_anchor_plan(
        anchor, [survey_a, survey_b], sep_arcsec=1.5, out_dir=str(survey_tree / "out")
    )
    # Rows at different longitudes land in different base cells.
    for index in range(len(plan.anchor.partitions)):
        run_anchor_partition(plan, index)
    result = pl.concat(
        [pl.read_parquet(p) for p in (survey_tree / "out" / "dataset").rglob("*.parquet")]
    ).sort("source_id")

    assert result.height == 6
    assert result["source_id"].to_list() == [1, 2, 3, 4, 5, 6]
    # Every anchor-only column survives.
    for column in ("ra", "dec", "pmra", "pmdec", "parallax", "gmag"):
        assert column in result.columns

    first = result.row(0, named=True)
    assert first["sid_a"] == 101
    assert first["a_n_within"] == 3
    assert first["a_crowded"] is True
    assert first["a_sep_arcsec"] == pytest.approx(0.0, abs=1e-6)
    assert first["a_sep_second"] == pytest.approx(0.72, abs=0.02)
    assert first["a_quality"] == "ambiguous+pm_warn"
    assert first["a_pm_warn"] is True

    second = result.row(1, named=True)
    assert second["sid_a"] is None
    assert second["a_sep_arcsec"] is None
    assert second["a_n_within"] == 0
    assert second["a_quality"] == "no_match"
    assert second["a_pm_warn"] is True

    third = result.row(2, named=True)
    assert third["sid_b"] == 201
    assert third["b_n_within"] == 1
    assert third["b_quality"] == "good"
    assert third["b_pm_warn"] is False


def test_anchor_id_name_override(survey_tree: Path) -> None:
    anchor = _source("anchor", survey_tree / "anchor")
    survey = _source("a", survey_tree / "a", id_column="sid")
    plan = build_anchor_plan(
        anchor,
        [survey],
        sep_arcsec=1.5,
        out_dir=str(survey_tree / "out"),
        anchor_id_name="gaia_source_id",
    )
    run_anchor_partition(plan, 0)
    result = pl.read_parquet(next((survey_tree / "out" / "dataset").rglob("*.parquet")))
    assert "gaia_source_id" in result.columns
    assert "source_id" not in result.columns


def test_ray_anchor_match_writes_metadata(survey_tree: Path, monkeypatch) -> None:
    # Force the serial path: the engine imports Ray lazily, so a None entry
    # makes ``import ray`` raise ImportError without starting a cluster.
    monkeypatch.setitem(sys.modules, "ray", None)
    anchor = _source("anchor", survey_tree / "anchor")
    survey = _source("a", survey_tree / "a", id_column="sid")
    out = survey_tree / "out"
    ray_anchor_match(anchor, [survey], sep_arcsec=1.5, output_file=str(out))
    assert (out / "properties").is_file()
    assert (out / "partition_info.csv").is_file()
    assert "hats_nrows=6" in (out / "properties").read_text()
    from hats.io.validation import is_valid_catalog

    assert is_valid_catalog(out, strict=True)


def test_hats_aliases_resolve_without_network() -> None:
    cm = CrossMatch()
    expected = {
        "gaia_hats": "gaia_dr3_hats",
        "gaia_lsdb": "gaia_lsdb_hats",
        "sdss_hats": "sdss_dr16_hats",
        "catwise_hats": "catwise2020_hats",
        "vhs_hats": "vhs_dr5_hats",
        "ps1_hats": "ps1_dr2_hats",
        "galex_hats": "galex_gr67_hats",
        "splus_hats": "splus_dr4_hats",
        "twomass_hats": "twomass_psc_hats",
        "wise_hats": "allwise_hats",
        "skymapper_hats": "skymapper_dr4_hats",
        "ls_dr11": "legacy_dr11",
    }
    for alias, target in expected.items():
        assert cm.resolve_name(alias) == target
    source = cm.resolve_source("gaia_hats", {})
    assert source.access_method == "hats"
    assert source.access_identifier.startswith("https://vizcat.cds.unistra.fr/hats/gaia_dr3")
    local = cm.resolve_source("gaia_lsdb", {})
    assert local.access_identifier == "https://data.lsdb.io/hats/gaia_dr3/gaia"
    assert local.id_column == "source_id"


def test_allwise_motion_fit_is_not_used_to_move_positions() -> None:
    # The AllWISE motion fit is not astrometry: propagating by it moved a
    # source by 1630" over 2010->2016 (Gaia-master pilot). It stays as data.
    source = CrossMatch().resolve_source("allwise_hats", {})
    assert source.pm_ra_column is None and source.pm_dec_column is None
    assert {"pmra", "pmdec"} <= set(source.default_columns or [])


def test_anchor_rejects_unsupported_matcher(survey_tree: Path) -> None:
    anchor = _source("anchor", survey_tree / "anchor")
    survey = _source("a", survey_tree / "a", id_column="sid")
    with pytest.raises(CrossMatchError, match="matcher='sky'"):
        build_anchor_plan(
            anchor,
            [survey],
            sep_arcsec=1.5,
            out_dir=str(survey_tree / "out"),
            matcher="skyerr",
        )


def test_anchor_cli_path_mirrors_remote_hats(
    survey_tree: Path, tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setitem(sys.modules, "ray", None)
    handler = functools.partial(SimpleHTTPRequestHandler, directory=str(survey_tree))
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        base = f"http://127.0.0.1:{server.server_port}"
        cm = CrossMatch()
        out = tmp_path / "remote_master"
        cm.anchor_match(
            [f"{base}/anchor", f"{base}/a"],
            output_file=str(out),
            engine="ray-anchor",
            radius_arcsec=1.5,
            cache_root=str(tmp_path / "cache"),
        )
        assert (out / "properties").is_file()
        result = pl.concat(
            [pl.read_parquet(p) for p in (out / "dataset").glob("Norder=*/*/*.parquet")]
        )
        assert result.height == 6
        assert "source_id" in result.columns
        assert "a_n_within" in result.columns
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_engine_routes_to_anchor(monkeypatch) -> None:
    cm = CrossMatch()
    seen: dict[str, object] = {}

    def spy(catalogues, output_file, progress_cb=None, **params):
        seen["catalogues"] = catalogues
        seen["output"] = output_file
        seen["engine"] = params.get("engine")

    monkeypatch.setattr(cm, "_ray_anchor_multi", spy)
    cm.crossmatch_multi(
        ["anchor", "survey"],
        output_file="out.hats",
        engine="ray-anchor",
    )
    assert seen["catalogues"] == ["anchor", "survey"]
    assert seen["output"] == "out.hats"
    assert seen["engine"] == "ray-anchor"


# --------------------------------------------------------------------------- #
# per-partition candidate cones (mixed-order inputs)
# --------------------------------------------------------------------------- #
def _ang_deg(ra1: float, dec1: float, ra2: float, dec2: float) -> float:
    import math

    r1, d1, r2, d2 = map(math.radians, (ra1, dec1, ra2, dec2))
    cos_d = math.sin(d1) * math.sin(d2) + math.cos(d1) * math.cos(d2) * math.cos(r1 - r2)
    return math.degrees(math.acos(max(-1.0, min(1.0, cos_d))))


@pytest.fixture()
def mixed_order_trees(tmp_path: Path):
    import numpy as np

    rng = np.random.default_rng(7)
    # A dense anchor patch (deep partitions) plus a sparse all-sky sprinkle.
    n_dense = 4000
    ra = np.concatenate([rng.uniform(40.0, 44.0, n_dense), rng.uniform(0, 360, 400)])
    dec = np.concatenate(
        [rng.uniform(-2.0, 2.0, n_dense), np.degrees(np.arcsin(rng.uniform(-1, 1, 400)))]
    )
    anchor = pl.DataFrame({"id": np.arange(ra.size), "ra": ra, "dec": dec})
    _write_hats_native(anchor, tmp_path / "anchor", ra_column="ra", dec_column="dec", threshold=200)
    # A sparse all-sky survey stays at order 0 (one huge cell per base pixel).
    s_ra = rng.uniform(0, 360, 300)
    s_dec = np.degrees(np.arcsin(rng.uniform(-1, 1, 300)))
    survey = pl.DataFrame({"sid": np.arange(300), "ra": s_ra, "dec": s_dec})
    _write_hats_native(survey, tmp_path / "s0", ra_column="ra", dec_column="dec", threshold=10_000)
    # A dense survey patch at deeper orders.
    d_ra = rng.uniform(40.0, 44.0, 3000)
    d_dec = rng.uniform(-2.0, 2.0, 3000)
    deep = pl.DataFrame({"sid": np.arange(3000), "ra": d_ra, "dec": d_dec})
    _write_hats_native(deep, tmp_path / "deep", ra_column="ra", dec_column="dec", threshold=150)
    return tmp_path


def test_anchor_cones_are_per_partition_and_select_by_overlap(mixed_order_trees: Path) -> None:
    from xmatcher.ray_anchor import _cell_radius_deg, anchor_cone_radius_deg
    from xmatcher.ray_union import _pixel_center_deg

    root = mixed_order_trees
    plan = build_anchor_plan(
        _source("anchor", root / "anchor"),
        [
            _source("s0", root / "s0", id_column="sid"),
            _source("deep", root / "deep", id_column="sid"),
        ],
        sep_arcsec=2.0,
        out_dir=str(root / "out"),
    )
    orders = {p.order for p in plan.anchor.partitions}
    assert len(orders) > 1  # mixed anchor orders
    assert {p.order for p in plan.surveys[0].partitions} == {0}
    assert max(p.order for p in plan.surveys[1].partitions) >= 2
    assert plan.delta_deg == 0.0  # no target epoch -> no motion halo

    deep_total = len(plan.surveys[1].partitions)
    for part, cand in zip(plan.anchor.partitions, plan.candidates, strict=True):
        cone = anchor_cone_radius_deg(part, plan.sep_arcsec, plan.delta_deg)
        a_ra, a_dec = _pixel_center_deg(part.order, part.pix)
        for survey, tag in zip(plan.surveys, plan.survey_tags, strict=True):
            for idx in cand[tag]:
                sp = survey.partitions[idx]
                s_ra, s_dec = _pixel_center_deg(sp.order, sp.pix)
                # every selected survey cell overlaps the anchor partition's cone
                # (cdshealpix's cone cover may add cells just beyond its edge)
                assert _ang_deg(a_ra, a_dec, s_ra, s_dec) <= cone + 2.0 * _cell_radius_deg(
                    sp.order, sp.pix
                )
        if part.order >= 3:
            # a deep anchor partition never reads the whole sky
            assert len(cand["s0"]) < len(plan.surveys[0].partitions)
            assert len(cand["deep"]) < deep_total


def test_anchor_cone_scales_with_the_anchor_partition() -> None:
    from xmatcher.ray_anchor import anchor_cone_radius_deg
    from xmatcher.ray_union import PartitionPlan

    def part(order: int) -> PartitionPlan:
        return PartitionPlan(order=order, pix=0, rel="x")

    radii = [anchor_cone_radius_deg(part(k), 1.0, 0.0) for k in range(0, 6)]
    assert all(a > b for a, b in zip(radii, radii[1:], strict=False))
    # order-2 cell radius is ~10 deg, far below the old 2 x (radius + max diagonal)
    assert 5.0 < radii[2] < 15.0
    halo = anchor_cone_radius_deg(part(4), 1.0, 600.0 / 3600.0) - radii[4]
    assert abs(halo - 600.0 / 3600.0) < 1e-12


def test_anchor_target_epoch_gets_a_motion_halo(survey_tree: Path) -> None:
    anchor = _source(
        "anchor", survey_tree / "anchor", epoch=2016.0, pm_ra_column="pmra", pm_dec_column="pmdec"
    )
    survey = _source("a", survey_tree / "a", id_column="sid", epoch=2016.0)
    plan = build_anchor_plan(
        anchor, [survey], sep_arcsec=1.5, out_dir=str(survey_tree / "out"), target_epoch=2016.0
    )
    assert plan.delta_deg == pytest.approx(600.0 / 3600.0)


# --------------------------------------------------------------------------- #
# invalid survey coordinates are dropped and counted; anchors stay strict
# --------------------------------------------------------------------------- #
def _corrupt_first_partition(tree: Path, rows: pl.DataFrame) -> None:
    part = next((tree / "dataset").rglob("Npix=*.parquet"))
    frame = pl.read_parquet(part)
    extra = rows.with_columns(
        [
            pl.lit(None, dtype=frame.schema[c]).alias(c)
            for c in frame.columns
            if c not in rows.columns
        ]
    ).select(frame.columns)
    pl.concat([frame, extra.cast(frame.schema)]).write_parquet(part)


def test_invalid_survey_rows_are_dropped_and_counted(survey_tree: Path, monkeypatch) -> None:
    monkeypatch.setitem(sys.modules, "ray", None)
    _corrupt_first_partition(
        survey_tree / "a",
        pl.DataFrame({"sid": [901, 902], "ra": [10.0, float("nan")], "dec": [95.0, 0.0]}),
    )
    anchor = _source("anchor", survey_tree / "anchor")
    survey = _source("a", survey_tree / "a", id_column="sid")
    out = survey_tree / "out"
    ray_anchor_match(anchor, [survey], sep_arcsec=1.5, output_file=str(out))
    props = (out / "properties").read_text()
    assert "xmatcher_invalid_survey_rows=a:2" in props
    state = (out / "resume.state").read_text()
    assert '"invalid_rows"' in state
    rows = pl.concat([pl.read_parquet(p) for p in (out / "dataset").rglob("Npix=*.parquet")])
    assert rows.height == 6  # every anchor row kept
    assert 901 not in rows["sid_a"].drop_nulls().to_list()
    # a resumed run (all partitions done) keeps the count
    ray_anchor_match(anchor, [survey], sep_arcsec=1.5, output_file=str(out))
    assert "xmatcher_invalid_survey_rows=a:2" in (out / "properties").read_text()


def test_invalid_anchor_rows_still_abort(survey_tree: Path) -> None:
    _corrupt_first_partition(
        survey_tree / "anchor",
        pl.DataFrame({"id": [99], "ra": [float("nan")], "dec": [0.0]}),
    )
    anchor = _source("anchor", survey_tree / "anchor")
    survey = _source("a", survey_tree / "a", id_column="sid")
    plan = build_anchor_plan(anchor, [survey], sep_arcsec=1.5, out_dir=str(survey_tree / "out"))
    with pytest.raises(CrossMatchError, match="non-finite or out-of-range"):
        for index in range(len(plan.anchor.partitions)):
            run_anchor_partition(plan, index)


# --------------------------------------------------------------------------- #
# anchor-partition subsets (pilots) and per-task stats
# --------------------------------------------------------------------------- #
def test_parse_anchor_partitions() -> None:
    from xmatcher.ray_anchor import parse_anchor_partitions

    assert parse_anchor_partitions(None) is None
    assert parse_anchor_partitions("") is None
    assert parse_anchor_partitions("2/69, 6/42837 2/69") == [(2, 69), (6, 42837)]
    assert parse_anchor_partitions("Norder=3/Npix=5") == [(3, 5)]
    assert parse_anchor_partitions([(0, 4)]) == [(0, 4)]
    with pytest.raises(CrossMatchError, match="ORDER/PIX"):
        parse_anchor_partitions("2:69")
    with pytest.raises(CrossMatchError, match="not a HEALPix cell"):
        parse_anchor_partitions("0/12")


def test_anchor_partition_subset_runs_only_those_partitions(survey_tree: Path, monkeypatch) -> None:
    monkeypatch.setitem(sys.modules, "ray", None)
    anchor = _source("anchor", survey_tree / "anchor")
    survey = _source("a", survey_tree / "a", id_column="sid")
    full = build_anchor_plan(anchor, [survey], sep_arcsec=1.5, out_dir=str(survey_tree / "x"))
    first = full.anchor.partitions[0]
    spec = f"{first.order}/{first.pix}"

    out = survey_tree / "pilot"
    ray_anchor_match(anchor, [survey], sep_arcsec=1.5, output_file=str(out), anchor_partitions=spec)
    files = list((out / "dataset").rglob("Npix=*.parquet"))
    assert [f.name for f in files] == [f"Npix={first.pix}.parquet"]
    rows = pl.read_parquet(files[0])
    assert rows.height == pl.read_parquet(survey_tree / "anchor" / first.rel).height
    props = (out / "properties").read_text()
    assert f"xmatcher_anchor_partitions={spec}" in props
    assert f"hats_nrows={rows.height}" in props

    import json

    state = json.loads((out / "resume.state").read_text())
    stats = state["task_stats"][f"Norder={first.order}/Dir=0/Npix={first.pix}.parquet"]
    assert stats["anchor_rows"] == rows.height
    assert stats["seconds"] >= 0.0 and stats["peak_rss_gib"] > 0.0
    survey_stats = stats["surveys"]["a"]
    assert {"partitions", "rows", "read_s", "match_s", "invalid", "matched"} <= set(survey_stats)
    assert survey_stats["matched"] == int((rows["a_n_within"] > 0).sum())

    # the full run into the same directory is a different request: it starts over
    ray_anchor_match(anchor, [survey], sep_arcsec=1.5, output_file=str(out))
    assert "xmatcher_anchor_partitions" not in (out / "properties").read_text()
    assert "hats_nrows=6" in (out / "properties").read_text()


def test_anchor_partition_subset_rejects_unknown_cells(survey_tree: Path) -> None:
    anchor = _source("anchor", survey_tree / "anchor")
    survey = _source("a", survey_tree / "a", id_column="sid")
    with pytest.raises(CrossMatchError, match="not in the anchor tree"):
        build_anchor_plan(
            anchor,
            [survey],
            sep_arcsec=1.5,
            out_dir=str(survey_tree / "out"),
            anchor_partitions="5/7",
        )


def test_anchor_partitions_need_the_anchor_engine() -> None:
    cm = CrossMatch()
    with pytest.raises(CrossMatchError, match="only to engine='ray-anchor'"):
        cm.crossmatch_multi(
            ["a.parquet", "b.parquet", "c.parquet"],
            output_file="out.hats",
            engine="ray-union",
            anchor_partitions="2/69",
        )


def test_cli_anchor_partitions_reach_the_engine(monkeypatch, tmp_path: Path) -> None:
    from xmatcher import cli

    seen: dict[str, object] = {}

    def spy(self, catalogues, output_file, progress_cb=None, **params):
        seen["anchor_partitions"] = params.get("anchor_partitions")
        seen["read_threads"] = params.get("read_threads")
        seen["task_memory_scale"] = params.get("task_memory_scale")

    monkeypatch.setattr(CrossMatch, "_ray_anchor_multi", spy)
    code = cli.main(
        [
            "match",
            "anchor",
            "survey",
            "--engine",
            "ray-anchor",
            "--anchor-partitions",
            "2/69,6/42837",
            "-o",
            str(tmp_path / "subset.hats"),
        ]
    )
    assert code == 0
    assert seen["anchor_partitions"] == "2/69,6/42837"
    assert (
        cli.main(
            [
                "match",
                "anchor",
                "survey",
                "--engine",
                "ray-anchor",
                "--read-threads",
                "4",
                "--task-memory-scale",
                "1.5",
                "-o",
                str(tmp_path / "subset2.hats"),
            ]
        )
        == 0
    )
    assert (seen["read_threads"], seen["task_memory_scale"]) == (4, 1.5)
    assert (
        cli.main(
            ["match", "a.parquet", "b.parquet", "--engine", "fast", "--anchor-partitions", "2/69"]
        )
        == 2
    )


def test_footer_row_count_sums_npix_directories(tmp_path: Path) -> None:
    from xmatcher.ray_union import _footer_row_count
    from xmatcher.storage import LocalStorage

    part = tmp_path / "dataset" / "Norder=3" / "Dir=0" / "Npix=158"
    part.mkdir(parents=True)
    pl.DataFrame({"a": [1, 2]}).write_parquet(part / "part0.parquet")
    pl.DataFrame({"a": [3, 4, 5]}).write_parquet(part / "part1.parquet")
    pl.DataFrame({"a": [9]}).write_parquet(part.parent / "Npix=159.parquet")
    storage = LocalStorage(str(tmp_path))
    assert _footer_row_count(storage, "dataset/Norder=3/Dir=0/Npix=158") == 5
    assert _footer_row_count(storage, "dataset/Norder=3/Dir=0/Npix=159.parquet") == 1
    assert _footer_row_count(storage, "dataset/Norder=3/Dir=0/missing.parquet") is None


# --------------------------------------------------------------------------- #
# task memory model, concurrent reads, no-op propagation
# --------------------------------------------------------------------------- #
def test_task_memory_estimate_tracks_the_largest_survey(mixed_order_trees: Path) -> None:
    from xmatcher.ray_anchor import _TASK_BASE_BYTES, _task_memory_bytes

    root = mixed_order_trees
    plan = build_anchor_plan(
        _source("anchor", root / "anchor"),
        [
            _source("s0", root / "s0", id_column="sid"),
            _source("deep", root / "deep", id_column="sid"),
        ],
        sep_arcsec=2.0,
        out_dir=str(root / "out"),
    )
    assert len(plan.task_memory) == len(plan.anchor.partitions)
    assert all(m > _TASK_BASE_BYTES for m in plan.task_memory)
    small = _task_memory_bytes(plan.anchor, 10, plan.surveys, [10, 10], None)
    big = _task_memory_bytes(plan.anchor, 10, plan.surveys, [10, 10_000_000], None)
    assert big - small > 10_000_000 * 24  # >= coordinates + id per survey row
    # a survey that moves pays for its propagation arrays
    moving = replace(plan.surveys[1], pm_ra_column="ra", pm_dec_column="dec", epoch=2000.0)
    moved = _task_memory_bytes(plan.anchor, 10, [plan.surveys[0], moving], [10, 10_000_000], 2016.0)
    assert moved > big


def test_task_reservation_caps_and_disables() -> None:
    from xmatcher.ray_anchor import AnchorPlan, _task_reservation

    plan = AnchorPlan.__new__(AnchorPlan)
    plan.task_memory = [10 * 1024**3, 200 * 1024**3]
    cap = 100 * 1024**3
    assert _task_reservation(plan, 0, 1.0, cap) == 10 * 1024**3
    assert _task_reservation(plan, 0, 1.5, cap) == 15 * 1024**3
    assert _task_reservation(plan, 1, 1.0, cap) == cap
    assert _task_reservation(plan, 0, 0.0, cap) == 0
    assert _task_reservation(plan, 0, 1.0, 0) == 0


def test_concurrent_reads_match_serial_reads(mixed_order_trees: Path) -> None:
    from xmatcher.ray_anchor import _read_frame

    root = mixed_order_trees
    plan = build_anchor_plan(
        _source("anchor", root / "anchor"),
        [_source("deep", root / "deep", id_column="sid")],
        sep_arcsec=2.0,
        out_dir=str(root / "out"),
    )
    survey = plan.surveys[0]
    idxs = list(range(len(survey.partitions)))
    assert len(idxs) > 2
    serial = _read_frame(survey, idxs, replace(plan, read_threads=1), invalid={})
    threaded = _read_frame(survey, idxs, replace(plan, read_threads=4), invalid={})
    assert serial.equals(threaded)


def test_surveys_without_motion_skip_propagation(survey_tree: Path, monkeypatch) -> None:
    from xmatcher import matchers
    from xmatcher.ray_anchor import _read_frame

    anchor = _source(
        "anchor", survey_tree / "anchor", epoch=2016.0, pm_ra_column="pmra", pm_dec_column="pmdec"
    )
    survey = _source("a", survey_tree / "a", id_column="sid", epoch=2000.0)
    plan = build_anchor_plan(
        anchor, [survey], sep_arcsec=1.5, out_dir=str(survey_tree / "out"), target_epoch=2016.0
    )
    calls: list[str] = []
    real = matchers._apply_proper_motion

    def spy(left, right, left_src, *args, **kwargs):
        calls.append(left_src.name)
        return real(left, right, left_src, *args, **kwargs)

    monkeypatch.setattr(matchers, "_apply_proper_motion", spy)
    cat = plan.surveys[0]
    _read_frame(cat, list(range(len(cat.partitions))), plan, invalid={})
    assert calls == []  # no motion columns: nothing to propagate
    _read_frame(plan.anchor, [0], plan)
    assert calls  # the anchor moves
