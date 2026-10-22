from __future__ import annotations

import functools
import sys
import threading
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
    run_anchor_partition(plan, 0)
    output = next((survey_tree / "out" / "dataset").rglob("*.parquet"))
    result = pl.read_parquet(output).sort("source_id")

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
    assert (out / "dataset" / "partition_info.parquet").is_file()
    assert (out / "partition_info.csv").is_file()
    assert "hats_nrows=6" in (out / "properties").read_text()


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
        result = pl.read_parquet(next((out / "dataset").glob("Norder=*/*/*.parquet")))
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
