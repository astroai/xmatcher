"""Round-2 audit regressions: public API, CLI and local I/O."""

from __future__ import annotations

import sys
import types

import polars as pl
import pytest

from xmatcher.matchers import MatchSpec
from xmatcher.sources import CatalogueSource


def _fake_xmatch(monkeypatch, table_factory):
    import xmatcher.remote_cds as rc

    class FakeXMatch:
        def query(self, cat1, cat2, max_distance, colRA1, colDec1):
            return table_factory(rc._XMATCH_KEY, colRA1, colDec1)

    module = types.ModuleType("astroquery.xmatch")
    module.XMatch = FakeXMatch
    monkeypatch.setitem(sys.modules, "astroquery.xmatch", module)
    return rc


_LOCAL = CatalogueSource(name="loc", is_local=True, ra_column="ra", dec_column="dec")
_REMOTE = CatalogueSource(
    name="viz", is_local=False, access_method="cds_xmatch", access_identifier="I/355"
)


def _one_match(key, ra, dec):
    from astropy.table import Table

    return Table({"angDist": [0.3], key: [0], ra: [10.0], dec: [5.0], "mag": [21.0]})


def test_cds_xmatch_remote_first_puts_remote_columns_first(monkeypatch):
    rc = _fake_xmatch(monkeypatch, _one_match)
    local = pl.DataFrame({"ra": [10.0, 80.0], "dec": [5.0, 5.0], "mag": [18.0, 19.0]})
    out = rc.cds_xmatch_local_remote(
        _LOCAL, _REMOTE, local.lazy(), MatchSpec(radius_arcsec=2.0, find="all"), local_first=False
    )
    # remote operand first: its columns are unsuffixed, the local ones get _2
    assert out.columns == ["mag", "ra", "dec", "mag_2", "sep_arcsec"]
    assert out.row(0) == (21.0, 10.0, 5.0, 18.0, 0.3)


def test_cds_xmatch_empty_result_keeps_match_schema(monkeypatch):
    from astropy.table import Table

    def empty(key, ra, dec):
        return Table(
            {"angDist": [], key: [], ra: [], dec: [], "mag": []},
            dtype=[float, int, float, float, float],
        )

    rc = _fake_xmatch(monkeypatch, empty)
    local = pl.DataFrame({"ra": [10.0], "dec": [5.0], "mag": [18.0]})
    out = rc.cds_xmatch_local_remote(
        _LOCAL, _REMOTE, local.lazy(), MatchSpec(radius_arcsec=2.0, find="all")
    )
    assert out.is_empty()
    assert out.columns == ["ra", "dec", "mag", "mag_2", "sep_arcsec"]


def test_cds_xmatch_no_table_still_has_sep_column(monkeypatch):
    rc = _fake_xmatch(monkeypatch, lambda *a: None)
    local = pl.DataFrame({"ra": [10.0], "dec": [5.0]})
    out = rc.cds_xmatch_local_remote(
        _LOCAL, _REMOTE, local.lazy(), MatchSpec(radius_arcsec=2.0, find="all")
    )
    assert out.is_empty()
    assert out.columns == ["ra", "dec", "sep_arcsec"]


def test_local_vs_remote_forwards_operand_order_to_cds(monkeypatch):
    from xmatcher import CrossMatch, MatchRequest, remote_cds

    seen = {}

    def fake(local, remote, local_lf, spec, *, local_first):
        seen["local_first"] = local_first
        return pl.DataFrame()

    monkeypatch.setattr(remote_cds, "cds_xmatch_local_remote", fake)
    cm = CrossMatch()
    local = _LOCAL.with_frame(pl.DataFrame({"ra": [1.0], "dec": [2.0]}).lazy())
    req = MatchRequest.from_params("viz", "loc", find="all")
    cm._local_vs_remote(_REMOTE, local, req)
    assert seen == {"local_first": False}


@pytest.mark.parametrize(
    "method", ["crossmatch", "crossmatch_multi", "union_match", "fof_match", "nway_match"]
)
def test_unknown_match_parameter_is_rejected(method):
    from xmatcher import CrossMatch

    frame = pl.DataFrame({"ra": [1.0], "dec": [2.0]})
    cm = CrossMatch()
    call = getattr(cm, method)
    args = (frame, frame) if method == "crossmatch" else ([frame, frame],)
    # `radius` used to be dropped silently, matching at the 1" default
    with pytest.raises(TypeError, match="radius_arcsec"):
        call(*args, radius=5.0)


def test_unknown_crossmatch_constructor_option_is_rejected():
    from xmatcher import CrossMatch

    with pytest.raises(TypeError):
        CrossMatch(n_workers=4)


def test_ray_union_python_api_uses_the_shared_max_error_default(tmp_path, monkeypatch):
    """ray-union used 1 sigma when max_error was omitted; every other engine uses 3."""
    from xmatcher import CrossMatch, ray_union

    seen = {}

    def fake_union(sources, **kwargs):
        seen.update(kwargs)

    monkeypatch.setattr(ray_union, "ray_union_match", fake_union)
    monkeypatch.setattr(ray_union, "last_plan", lambda: None)
    paths = []
    for name in ("a", "b"):
        path = tmp_path / f"{name}.parquet"
        pl.DataFrame({"ra": [1.0], "dec": [2.0], "e": [0.1]}).write_parquet(path)
        paths.append(str(path))
    CrossMatch().union_match(
        paths,
        output_file=str(tmp_path / "out.hats"),
        engine="ray-union",
        matcher="skyerr",
        ra_err_column_1="e",
        dec_err_column_1="e",
        ra_err_column_2="e",
        dec_err_column_2="e",
        pos_err_units_1="arcsec",
        pos_err_units_2="arcsec",
        no_sync=True,
    )
    assert seen["max_error"] == MatchSpec().max_error == 3.0
