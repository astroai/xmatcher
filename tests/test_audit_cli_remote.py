"""Release-audit regressions: engine/remote wrappers keep the shared output schema."""

import polars as pl
import pytest

from xmatcher import stilts
from xmatcher.matchers import MatchSpec
from xmatcher.sources import CatalogueSource


def test_stilts_case_only_column_clash_keeps_separation(monkeypatch):
    """STILTS suffixes ``RA`` against ``ra`` (case-insensitive dups)."""
    from astropy.table import Table

    def fake_run(base, task, params, java_opts=None, tmpdir=None):
        Table(
            {
                "id": [1],
                "ra": [10.0],
                "dec": [20.0],
                "sid": [7],
                "RA_2": [10.0001],
                "DEC_2": [20.0001],
                "Separation": [0.49],
            }
        ).write(params["out"], format="fits")

    monkeypatch.setattr(stilts, "_resolve_base_command", lambda _base: ["stilts"])
    monkeypatch.setattr(stilts, "_run_stilts", fake_run)
    left = pl.DataFrame({"id": [1], "ra": [10.0], "dec": [20.0]})
    right = pl.DataFrame({"sid": [7], "RA": [10.0001], "DEC": [20.0001]})
    result = stilts.stilts_sky_match(
        CatalogueSource(name="a", is_local=True, ra_column="ra", dec_column="dec"),
        CatalogueSource(name="b", is_local=True, ra_column="RA", dec_column="DEC"),
        left,
        right,
        MatchSpec(radius_arcsec=2.0),
    )
    assert result.columns == ["id", "ra", "dec", "sid", "RA", "DEC", "sep_arcsec"]
    assert result["sep_arcsec"][0] == pytest.approx(0.494, abs=1e-3)


def _tap_source(name: str, table: str) -> CatalogueSource:
    return CatalogueSource(
        name=name,
        is_local=False,
        access_method="tap",
        ra_column="ra",
        dec_column="dec",
        tap_url="http://x/tap",
        access_identifier=table,
        default_columns=["source_id", "ra", "dec"],
    )


def test_tap_self_join_uses_local_engine_schema(monkeypatch):
    from astropy.table import Table

    import xmatcher.remote_tap as rt

    captured = {}

    def fake_execute(service, query, maxrec=None):
        captured["query"] = query
        return Table({"source_id": [1], "sep_arcsec": [0.1]})

    monkeypatch.setattr(rt, "get_tap_service", lambda url, auth_session=None: object())
    monkeypatch.setattr(rt, "execute_tap_query", fake_execute)
    rt.tap_self_join(
        _tap_source("c1", "s.t1"),
        _tap_source("c2", "s.t2"),
        MatchSpec(radius_arcsec=1.0, find="all"),
    )
    query = captured["query"]
    assert 'a."source_id" AS "source_id"' in query
    assert 'b."source_id" AS "source_id_2"' in query
    assert 'b."ra" AS "ra_2"' in query
    assert 'AS "sep_arcsec"' in query
    assert "all_sep_arcsec" not in query


def test_cds_xmatch_reports_sep_arcsec(monkeypatch):
    import sys
    import types

    from astropy.table import Table

    import xmatcher.remote_cds as rc

    class FakeXMatch:
        def query(self, cat1, cat2, max_distance, colRA1, colDec1):
            return Table(
                {
                    "angDist": [0.3],
                    rc._XMATCH_KEY: [0],
                    colRA1: [10.0],
                    colDec1: [5.0],
                    "mag": [21.0],
                }
            )

    fake_module = types.ModuleType("astroquery.xmatch")
    fake_module.XMatch = FakeXMatch
    monkeypatch.setitem(sys.modules, "astroquery.xmatch", fake_module)
    local = pl.DataFrame({"ra": [10.0, 80.0], "dec": [5.0, 5.0], "mag": [18.0, 19.0]})
    out = rc.cds_xmatch_local_remote(
        CatalogueSource(name="loc", is_local=True, ra_column="ra", dec_column="dec"),
        CatalogueSource(
            name="viz", is_local=False, access_method="cds_xmatch", access_identifier="I/355"
        ),
        local.lazy(),
        MatchSpec(radius_arcsec=2.0, find="all"),
    )
    assert out.columns == ["ra", "dec", "mag", "mag_2", "sep_arcsec"]
    assert out["sep_arcsec"].to_list() == [0.3]
