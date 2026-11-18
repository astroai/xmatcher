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
