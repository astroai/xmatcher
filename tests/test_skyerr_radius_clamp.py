"""Cross-engine skyerr pre-query clamp: corrupt σ must not explode search radius."""

from __future__ import annotations

import numpy as np
import polars as pl
import pytest

from xmatcher.matchers import MatchSpec, _scipy_match, _torchsky_match, _zone_match_healpix
from xmatcher.sources import CatalogueSource


def _frames():
    left = pl.DataFrame(
        {
            "left_id": [0, 1],
            "ra": [10.0, 10.01],
            "dec": [0.0, 0.0],
            "ra_err": [0.1, 1e6],  # corrupt σ on second row
            "dec_err": [0.1, 1e6],
        }
    )
    right = pl.DataFrame(
        {
            "right_id": [0, 1],
            "ra": [10.0, 10.01],
            "dec": [0.0, 0.0],
            "ra_err": [0.1, 0.1],
            "dec_err": [0.1, 0.1],
        }
    )
    left_src = CatalogueSource(
        name="left",
        is_local=True,
        id_column="left_id",
        ra_column="ra",
        dec_column="dec",
        ra_err_column="ra_err",
        dec_err_column="dec_err",
        pos_err_units="arcsec",
    )
    right_src = CatalogueSource(
        name="right",
        is_local=True,
        id_column="right_id",
        ra_column="ra",
        dec_column="dec",
        ra_err_column="ra_err",
        dec_err_column="dec_err",
        pos_err_units="arcsec",
    )
    return left, right, left_src, right_src


def test_fast_skyerr_clamp_survives_corrupt_sigma():
    left, right, left_src, right_src = _frames()
    spec = MatchSpec(matcher="skyerr", max_error=5.0, radius_arcsec=2.0, find="all")
    l_idx, r_idx, seps = _scipy_match(left, right, left_src, right_src, spec)
    assert len(l_idx) >= 1
    assert float(np.max(seps)) <= 2.0 + 1e-6


def test_zone_skyerr_clamp_survives_corrupt_sigma():
    pytest.importorskip("cdshealpix")
    left, right, left_src, right_src = _frames()
    spec = MatchSpec(matcher="skyerr", max_error=5.0, radius_arcsec=2.0, find="all")
    l_idx, r_idx, seps = _zone_match_healpix(left, right, left_src, right_src, spec)
    assert len(l_idx) >= 1
    assert float(np.max(seps)) <= 2.0 + 1e-6


def test_torchsky_skyerr_clamp_survives_corrupt_sigma():
    pytest.importorskip("torchsky")
    left, right, left_src, right_src = _frames()
    spec = MatchSpec(matcher="skyerr", max_error=5.0, radius_arcsec=2.0, find="all")
    l_idx, r_idx, seps = _torchsky_match(left, right, left_src, right_src, spec)
    assert len(l_idx) >= 1
    assert float(np.max(seps)) <= 2.0 + 1e-6
