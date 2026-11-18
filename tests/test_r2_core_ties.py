"""``find="best"`` resolves exact-duplicate counterparts identically on every engine."""

import numpy as np
import polars as pl
import pytest

from xmatcher import _torchsky
from xmatcher.matchers import MatchSpec, sky_match
from xmatcher.sources import CatalogueSource

_ENGINES = ["fast", "zone", "astropy", "torchsky"]


def _src(name):
    return CatalogueSource(
        name=name,
        is_local=True,
        ra_column="ra",
        dec_column="dec",
        ra_err_column="e_ra",
        dec_err_column="e_dec",
        pos_err_units="arcsec",
        frame="icrs",
    )


def _frames():
    rng = np.random.default_rng(7)
    n_left = 40
    ra = 30.0 + rng.uniform(0.0, 0.2, n_left)
    dec = rng.uniform(-0.1, 0.1, n_left)
    left = pl.DataFrame({"ra": ra, "dec": dec, "e_ra": 0.2, "e_dec": 0.2})
    # Every left source has 25 exact-duplicate counterparts 0.3" away,
    # interleaved through the right table, plus a far decoy.
    copies = 25
    r_ra = np.repeat(ra + 0.3 / 3600.0, copies)
    r_dec = np.repeat(dec, copies)
    order = rng.permutation(r_ra.size)
    right = pl.DataFrame(
        {
            "ra": r_ra[order],
            "dec": r_dec[order],
            "e_ra": 0.2,
            "e_dec": 0.2,
            "owner": np.repeat(np.arange(n_left), copies)[order],
        }
    ).with_row_index("rid")
    expected = right.group_by("owner").agg(pl.col("rid").min()).sort("owner")["rid"].to_list()
    return left.with_row_index("lid"), right, expected


@pytest.mark.parametrize("matcher", ["sky", "skyerr", "skyellipse"])
@pytest.mark.parametrize("engine", _ENGINES)
def test_best_ties_go_to_lowest_right_index(engine, matcher):
    if engine == "torchsky":
        if not _torchsky.installed():
            pytest.skip("torchsky not installed")
        if matcher == "skyellipse":
            pytest.skip("torchsky has no skyellipse")
    left, right, expected = _frames()
    spec = MatchSpec(radius_arcsec=2.0, matcher=matcher, find="best", max_error=5.0)
    result = (
        sky_match(_src("L"), _src("R"), left.lazy(), right.lazy(), spec, engine=engine)
        .collect()
        .sort("lid")
    )
    assert result["lid"].to_list() == list(range(left.height))
    assert result["rid"].to_list() == expected
