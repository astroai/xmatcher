"""A saved ML/XGB model that cannot be used is an error, never silently replaced."""

import numpy as np
import polars as pl
import pytest

from xmatcher.exceptions import CrossMatchError
from xmatcher.matchers import MatchSpec, sky_match
from xmatcher.sources import CatalogueSource


def _src(name):
    return CatalogueSource(name=name, is_local=True, ra_column="ra", dec_column="dec")


@pytest.mark.parametrize(
    "matcher, path_field", [("ml", "ml_model_path"), ("xgb", "xgb_model_path")]
)
def test_unreadable_saved_model_is_an_error_and_is_not_overwritten(tmp_path, matcher, path_field):
    n = 64
    ra = np.linspace(10.0, 10.5, n)
    mag = np.linspace(14.0, 20.0, n)
    left = pl.DataFrame({"ra": ra, "dec": np.zeros(n), "mag": mag})
    right = pl.DataFrame({"ra": ra + 0.2 / 3600.0, "dec": np.zeros(n), "mag": mag + 0.05})
    model = tmp_path / "model.joblib"
    model.write_bytes(b"not a pickled classifier")
    spec = MatchSpec(
        radius_arcsec=2.0, matcher=matcher, ml_color_columns=["mag"], **{path_field: str(model)}
    )
    with pytest.raises(CrossMatchError, match="model"):
        sky_match(_src("L"), _src("R"), left.lazy(), right.lazy(), spec, engine="fast").collect()
    assert model.read_bytes() == b"not a pickled classifier"
