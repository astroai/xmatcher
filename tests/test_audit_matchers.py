"""Audit regressions: matcher geometry and scoring against independent references.

Random catalogues use fixed seeds and are checked against astropy
(``search_around_sky``, ``spherical_offsets_to``, ``position_angle``) or
closed-form expressions, never against the code under test.
"""

import math

import astropy.units as u
import numpy as np
import polars as pl
import pytest
from astropy.coordinates import SkyCoord, search_around_sky

from xmatcher import bayes
from xmatcher.crossmatch import CrossMatch
from xmatcher.matchers import MatchSpec, _skyellipse_d2, sky_match
from xmatcher.sources import CatalogueSource

ERRORS = {"ra_err_column": "e_ra", "dec_err_column": "e_dec", "corr_column": "rho"}
GAIA_ID = 6_917_528_997_577_384_321  # > 2**53: not representable in float64


def _src(name, **kw):
    return CatalogueSource(name=name, is_local=True, ra_column="ra", dec_column="dec", **kw)


def _region(rng, n, region):
    if region == "wrap":
        return rng.uniform(-0.05, 0.05, n) % 360.0, rng.uniform(-0.05, 0.05, n)
    if region == "north_pole":
        return rng.uniform(0.0, 360.0, n), 90.0 - rng.uniform(0.0, 0.03, n)
    if region == "south_pole":
        return rng.uniform(0.0, 360.0, n), -90.0 + rng.uniform(0.0, 0.03, n)
    return rng.uniform(10.0, 10.1, n), rng.uniform(20.0, 20.1, n)


# ----------------------------------------------------------------- sky engines
@pytest.mark.parametrize("region", ["plain", "wrap", "north_pole", "south_pole"])
@pytest.mark.parametrize("engine", ["fast", "zone", "astropy"])
@pytest.mark.parametrize("find", ["all", "best"])
def test_sky_match_agrees_with_astropy_search_around_sky(region, engine, find):
    rng = np.random.default_rng(20261117)
    ra1, dec1 = _region(rng, 300, region)
    ra2, dec2 = _region(rng, 400, region)
    left = pl.DataFrame({"id": np.arange(300, dtype=np.int64) + GAIA_ID, "ra": ra1, "dec": dec1})
    right = pl.DataFrame({"id": np.arange(400, dtype=np.int64) + GAIA_ID, "ra": ra2, "dec": dec2})
    radius = 10.0
    c1 = SkyCoord(ra1 * u.deg, dec1 * u.deg)
    c2 = SkyCoord(ra2 * u.deg, dec2 * u.deg)
    i1, i2, sep, _ = search_around_sky(c1, c2, radius * u.arcsec)
    if find == "all":
        expected = set(zip(i1.tolist(), i2.tolist(), strict=True))
    else:
        nearest: dict[int, tuple[float, int]] = {}
        for a, b, s in zip(i1.tolist(), i2.tolist(), sep.arcsec.tolist(), strict=True):
            nearest[a] = min(nearest.get(a, (math.inf, -1)), (s, b))
        expected = {(a, b) for a, (_, b) in nearest.items()}

    out = sky_match(
        _src("a"),
        _src("b"),
        left.lazy(),
        right.lazy(),
        MatchSpec(radius_arcsec=radius, find=find, join_type="all1"),
        engine=engine,
    ).collect()

    assert out.schema["id"] == pl.Int64 and out.schema["id_2"] == pl.Int64
    assert out.filter(pl.col("id_2").is_null()).height == 300 - len({a for a, _ in expected})
    matched = out.filter(pl.col("id_2").is_not_null())
    lid = matched["id"].to_numpy() - GAIA_ID
    rid = matched["id_2"].to_numpy() - GAIA_ID
    assert set(zip(lid.tolist(), rid.tolist(), strict=True)) == expected
    reference = c1[lid].separation(c2[rid]).arcsec
    np.testing.assert_allclose(matched["sep_arcsec"].to_numpy(), reference, rtol=0, atol=1e-9)


# ------------------------------------------------------------------ skyellipse
def _reference_d2(ra1, dec1, ra2, dec2, cov1, cov2):
    """d² on the second source's tangent plane, from astropy offsets and PAs."""
    c1 = SkyCoord(ra1 * u.deg, dec1 * u.deg)
    c2 = SkyCoord(ra2 * u.deg, dec2 * u.deg)
    d_east, d_north = c2.spherical_offsets_to(c1)
    delta = -np.stack([d_east.arcsec, d_north.arcsec], axis=-1)  # source 1 minus source 2
    # Parallel transport along the geodesic: the frame of source 1 is rotated by
    # gamma relative to the frame of source 2 (position angles east of north).
    pa_12 = c1.position_angle(c2).rad
    pa_21 = c2.position_angle(c1).rad
    gamma = (pa_21 + math.pi) - pa_12

    def matrix(cov):
        sxx, syy, rho = cov
        sxy = rho * np.sqrt(sxx * syy)
        return np.stack([np.stack([sxx, sxy], -1), np.stack([sxy, syy], -1)], -2)

    # A direction at position angle phi in frame 1 has angle phi + gamma in
    # frame 2. With (east, north) = (sin phi, cos phi) components that is
    # R = [[cos g, sin g], [-sin g, cos g]].
    cos_g, sin_g = np.cos(gamma), np.sin(gamma)
    rot = np.stack([np.stack([cos_g, sin_g], -1), np.stack([-sin_g, cos_g], -1)], -2)
    c1_in_2 = rot @ matrix(cov1) @ np.swapaxes(rot, -1, -2)
    total = c1_in_2 + matrix(cov2)
    return np.einsum("ni,nij,nj->n", delta, np.linalg.inv(total), delta)


def _random_pairs(rng, n, dec_low, dec_high):
    ra1 = rng.uniform(0.0, 360.0, n)
    dec1 = rng.uniform(dec_low, dec_high, n)
    offset = rng.uniform(0.0, 3.0, n) / 3600.0
    pa = rng.uniform(0.0, 2 * math.pi, n)
    c2 = SkyCoord(ra1 * u.deg, dec1 * u.deg).directional_offset_by(pa * u.rad, offset * u.deg)
    cov = []
    for _ in range(2):
        cov.append(
            (
                rng.uniform(0.05, 1.0, n) ** 2,
                rng.uniform(0.05, 1.0, n) ** 2,
                rng.uniform(-0.9, 0.9, n),
            )
        )
    return ra1, dec1, c2.ra.deg, c2.dec.deg, cov[0], cov[1]


@pytest.mark.parametrize(("dec_low", "dec_high"), [(-60.0, 60.0), (89.999, 90.0), (-90.0, -89.999)])
def test_skyellipse_d2_matches_astropy_tangent_plane_reference(dec_low, dec_high):
    rng = np.random.default_rng(7)
    ra1, dec1, ra2, dec2, cov1, cov2 = _random_pairs(rng, 500, dec_low, dec_high)
    got = _skyellipse_d2(ra1, dec1, ra2, dec2, cov1, cov2)
    expected = _reference_d2(ra1, dec1, ra2, dec2, cov1, cov2)
    np.testing.assert_allclose(got, expected, rtol=1e-5)


def test_skyellipse_d2_across_the_pole_rotates_both_ellipses():
    # Left at RA 0 and right at RA 90, both eps from the pole: separation
    # sqrt(2)*eps = 1". Each ellipse is elongated along its own east axis,
    # which point in perpendicular directions; on the midpoint plane the
    # combined covariance is 1.01 * I, so d2 = 1 / 1.01.
    eps = 1.0 / math.sqrt(2.0) / 3600.0
    d2 = _skyellipse_d2(
        np.array([0.0]),
        np.array([90.0 - eps]),
        np.array([90.0]),
        np.array([90.0 - eps]),
        (np.array([1.0]), np.array([0.01]), np.array([0.0])),
        (np.array([1.0]), np.array([0.01]), np.array([0.0])),
    )
    assert d2[0] == pytest.approx(1.0 / 1.01, rel=1e-6)


@pytest.mark.parametrize("engine", ["fast", "zone", "astropy", "ray"])
def test_skyellipse_accepts_close_pair_straddling_the_pole(engine):
    # 0.72" apart across the pole with 0.5" per-axis errors: d2 = 1.04.
    dec = 90.0 - 0.36 / 3600.0
    left = pl.DataFrame({"ra": [0.0], "dec": [dec], "e_ra": [0.5], "e_dec": [0.5], "rho": [0.0]})
    right = left.with_columns(pl.lit(180.0).alias("ra"))
    out = sky_match(
        _src("a", **ERRORS),
        _src("b", **ERRORS),
        left.lazy(),
        right.lazy(),
        MatchSpec(radius_arcsec=5.0, matcher="skyellipse", max_error=1.5),
        engine=engine,
    ).collect()
    assert out["sep_arcsec"].to_list() == pytest.approx([0.72])


@pytest.mark.parametrize("engine", ["fast", "zone", "astropy", "ray"])
@pytest.mark.parametrize("matcher", ["skyerr", "skyellipse"])
def test_error_matchers_never_exceed_radius_ceiling(engine, matcher):
    left = pl.DataFrame({"ra": [10.0], "dec": [0.0], "e_ra": [10.0], "e_dec": [10.0], "rho": [0.0]})
    right = left.with_columns((pl.col("ra") + 5.0 / 3600.0).alias("ra"))
    out = sky_match(
        _src("a", **ERRORS),
        _src("b", **ERRORS),
        left.lazy(),
        right.lazy(),
        MatchSpec(radius_arcsec=1.0, matcher=matcher, max_error=3.0),
        engine=engine,
    ).collect()
    assert out.height == 0


@pytest.mark.parametrize("engine", ["fast", "zone", "astropy"])
def test_skyellipse_pair_set_matches_reference_d2(engine):
    rng = np.random.default_rng(11)
    n = 400
    ra1, dec1, ra2, dec2, cov1, cov2 = _random_pairs(rng, n, 30.0, 30.5)
    max_error = 2.0
    left = pl.DataFrame(
        {
            "ra": ra1,
            "dec": dec1,
            "e_ra": np.sqrt(cov1[0]),
            "e_dec": np.sqrt(cov1[1]),
            "rho": cov1[2],
        }
    )
    right = pl.DataFrame(
        {
            "ra": ra2,
            "dec": dec2,
            "e_ra": np.sqrt(cov2[0]),
            "e_dec": np.sqrt(cov2[1]),
            "rho": cov2[2],
        }
    )
    c1 = SkyCoord(ra1 * u.deg, dec1 * u.deg)
    c2 = SkyCoord(ra2 * u.deg, dec2 * u.deg)
    radius = 30.0
    i1, i2, _, _ = search_around_sky(c1, c2, radius * u.arcsec)
    d2 = _reference_d2(
        ra1[i1],
        dec1[i1],
        ra2[i2],
        dec2[i2],
        tuple(values[i1] for values in cov1),
        tuple(values[i2] for values in cov2),
    )
    # Ignore pairs within float noise of the threshold.
    assert not np.any(np.abs(d2 - max_error**2) < 1e-6)
    expected = {(a, b) for a, b, d in zip(i1, i2, d2, strict=True) if d <= max_error**2}
    out = sky_match(
        _src("a", **ERRORS),
        _src("b", **ERRORS),
        left.with_row_index("i").lazy(),
        right.with_row_index("j").lazy(),
        MatchSpec(radius_arcsec=radius, matcher="skyellipse", max_error=max_error, find="all"),
        engine=engine,
    ).collect()
    got = set(zip(out["i"].to_list(), out["j"].to_list(), strict=True))
    assert got == expected
    assert len(expected) > 100


# ------------------------------------------------------------ likelihood ratio
def test_likelihood_ratio_prefers_coincident_counterpart():
    rng = np.random.default_rng(0)
    n_bg = 2000
    right = pl.DataFrame(
        {
            "ra": np.r_[10.0 + 0.02 / 3600.0, 10.0 + 1.2 / 3600.0, rng.uniform(9.5, 10.5, n_bg)],
            "dec": np.r_[0.0, 0.0, rng.uniform(-0.5, 0.5, n_bg)],
            "mag": np.r_[18.0, 18.0, rng.uniform(15.0, 22.0, n_bg)],
        }
    )
    left = pl.DataFrame({"ra": [10.0], "dec": [0.0]})
    out = sky_match(
        _src("a", default_pos_error_arcsec=0.3),
        _src("b", default_pos_error_arcsec=0.3),
        left.lazy(),
        right.lazy(),
        MatchSpec(radius_arcsec=3.0, matcher="lr", lr_magnitude_column="mag", find="all"),
        engine="fast",
    ).collect()
    by_sep = dict(zip(out["sep_arcsec"].round(6).to_list(), out["lr"].to_list(), strict=True))
    # Same magnitude bin, so the LR ratio is the ratio of 2-D Gaussian
    # surface densities with per-axis variance 0.3² + 0.3².
    var = 0.3**2 + 0.3**2
    expected_ratio = math.exp(-(0.02**2 - 1.2**2) / (2.0 * var))
    assert by_sep[0.02] / by_sep[1.2] == pytest.approx(expected_ratio, rel=1e-9)
    best = sky_match(
        _src("a", default_pos_error_arcsec=0.3),
        _src("b", default_pos_error_arcsec=0.3),
        left.lazy(),
        right.lazy(),
        MatchSpec(radius_arcsec=3.0, matcher="lr", lr_magnitude_column="mag"),
        engine="fast",
    ).collect()
    assert best["sep_arcsec"].to_list() == pytest.approx([0.02])


# --------------------------------------------------------------- Bayes factors
def test_nway_bayes_factor_closed_forms():
    rng = np.random.default_rng(3)
    n = 50
    ra = [rng.uniform(0, 360, n)]
    dec = [rng.uniform(-80, 80, n)]
    sig = [rng.uniform(0.1, 1.0, n) for _ in range(3)]
    base = SkyCoord(ra[0] * u.deg, dec[0] * u.deg)
    for _ in range(2):
        moved = base.directional_offset_by(
            rng.uniform(0, 2 * math.pi, n) * u.rad, rng.uniform(0, 2.0, n) * u.arcsec
        )
        ra.append(moved.ra.deg)
        dec.append(moved.dec.deg)
    coords = [SkyCoord(r * u.deg, d * u.deg) for r, d in zip(ra, dec, strict=True)]
    arcsec = math.radians(1.0 / 3600.0)

    # N = 2: B = 2 / s² exp(-psi² / 2s²) with s² = sigma1² + sigma2² (radians).
    s2 = (sig[0] ** 2 + sig[1] ** 2) * arcsec**2
    psi = coords[0].separation(coords[1]).rad
    expected2 = np.log10(2.0 / s2) - psi**2 / (2.0 * s2) / math.log(10.0)
    got2 = bayes.compute_nway_bayes_factor(ra[:2], dec[:2], sig[:2], 1.0)
    np.testing.assert_allclose(got2, expected2, rtol=1e-10)

    # N = 3: Budavari & Szalay (2008) eq. 18 written out term by term.
    w = [1.0 / (s * arcsec) ** 2 for s in sig]
    total = w[0] + w[1] + w[2]
    chi = sum(
        w[i] * w[j] * coords[i].separation(coords[j]).rad ** 2 for i, j in ((0, 1), (0, 2), (1, 2))
    )
    expected3 = np.log10(4.0 * w[0] * w[1] * w[2] / total) - chi / (2.0 * total) / math.log(10.0)
    got3 = bayes.compute_nway_bayes_factor(ra, dec, sig, 1.0)
    np.testing.assert_allclose(got3, expected3, rtol=1e-10)


def test_pairwise_p_match_closed_form():
    sep = np.array([0.0, 0.5, 2.0, 4.0])
    sl, sr, radius = np.array([0.3] * 4), np.array([0.4] * 4), 5.0
    s2 = 0.3**2 + 0.4**2
    match = np.exp(-(sep**2) / (2 * s2)) / (2 * math.pi * s2)
    background = 1.0 / (math.pi * radius**2)
    expected = match / (match + background)
    np.testing.assert_allclose(bayes.compute_p_match(sep, sl, sr, radius), expected, rtol=1e-12)


# ------------------------------------------------------------- friends of friends
def test_fof_preserves_int64_ids_and_wraps_coordinate_means():
    primary = pl.DataFrame({"id": [1, 2], "ra": [359.99995, 10.0], "dec": [0.0, 0.0]})
    secondary = pl.DataFrame(
        {
            "source_id": [GAIA_ID, GAIA_ID + 1, GAIA_ID + 2],
            "ra": [359.99998, 0.00004, 10.0],
            "dec": [0.0, 0.0, 0.0],
            "mag": [15.0, 16.0, float("nan")],
        }
    )
    out = CrossMatch().fof_match([primary, secondary], radius_arcsec=1.0, engine="fast")
    assert out.schema["source_id"] == pl.Int64
    assert out["source_id"].to_list() == [GAIA_ID, GAIA_ID + 2]
    # Two members either side of RA = 0: the mean stays at RA ~ 0, not 180.
    assert out["ra_2"][0] == pytest.approx(0.00001, abs=1e-9)
    assert out["mag"].to_list()[0] == pytest.approx(15.5)
    assert out["mag"].to_list()[1] is None


def test_fof_keeps_columns_of_catalogues_first_seen_after_many_bundles():
    n = 150
    first = pl.DataFrame({"ra": np.arange(n) * 0.01, "dec": np.zeros(n)})
    second = first.clone()
    third = pl.DataFrame({"ra": [(n - 1) * 0.01], "dec": [0.0], "flux": [5.0]})
    out = CrossMatch().fof_match([first, second, third], radius_arcsec=1.0, engine="fast")
    assert out.height == n
    assert {"ra_3", "dec_3", "flux"} <= set(out.columns)
    assert out["flux"].to_list() == [None] * (n - 1) + [5.0]
    assert out["_src_cats"][-1] == "1+2+3"
    assert out["n_cats"][-1] == 3


def test_fof_suffixes_never_overwrite_existing_columns():
    primary = pl.DataFrame({"ra": [1.0], "dec": [0.0], "mag": [11.0], "mag_2": [12.0]})
    secondary = pl.DataFrame({"ra": [1.0], "dec": [0.0], "mag": [22.0]})
    out = CrossMatch().fof_match([primary, secondary], radius_arcsec=1.0, engine="fast")
    assert out["mag"].to_list() == [11.0]
    assert out["mag_2"].to_list() == [12.0]
    assert out["mag_2_2"].to_list() == [22.0]
