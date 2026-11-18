"""Public Torchsky (>=0.5.0) integration.

The version floor is checked with stand-in modules, so it runs everywhere.
Everything else needs the real package and runs in the Torchsky pixi
environment (``pixi run -e torchsky ci-local``), where
``test_torchsky_environment_is_complete`` makes sure none of it is skipped.
Results are compared with Astropy, brute force or the default engines, never
with Torchsky itself.
"""

from __future__ import annotations

import math
import os
import sys
import types
from pathlib import Path

import astropy.units as u
import numpy as np
import polars as pl
import pytest
from astropy.coordinates import SkyCoord, search_around_sky

from xmatcher import _torchsky, astro_utils, hats_native
from xmatcher.exceptions import CrossMatchError
from xmatcher.matchers import MatchSpec, sky_match
from xmatcher.sources import CatalogueSource

needs_torchsky = pytest.mark.skipif(
    not _torchsky.installed(), reason="torchsky is not installed (pixi run -e torchsky ...)"
)


def _src(name: str, **kw) -> CatalogueSource:
    return CatalogueSource(name=name, is_local=True, ra_column="ra", dec_column="dec", **kw)


def test_torchsky_environment_is_complete() -> None:
    """In the torchsky environment every torchsky test must run, not skip."""
    if os.environ.get("XMATCHER_EXPECT_TORCHSKY") != "1":
        pytest.skip("asserted only in the torchsky pixi environment")
    assert _torchsky.installed()
    for name in ("catalogs", "wcs", "sphere.healpix"):
        assert _torchsky.module(name) is not None


# --------------------------------------------------------------- version floor
@pytest.fixture
def stand_in(monkeypatch: pytest.MonkeyPatch):
    """Install a fake ``torchsky`` with a chosen ``__version__``."""

    def install(version: str | None) -> types.ModuleType:
        root = types.ModuleType("torchsky")
        root.__file__ = "/stand-in/torchsky/__init__.py"
        if version is not None:
            root.__version__ = version  # type: ignore[attr-defined]
        wcs = types.ModuleType("torchsky.wcs")
        wcs.propagate_proper_motion = lambda *a, **k: (a[0], a[1])  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, "torchsky", root)
        monkeypatch.setitem(sys.modules, "torchsky.wcs", wcs)
        monkeypatch.setattr(_torchsky, "installed", lambda: True)
        return wcs

    _torchsky._checked.cache_clear()
    yield install
    _torchsky._checked.cache_clear()


@pytest.mark.parametrize("version", ["0.5.0", "0.5.1.dev3+g1234abc", "1.0.0"])
def test_supported_torchsky_versions_are_used(stand_in, version: str) -> None:
    wcs = stand_in(version)
    assert _torchsky.module("wcs") is wcs


@pytest.mark.parametrize(
    ("version", "found"), [("0.4.0", "found 0.4.0"), (None, "found an unversioned copy")]
)
def test_older_torchsky_is_refused_not_ignored(stand_in, version, found) -> None:
    stand_in(version)
    with pytest.raises(CrossMatchError, match=rf"needs torchsky>=0\.5\.0, {found}"):
        _torchsky.module("wcs")
    # Propagation must not quietly switch to Astropy behind an old Torchsky.
    with pytest.raises(CrossMatchError, match="needs torchsky>=0.5.0"):
        astro_utils.propagate_proper_motion(
            np.array([10.0]), np.array([0.0]), np.zeros(1), np.zeros(1), np.array([2000.0]), 2016.0
        )


def test_missing_torchsky_names_the_extra(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_torchsky, "installed", lambda: False)
    assert _torchsky.module("wcs") is None
    with pytest.raises(CrossMatchError, match=r"xmatcher\[torchsky\]"):
        _torchsky.require("catalogs", "engine='torchsky'")


# ------------------------------------------------------------- engine parity
def _region(rng: np.random.Generator, n: int, region: str) -> tuple[np.ndarray, np.ndarray]:
    if region == "wrap":
        return rng.uniform(-0.05, 0.05, n) % 360.0, rng.uniform(-0.05, 0.05, n)
    if region == "north_pole":
        return rng.uniform(0.0, 360.0, n), 90.0 - rng.uniform(0.0, 0.03, n)
    if region == "south_pole":
        return rng.uniform(0.0, 360.0, n), -90.0 + rng.uniform(0.0, 0.03, n)
    return rng.uniform(10.0, 10.1, n), rng.uniform(20.0, 20.1, n)


@needs_torchsky
@pytest.mark.parametrize("region", ["plain", "wrap", "north_pole", "south_pole"])
@pytest.mark.parametrize("find", ["all", "best"])
def test_torchsky_sky_engine_agrees_with_astropy(region: str, find: str) -> None:
    rng = np.random.default_rng(5)
    ra1, dec1 = _region(rng, 300, region)
    ra2, dec2 = _region(rng, 400, region)
    left = pl.DataFrame({"id": np.arange(300), "ra": ra1, "dec": dec1})
    right = pl.DataFrame({"id": np.arange(400), "ra": ra2, "dec": dec2})
    radius = 10.0
    c1, c2 = SkyCoord(ra1 * u.deg, dec1 * u.deg), SkyCoord(ra2 * u.deg, dec2 * u.deg)
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
        MatchSpec(radius_arcsec=radius, find=find),
        engine="torchsky",
    ).collect()

    pairs = list(zip(out["id"].to_list(), out["id_2"].to_list(), strict=True))
    assert len(pairs) == len(set(pairs))
    assert set(pairs) == expected
    reference = c1[out["id"].to_numpy()].separation(c2[out["id_2"].to_numpy()]).arcsec
    np.testing.assert_allclose(out["sep_arcsec"].to_numpy(), reference, rtol=0, atol=1e-6)


@needs_torchsky
@pytest.mark.parametrize("find", ["all", "best"])
@pytest.mark.parametrize("radius", [1.0, 30.0], ids=["radius-capped", "sigma-bound"])
def test_torchsky_skyerr_engine_agrees_with_fast(find: str, radius: float) -> None:
    rng = np.random.default_rng(9)
    n1, n2 = 400, 500
    ra1, dec1 = rng.uniform(150.0, 150.05, n1), rng.uniform(2.0, 2.05, n1)
    ra2, dec2 = rng.uniform(150.0, 150.05, n2), rng.uniform(2.0, 2.05, n2)
    errors = {"ra_err_column": "e_ra", "dec_err_column": "e_dec"}
    left = pl.DataFrame(
        {"id": np.arange(n1), "ra": ra1, "dec": dec1, "e_ra": rng.uniform(0.05, 3.0, n1)}
    ).with_columns(pl.col("e_ra").alias("e_dec"))
    right = pl.DataFrame(
        {"id": np.arange(n2), "ra": ra2, "dec": dec2, "e_ra": rng.uniform(0.05, 3.0, n2)}
    ).with_columns(pl.col("e_ra").alias("e_dec"))
    spec = MatchSpec(radius_arcsec=radius, matcher="skyerr", max_error=3.0, find=find)

    def run(engine: str) -> pl.DataFrame:
        return (
            sky_match(
                _src("a", **errors),
                _src("b", **errors),
                left.lazy(),
                right.lazy(),
                spec,
                engine=engine,
            )
            .collect()
            .sort(["id", "id_2"])
        )

    torchsky, fast = run("torchsky"), run("fast")
    assert torchsky.height > 0
    assert torchsky.select("id", "id_2").equals(fast.select("id", "id_2"))
    np.testing.assert_allclose(torchsky["sep_arcsec"], fast["sep_arcsec"], rtol=0, atol=1e-6)
    assert float(torchsky["sep_arcsec"].max()) <= radius


@needs_torchsky
def test_torchsky_engine_matches_galactic_inputs_like_fast() -> None:
    """Both sides in one non-ICRS frame: Torchsky converts to ICRS, fast does not."""
    rng = np.random.default_rng(13)
    lon, lat = rng.uniform(30.0, 30.05, 300), rng.uniform(-1.0, -0.95, 300)
    left = pl.DataFrame({"id": np.arange(300), "ra": lon, "dec": lat})
    right = pl.DataFrame(
        {"id": np.arange(300), "ra": lon + rng.normal(0, 0.3, 300) / 3600.0, "dec": lat}
    )

    def run(engine: str) -> pl.DataFrame:
        return (
            sky_match(
                _src("a", frame="galactic"),
                _src("b", frame="galactic"),
                left.lazy(),
                right.lazy(),
                MatchSpec(radius_arcsec=1.0, find="all"),
                engine=engine,
            )
            .collect()
            .sort(["id", "id_2"])
        )

    torchsky, fast = run("torchsky"), run("fast")
    assert torchsky.height > 250
    assert torchsky.select("id", "id_2").equals(fast.select("id", "id_2"))
    np.testing.assert_allclose(torchsky["sep_arcsec"], fast["sep_arcsec"], rtol=0, atol=1e-6)


# --------------------------------------------------------- propagation parity
def _sep_mas(ra1, dec1, ra2, dec2) -> np.ndarray:
    return SkyCoord(ra1 * u.deg, dec1 * u.deg).separation(SkyCoord(ra2 * u.deg, dec2 * u.deg)).mas


def _physical_stars(n: int, seed: int) -> dict[str, np.ndarray]:
    """Random stars up to Barnard's-star proper motion and 900 km/s transverse."""
    rng = np.random.default_rng(seed)
    dec = np.degrees(np.arcsin(rng.uniform(-1.0, 1.0, n)))
    dec[: n // 20] = rng.uniform(89.9, 89.99999, n // 20) * rng.choice([-1.0, 1.0], n // 20)
    plx = 10 ** rng.uniform(-0.5, 2.85, n)
    pm = np.minimum(rng.uniform(0.0, 900.0, n) * plx / 4.74047, 10_400.0)
    angle = rng.uniform(0.0, 2.0 * np.pi, n)
    return {
        "ra": rng.uniform(0.0, 360.0, n),
        "dec": dec,
        "pmra": pm * np.sin(angle),
        "pmdec": pm * np.cos(angle),
        "plx": plx,
        "rv": rng.normal(0.0, 150.0, n),
    }


@needs_torchsky
@pytest.mark.parametrize(("source", "target"), [(2016.0, 2000.0), (2016.0, 1950.0)])
def test_torchsky_propagation_agrees_with_astropy(monkeypatch, source, target) -> None:
    """Installing Torchsky switches the propagation backend; results must not move."""
    s = _physical_stars(4000, seed=int(target))
    epochs = np.full(s["ra"].size, source)
    pm_args = (s["ra"], s["dec"], s["pmra"], s["pmdec"], epochs, target)
    space_args = (s["ra"], s["dec"], s["pmra"], s["pmdec"], s["plx"], s["rv"], epochs, target)
    with_torchsky = (
        astro_utils.propagate_proper_motion(*pm_args),
        astro_utils.propagate_space_motion(*space_args),
    )
    monkeypatch.setattr(astro_utils, "_load_torchsky_propagate_proper_motion", lambda: None)
    monkeypatch.setattr(astro_utils, "_load_torchsky_propagate_space_motion", lambda: None)
    with_astropy = (
        astro_utils.propagate_proper_motion(*pm_args),
        astro_utils.propagate_space_motion(*space_args),
    )
    # 66 years of Barnard's-star motion: PM-only within 0.05 mas, full space
    # motion (perspective acceleration, light time) within 0.2 mas.
    assert _sep_mas(*with_torchsky[0], *with_astropy[0]).max() < 0.05
    assert _sep_mas(*with_torchsky[1], *with_astropy[1]).max() < 0.2


@needs_torchsky
def test_torchsky_motion_jacobian_matches_astropy_finite_differences(monkeypatch) -> None:
    """The (east, north, pm_east, pm_north) Jacobian, in mas, against Astropy.

    Target-epoch skyellipse propagates Gaia-style covariances with it, so its
    local tangent basis and units must be those of ``ra_error``/``dec_error``.
    """
    # Away from the poles: there a 26-year path can pass the pole itself and
    # the local frame turns through large angles (positions are still checked
    # by the parity test above).
    s = {key: value[2:] for key, value in _physical_stars(42, seed=3).items()}
    assert np.abs(s["dec"]).max() < 89.0
    epochs = np.full(40, 2016.0)
    _, _, jacobian = astro_utils.propagate_proper_motion_with_jacobian(
        s["ra"], s["dec"], s["pmra"], s["pmdec"], epochs, 1990.0
    )
    monkeypatch.setattr(astro_utils, "_load_torchsky_propagate_proper_motion", lambda: None)

    def moved(ra, dec, pmra, pmdec) -> SkyCoord:
        out = astro_utils.propagate_proper_motion(ra, dec, pmra, pmdec, epochs, 1990.0)
        return SkyCoord(out[0] * u.deg, out[1] * u.deg)

    start = SkyCoord(s["ra"] * u.deg, s["dec"] * u.deg)
    for k in range(4):
        columns = []
        for sign in (1.0, -1.0):
            d = np.zeros(4)
            d[k] = sign  # 1 mas or 1 mas/yr
            shifted = start.spherical_offsets_by(d[0] * u.mas, d[1] * u.mas)
            columns.append(
                moved(shifted.ra.deg, shifted.dec.deg, s["pmra"] + d[2], s["pmdec"] + d[3])
            )
        east, north = columns[1].spherical_offsets_to(columns[0])
        reference = np.stack([east.mas, north.mas], axis=1) / 2.0
        # The models rotate the local frame along the path slightly differently
        # (growing with motion x tan(Dec)): cross terms agree to 1e-4, a 0.01%
        # covariance effect.
        np.testing.assert_allclose(jacobian[:, :, k], reference, rtol=1e-5, atol=1e-4)


# ---------------------------------------------------------------- HATS paths
@needs_torchsky
@pytest.mark.parametrize("order", [3, 6, 9])
def test_torchsky_neighbour_cover_contains_every_reachable_pixel(order: int) -> None:
    """Pixels holding any point within r of the centre pixel are in the cover."""
    import cdshealpix
    from astropy.coordinates import Latitude, Longitude

    rng = np.random.default_rng(order)
    radius_arcsec = 30.0
    pixel_rad = math.sqrt(4.0 * math.pi / (12 * 4**order))
    depth = np.uint64(order)
    for center in rng.integers(0, 12 * 4**order, 25):
        cover = set(hats_native._healpix_neighbors(order, int(center), radius_arcsec=radius_arcsec))
        lon, lat = cdshealpix.nested.healpix_to_lonlat(np.array([center], dtype=np.uint64), order)
        middle = SkyCoord(lon[0], lat[0])
        # Points of the centre pixel (rejection-sampled), pushed up to r away.
        cloud = middle.directional_offset_by(
            rng.uniform(0, 360, 6000) * u.deg,
            np.sqrt(rng.uniform(0, 1, 6000)) * 1.2 * pixel_rad * u.rad,
        )
        inside = cdshealpix.nested.lonlat_to_healpix(
            Longitude(cloud.ra), Latitude(cloud.dec), depth
        ) == np.uint64(center)
        start = cloud[inside]
        assert len(start) > 100
        moved = start.directional_offset_by(
            rng.uniform(0, 360, len(start)) * u.deg,
            rng.uniform(0, radius_arcsec, len(start)) * u.arcsec,
        )
        reached = cdshealpix.nested.lonlat_to_healpix(
            Longitude(moved.ra), Latitude(moved.dec), depth
        )
        assert {int(p) for p in reached} <= cover


@needs_torchsky
@pytest.mark.filterwarnings("error:The given NumPy array is not writable")
def test_torchsky_pruned_hats_match_equals_brute_force(tmp_path: Path) -> None:
    """Inner join, single-order HATS left, local right: the pruned path."""
    from .test_audit_hats import (
        _boundary_pairs,
        _brute_pairs,
        _hats_source,
        _pairs,
        _reference_tree,
    )

    radius = 30.0
    left, right = _boundary_pairs(6000, radius, seed=8)
    right = right.head(300)  # a sparse right side, so most left pixels are pruned
    lroot = _reference_tree(tmp_path / "left", left, 4)
    spec = MatchSpec(radius_arcsec=radius, join_type="1and2", find="all")
    got = _pairs(
        hats_native.hats_native_crossmatch(
            _hats_source("L", lroot), _src("R"), spec, local_lf2=right.lazy()
        )
    )
    assert len(got) == len(set(got))
    assert set(got) == _brute_pairs(left, right, radius)
