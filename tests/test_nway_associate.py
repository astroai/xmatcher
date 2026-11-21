"""N-way association through torchsky ``associate_many`` (nway_match, matcher='lr')."""

from __future__ import annotations

import math

import numpy as np
import polars as pl
import pytest

from xmatcher import CrossMatch
from xmatcher.exceptions import CrossMatchError

pytest.importorskip("torchsky.catalogs")

_ERRORS = {f"{k}_{i}": "err" for i in (1, 2, 3) for k in ("ra_err_column", "dec_err_column")}


def _scene(seed: int = 29, n: int = 600, n_bg: int = 4800, keep=(0.8, 0.6)):
    """Primary plus two secondaries with about one background source per gate.

    Per-axis errors are log-uniform in 0.08-0.5 arcsec. Each secondary holds a fraction
    ``keep`` of the primaries plus uniform background; true counterparts have
    magnitude ``G - 1 +- 0.2`` (G in 14-18), background 18-22: the injected
    scene of torchsky's ``test_nway_photometric_terms_raise_map_accuracy``.
    """
    rng = np.random.default_rng(seed)
    box = 0.1 * math.sqrt(n / 1500)  # torchsky's density on a smaller patch
    ra = 150 + rng.uniform(0, box, n)
    dec = 2 + rng.uniform(0, box, n)
    gmag = 14 + 4 * rng.uniform(size=n)
    frames, truth = [], []
    for c, frac in enumerate((1.0, *keep)):
        src = np.flatnonzero(rng.uniform(size=n) < frac)
        n_bg_c = n_bg if c else 0
        ra0 = np.concatenate([ra[src], 150 + rng.uniform(0, box, n_bg_c)])
        dec0 = np.concatenate([dec[src], 2 + rng.uniform(0, box, n_bg_c)])
        sig = 0.08 * (0.5 / 0.08) ** rng.uniform(size=ra0.size)  # log-uniform, arcsec
        dec_obs = dec0 + rng.normal(size=ra0.size) * sig / 3600
        ra_obs = ra0 + rng.normal(size=ra0.size) * sig / 3600 / np.cos(np.radians(dec0))
        if c == 0:
            mag = gmag.copy()
        else:
            mag = 18 + 4 * rng.uniform(size=ra0.size)
            mag[: src.size] = gmag[src] - 1.0 + 0.2 * rng.normal(size=src.size)
        frames.append(
            pl.DataFrame(
                {"id": np.arange(ra0.size), "ra": ra_obs, "dec": dec_obs, "err": sig, "mag": mag}
            )
        )
        t = np.full(n, -1)
        t[src] = np.arange(src.size)
        truth.append(t)
    return frames, truth


def _map_accuracy(result: pl.DataFrame, truth: list[np.ndarray]) -> float:
    """Fraction of primaries whose MAP members (or absences) are all right."""
    pred = np.full((truth[0].size, 2), -1)
    rows = result["id"].to_numpy()
    pred[rows, 0] = result["id_2"].fill_null(-1).to_numpy()
    pred[rows, 1] = result["id_3"].fill_null(-1).to_numpy()
    return float(((pred[:, 0] == truth[1]) & (pred[:, 1] == truth[2])).mean())


def test_photometric_terms_raise_nway_map_accuracy():
    frames, truth = _scene()
    cm = CrossMatch()
    acc = {}
    for name, extra in (
        ("plain", {}),
        ("mag", {"lr_magnitude_column": "mag"}),
        ("colour", {"lr_magnitude_column": "mag", "lr_color_column": "mag"}),
    ):
        result = cm.nway_match(frames, radius_arcsec=30.0, find="best", **_ERRORS, **extra)
        assert result is not None
        acc[name] = _map_accuracy(result, truth)
    # Same direction as torchsky's injected scene: photometry fixes MAP triples
    # that position alone gets wrong.
    assert acc["mag"] > acc["plain"] + 0.08, acc
    assert acc["colour"] > acc["plain"] + 0.08, acc
    assert min(acc["mag"], acc["colour"]) > 0.85, acc


def test_lr_with_three_catalogues_is_one_nway_association():
    frames, truth = _scene(seed=3, n=300, n_bg=2400)
    cm = CrossMatch()
    result = cm.crossmatch_multi(
        frames,
        radius_arcsec=30.0,
        matcher="lr",
        lr_magnitude_column="mag",
        lr_q=0.7,
        engine="fast",
        **_ERRORS,
    )
    assert result is not None
    # find="best" (the default): one MAP row per primary with a counterpart.
    assert result["id"].is_unique().all()
    assert result["nway_best"].all()
    reliability = result["reliability"].to_numpy()
    assert ((reliability >= 0.0) & (reliability <= 1.0)).all()
    assert (result["p_any"].to_numpy() >= reliability - 1e-12).all()
    assert _map_accuracy(result, truth) > 0.85

    every = cm.crossmatch_multi(
        frames,
        radius_arcsec=30.0,
        matcher="lr",
        lr_magnitude_column="mag",
        lr_q=0.7,
        find="all",
        **_ERRORS,
    )
    assert every is not None
    # Posteriors of a primary's hypotheses with a counterpart sum to p_any.
    sums = every.group_by("id").agg(pl.col("reliability").sum(), pl.col("p_any").first())
    np.testing.assert_allclose(sums["reliability"], sums["p_any"], atol=1e-9)
    assert every.filter(pl.col("nway_best")).sort("id").equals(result.sort("id"))

    with_unmatched = cm.crossmatch_multi(
        frames,
        radius_arcsec=30.0,
        matcher="lr",
        lr_magnitude_column="mag",
        lr_q=0.7,
        join_type="all1",
        **_ERRORS,
    )
    assert with_unmatched is not None
    assert with_unmatched.height == frames[0].height
    unmatched = with_unmatched.filter(pl.col("reliability").is_null())
    assert unmatched["id_2"].null_count() == unmatched.height
    assert unmatched["nway_best"].null_count() == unmatched.height


def test_lr_with_three_catalogues_fails_closed():
    frames, _ = _scene(seed=4, n=40, n_bg=100)
    cm = CrossMatch()
    with pytest.raises(CrossMatchError, match="join_type"):
        cm.crossmatch_multi(
            frames, matcher="lr", lr_magnitude_column="mag", join_type="1or2", **_ERRORS
        )
    with pytest.raises(CrossMatchError, match="every secondary"):
        cm.crossmatch_multi(frames, matcher="lr", lr_magnitude_column_2="mag", **_ERRORS)
    with pytest.raises(CrossMatchError, match="positional errors"):
        cm.crossmatch_multi(frames, matcher="lr", lr_magnitude_column="mag")
    with pytest.raises(CrossMatchError, match="lr_color_column"):
        cm.crossmatch_multi(
            frames,
            matcher="lr",
            lr_magnitude_column="mag",
            lr_color_column="nope",
            **_ERRORS,
        )


def test_nway_with_two_catalogues_equals_pairwise_lr():
    frames, _ = _scene(seed=5, n=300, n_bg=2400, keep=(0.7,))
    cm = CrossMatch()
    common = dict(radius_arcsec=30.0, lr_magnitude_column="mag", lr_q=0.7)
    errors = {k: v for k, v in _ERRORS.items() if not k.endswith("_3")}
    pair = cm.crossmatch(
        frames[0], frames[1], matcher="lr", find="all", engine="fast", **common, **errors
    )
    many = cm.nway_match(frames, **common, **errors)
    assert pair is not None and many is not None
    pair = pair.sort("id", "id_2")
    many = many.sort("id", "id_2")
    assert pair.select("id", "id_2").equals(many.select("id", "id_2"))
    np.testing.assert_allclose(many["p_match"], pair["reliability"], rtol=1e-6, atol=1e-9)
    np.testing.assert_allclose(many["lr"], pair["lr"], rtol=1e-6)
    np.testing.assert_allclose(many["sep_arcsec_2"], pair["sep_arcsec"], rtol=1e-9, atol=1e-9)


def _schema(frame: pl.DataFrame) -> dict[str, pl.DataType]:
    return dict(frame.schema)


def test_nway_output_schema_is_stable():
    cm = CrossMatch()
    base = {"ra": [10.0, 30.0], "dec": [0.0, 10.0], "mag": [18.0, 20.0], "err": [0.3, 0.3]}
    cat1 = pl.DataFrame({"id": [1, 2], **base})
    cat2 = pl.DataFrame({"id": [10, 20], **base, "extra": ["a", "b"]}).with_columns(
        pl.col("ra") + 0.2 / 3600
    )
    cat3 = pl.DataFrame({"id": [100, 200], **base}).with_columns(pl.col("dec") - 0.2 / 3600)
    far = cat3.with_columns(pl.col("dec") + 1.0)
    far2 = cat2.with_columns(pl.col("dec") + 1.0)
    full = cm.nway_match([cat1, cat2, cat3], radius_arcsec=2.0, **_ERRORS)
    empty = cm.nway_match([cat1, far2, far], radius_arcsec=2.0, **_ERRORS)
    partial = cm.nway_match([cat1, cat2, far], radius_arcsec=2.0, **_ERRORS)
    assert full is not None and empty is not None and partial is not None
    expected = [
        "id", "ra", "dec", "mag", "err",
        "id_2", "ra_2", "dec_2", "mag_2", "err_2", "extra",
        "id_3", "ra_3", "dec_3", "mag_3", "err_3",
        "sep_arcsec_2", "sep_arcsec_3", "p_match", "lr", "p_any", "nway_best",
    ]  # fmt: skip
    assert full.columns == expected
    assert _schema(empty) == _schema(full) == _schema(partial)
    assert empty.height == 0
    # Partial hypotheses: catalogue 3 is absent, its columns are null.
    assert partial.height == 2
    assert partial["id_3"].null_count() == 2 and partial["sep_arcsec_3"].null_count() == 2
    # Within the radius but outside every chi-square gate (5" at 0.3" errors):
    # catalogue 3 takes no part rather than failing the association.
    gated_out = cat3.with_columns(pl.col("dec") + 5.0 / 3600)
    gated = cm.nway_match([cat1, cat2, gated_out], radius_arcsec=30.0, **_ERRORS)
    assert gated is not None and _schema(gated) == _schema(full)
    assert gated.height == 2 and gated["id_3"].null_count() == 2
    assert full.filter(pl.col("nway_best")).height == 2
    best = full.filter(pl.col("nway_best")).sort("id")
    assert best["id_2"].to_list() == [10, 20] and best["id_3"].to_list() == [100, 200]
    np.testing.assert_allclose(best["sep_arcsec_2"], [0.2, 0.2 * math.cos(math.radians(10))])


def test_nway_match_cli_end_to_end(tmp_path):
    from xmatcher.cli import main

    frames, truth = _scene(seed=7, n=200, n_bg=1600)
    paths = []
    for i, frame in enumerate(frames, start=1):
        path = tmp_path / f"cat{i}.parquet"
        frame.rename({"mag": f"m{i}"}).write_parquet(path)
        paths.append(str(path))
    out = tmp_path / "nway.parquet"
    argv = ["match", *paths, "-r", "30", "--matcher", "lr", "--engine", "fast"]
    argv += ["--lr-magnitude-column", "m2,m3", "--lr-color-column", "m1", "--lr-q", "0.7"]
    for i in (1, 2, 3):
        argv += ["--side", str(i), "ra_err_column=err,dec_err_column=err"]
    argv += ["-o", str(out)]
    assert main(argv) == 0
    result = pl.read_parquet(out)
    assert {"reliability", "lr", "p_any", "nway_best", "sep_arcsec_2", "sep_arcsec_3"} <= set(
        result.columns
    )
    assert result["id"].is_unique().all()
    assert _map_accuracy(result, truth) > 0.85

    # A per-catalogue magnitude column through --side; a wrong count fails.
    argv_side = [a for a in argv if a not in ("m2,m3",)]
    argv_side.remove("--lr-magnitude-column")
    argv_side += ["--side", "2", "lr_magnitude_column=m2", "--side", "3", "lr_magnitude_column=m3"]
    assert main(argv_side) == 0
    bad = [*argv]
    bad[bad.index("m2,m3")] = "m2,m3,m4"
    assert main(bad) == 1
