"""FITS / Parquet / HATS / CSV conversion in both directions (xmatcher.convert)."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from xmatcher.convert import HEALPIX29, convert, detect_format
from xmatcher.exceptions import InputError


def _catalogue(n: int = 6000, seed: int = 3) -> pa.Table:
    rng = np.random.default_rng(seed)
    ra = rng.uniform(0.0, 360.0, n)
    dec = np.degrees(np.arcsin(rng.uniform(-1.0, 1.0, n)))
    # Poles, the RA wrap and a duplicate position.
    ra[:6] = [0.0, 180.0, 0.0, 359.9999999, 0.0, 0.0]
    dec[:6] = [90.0, -90.0, 89.99999, 0.0, 0.0, 0.0]
    return pa.table(
        {
            "id": pa.array(np.arange(n, dtype=np.int64)),
            "ra": ra,
            "dec": dec,
            "mag": pa.array(rng.normal(18.0, 1.0, n).astype(np.float32)),
            "flag": pa.array(rng.random(n) < 0.5),
            "name": pa.array([f"src{i}" for i in range(n)]),
            "u16": pa.array(rng.integers(0, 65535, n).astype(np.uint16)),
            "i8": pa.array(rng.integers(-128, 127, n).astype(np.int8)),
            "u64": pa.array(rng.integers(0, 2**62, n).astype(np.uint64) + np.uint64(2**63)),
            "vec": pa.FixedSizeListArray.from_arrays(pa.array(rng.normal(size=3 * n)), 3),
            "nint": pa.array([None if i % 7 == 0 else i for i in range(n)], pa.int32()),
        }
    )


def _same_rows(expected: pa.Table, got: pa.Table) -> None:
    """Equal as sets of rows (HATS reorders spatially), keyed by ``id``."""
    exp = expected.sort_by("id")
    got = got.select(exp.column_names).sort_by("id")
    assert got.num_rows == exp.num_rows
    for name in exp.column_names:
        assert got.column(name).cast(exp.schema.field(name).type).equals(exp.column(name)), name


def _read_fits(path: Path) -> pa.Table:
    from torchfits import table as fits_table

    return fits_table.read_arrow(str(path))


def test_detect_format(tmp_path: Path) -> None:
    assert detect_format("a.fits") == "fits"
    assert detect_format("a.fits.gz") == "fits"
    assert detect_format("a.parquet") == "parquet"
    assert detect_format("a.csv") == "csv"
    assert detect_format("out.hats", output=True) == "hats"
    pq.write_table(_catalogue(10), tmp_path / "part.parquet")
    assert detect_format(tmp_path) == "parquet"
    with pytest.raises(InputError):
        detect_format("a.txt", output=True)


def test_parquet_fits_round_trip_keeps_types_and_nulls(tmp_path: Path) -> None:
    t = _catalogue()
    pq.write_table(t, tmp_path / "a.parquet")
    report = convert(tmp_path / "a.parquet", tmp_path / "a.fits", batch_rows=700)
    assert (report.rows, report.dst_format) == (t.num_rows, "fits")
    _same_rows(t, _read_fits(tmp_path / "a.fits"))
    # FITS -> Parquet gives the original table back.
    convert(tmp_path / "a.fits", tmp_path / "b.parquet")
    _same_rows(t, pq.read_table(tmp_path / "b.parquet"))
    # The binary table is standard: astropy reads it too (an independent reader).
    from astropy.table import Table

    at = Table.read(tmp_path / "a.fits")
    assert len(at) == t.num_rows
    assert at["u64"].dtype == np.uint64
    assert np.array_equal(np.asarray(at["u16"]), t.column("u16").to_numpy())
    assert np.array_equal(
        np.asarray(at["vec"]), t.column("vec").to_numpy(zero_copy_only=False).tolist()
    )


def test_fits_float_nulls_become_nan(tmp_path: Path) -> None:
    t = pa.table({"id": [0, 1, 2], "x": pa.array([1.5, None, 3.0], pa.float64())})
    pq.write_table(t, tmp_path / "n.parquet")
    convert(tmp_path / "n.parquet", tmp_path / "n.fits")
    x = _read_fits(tmp_path / "n.fits").column("x").to_numpy(zero_copy_only=False)
    assert x[0] == 1.5 and np.isnan(x[1]) and x[2] == 3.0


@pytest.mark.parametrize("src_fmt", ["parquet", "fits"])
def test_hats_round_trip_is_valid_and_lossless(tmp_path: Path, src_fmt: str) -> None:
    t = _catalogue()
    pq.write_table(t, tmp_path / "a.parquet")
    src = tmp_path / "a.parquet"
    if src_fmt == "fits":
        convert(src, tmp_path / "a.fits")
        src = tmp_path / "a.fits"
    report = convert(src, tmp_path / "a.hats", hats_max_rows=400, batch_rows=900, spill_rows=2000)
    assert report.rows == t.num_rows and report.partitions > 12
    hats = pytest.importorskip("hats")
    from hats.io.validation import is_valid_catalog

    assert is_valid_catalog(tmp_path / "a.hats", strict=True)
    cat = hats.read_hats(tmp_path / "a.hats")
    assert cat.catalog_info.total_rows == t.num_rows
    assert (cat.catalog_info.ra_column, cat.catalog_info.dec_column) == ("ra", "dec")
    # Back to Parquet and to FITS: the same rows, without the spatial index.
    convert(tmp_path / "a.hats", tmp_path / "back.parquet")
    back = pq.read_table(tmp_path / "back.parquet")
    assert HEALPIX29 not in back.column_names
    _same_rows(t, back)
    convert(tmp_path / "a.hats", tmp_path / "back.fits")
    _same_rows(t, _read_fits(tmp_path / "back.fits"))


def test_hats_partitions_are_sorted_and_indexed(tmp_path: Path) -> None:
    import torch
    from torchsky.catalogs import healpix29

    t = _catalogue(3000)
    pq.write_table(t, tmp_path / "a.parquet")
    convert(tmp_path / "a.parquet", tmp_path / "a.hats", hats_max_rows=300)
    for f in sorted((tmp_path / "a.hats" / "dataset").glob("Norder=*/Dir=*/Npix=*.parquet")):
        part = pq.read_table(f)
        h = part.column(HEALPIX29).to_numpy()
        assert np.all(np.diff(h) >= 0)
        ref = healpix29(
            torch.from_numpy(part.column("ra").to_numpy()),
            torch.from_numpy(part.column("dec").to_numpy()),
        ).numpy()
        assert np.array_equal(h, ref)
        order = int(f.parent.parent.name.split("=")[1])
        pix = int(f.stem.split("=")[1])
        assert np.all(h >> (2 * (29 - order)) == pix)
        assert part.num_rows <= 300 or order == 10


def test_hats_layout_does_not_depend_on_batching(tmp_path: Path) -> None:
    t = _catalogue(4000)
    pq.write_table(t, tmp_path / "a.parquet")
    convert(tmp_path / "a.parquet", tmp_path / "big.hats", hats_max_rows=500)
    convert(
        tmp_path / "a.parquet",
        tmp_path / "small.hats",
        hats_max_rows=500,
        batch_rows=97,
        spill_rows=300,
    )
    big = sorted(
        p.relative_to(tmp_path / "big.hats")
        for p in (tmp_path / "big.hats").rglob("Npix=*.parquet")
    )
    small = sorted(
        p.relative_to(tmp_path / "small.hats")
        for p in (tmp_path / "small.hats").rglob("Npix=*.parquet")
    )
    assert big == small
    for rel in big:
        assert pq.read_table(tmp_path / "big.hats" / rel).equals(
            pq.read_table(tmp_path / "small.hats" / rel)
        )


def test_hats_input_options(tmp_path: Path) -> None:
    t = _catalogue(1000)
    pq.write_table(t, tmp_path / "a.parquet")
    convert(tmp_path / "a.parquet", tmp_path / "a.hats")
    convert(tmp_path / "a.hats", tmp_path / "idx.parquet", keep_healpix29=True)
    assert HEALPIX29 in pq.read_schema(tmp_path / "idx.parquet").names
    convert(tmp_path / "a.hats", tmp_path / "few.csv", columns=["id", "ra", "dec"])
    import pyarrow.csv as pacsv

    few = pacsv.read_csv(tmp_path / "few.csv")
    assert few.column_names == ["id", "ra", "dec"] and few.num_rows == 1000
    # A HATS tree re-partitioned from a HATS tree keeps one index column.
    convert(tmp_path / "a.hats", tmp_path / "b.hats", keep_healpix29=True, hats_max_rows=200)
    assert (
        pq.read_schema(next((tmp_path / "b.hats").rglob("Npix=*.parquet"))).names.count(HEALPIX29)
        == 1
    )


def test_hats_needs_valid_positions_and_columns(tmp_path: Path) -> None:
    t = pa.table({"id": [0, 1], "ra": [10.0, float("nan")], "dec": [0.0, 1.0]})
    pq.write_table(t, tmp_path / "bad.parquet")
    with pytest.raises(InputError, match="invalid coordinates"):
        convert(tmp_path / "bad.parquet", tmp_path / "bad.hats")
    assert not (tmp_path / "bad.hats").exists()
    pq.write_table(pa.table({"x": [1.0], "y": [2.0]}), tmp_path / "nopos.parquet")
    with pytest.raises(InputError, match="RA column"):
        convert(tmp_path / "nopos.parquet", tmp_path / "nopos.hats")
    convert(tmp_path / "nopos.parquet", tmp_path / "pos.hats", ra_column="x", dec_column="y")
    assert (tmp_path / "pos.hats" / "properties").read_text().count("hats_col_ra=x") == 1


def test_overwrite_rules(tmp_path: Path) -> None:
    pq.write_table(_catalogue(10), tmp_path / "a.parquet")
    convert(tmp_path / "a.parquet", tmp_path / "a.fits")
    with pytest.raises(InputError, match="exists"):
        convert(tmp_path / "a.parquet", tmp_path / "a.fits")
    convert(tmp_path / "a.parquet", tmp_path / "a.fits", overwrite=True)
    with pytest.raises(InputError, match="does not exist"):
        convert(tmp_path / "missing.parquet", tmp_path / "x.fits")


def test_cli_convert(tmp_path: Path) -> None:
    from xmatcher.cli import main

    pq.write_table(_catalogue(500), tmp_path / "a.parquet")
    assert (
        main(
            [
                "convert",
                str(tmp_path / "a.parquet"),
                str(tmp_path / "a.hats"),
                "--hats-max-rows",
                "100",
            ]
        )
        == 0
    )
    assert main(["convert", str(tmp_path / "a.hats"), str(tmp_path / "a.fits")]) == 0
    assert main(["convert", str(tmp_path / "a.hats"), str(tmp_path / "a.fits")]) == 1  # exists
    _same_rows(pq.read_table(tmp_path / "a.parquet"), _read_fits(tmp_path / "a.fits"))
