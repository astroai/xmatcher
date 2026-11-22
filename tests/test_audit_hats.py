"""HATS conformance audit: xmatcher trees against the official ``hats`` reader.

Reference trees are built the way ``hats-import`` lays them out (``hats``
writes ``properties``/``partition_info.csv``/``_metadata``), and every tree
xmatcher writes must pass ``hats.io.validation.is_valid_catalog(strict=True)``
and read back as a plain Parquet dataset.
"""

from __future__ import annotations

import logging
from pathlib import Path

import cdshealpix
import numpy as np
import polars as pl
import pyarrow.dataset as pds
import pyarrow.parquet as pq
import pytest
from astropy.coordinates import Latitude, Longitude
from scipy.spatial import cKDTree

hats = pytest.importorskip("hats")
from hats.catalog import PartitionInfo, TableProperties  # noqa: E402
from hats.io.parquet_metadata import write_parquet_metadata  # noqa: E402
from hats.io.validation import is_valid_catalog  # noqa: E402
from hats.pixel_math.healpix_pixel import HealpixPixel  # noqa: E402
from hats.pixel_math.spatial_index import compute_spatial_index  # noqa: E402

from xmatcher import CatalogueSource, MatchSpec, hats_native  # noqa: E402
from xmatcher.exceptions import CrossMatchError  # noqa: E402
from xmatcher.hats_project import project_hats  # noqa: E402
from xmatcher.ray_union import _write_hats_tree  # noqa: E402


def _pixels(ra: np.ndarray, dec: np.ndarray, order: int) -> np.ndarray:
    return cdshealpix.nested.lonlat_to_healpix(
        Longitude(ra, unit="deg"),
        Latitude(dec, unit="deg"),
        np.full(len(ra), order, dtype=np.uint8),
    ).astype(np.int64)


def _properties(root: Path, rows: int, **extra: object) -> None:
    fields = {
        "catalog_name": root.name,
        "catalog_type": "object",
        "total_rows": rows,
        "ra_column": "ra",
        "dec_column": "dec",
        "healpix_column": "_healpix_29",
        "healpix_order": 29,
        **extra,
    }
    TableProperties.model_validate(fields).to_properties_file(root)


def _reference_tree(root: Path, frame: pl.DataFrame, orders: dict[int, int] | int) -> Path:
    """Write ``frame`` as a hats-import style tree.

    ``orders`` maps each order-0 base cell to the order its rows are tiled at
    (an int tiles everything at one order), which gives mixed-order trees.
    """
    ra = frame["ra"].to_numpy()
    dec = frame["dec"].to_numpy()
    base = _pixels(ra, dec, 0)
    order = np.array(
        [orders if isinstance(orders, int) else orders[int(b)] for b in base], dtype=np.int64
    )
    pix = np.empty(len(ra), dtype=np.int64)
    for k in np.unique(order):
        sel = order == k
        pix[sel] = _pixels(ra[sel], dec[sel], int(k))
    tiled = frame.with_columns(
        pl.Series("_healpix_29", compute_spatial_index(ra, dec)).cast(pl.Int64),
        pl.Series("_o", order),
        pl.Series("_p", pix),
    ).sort("_healpix_29")
    pixels = []
    for (k, p), sub in tiled.group_by(["_o", "_p"]):
        part = root / "dataset" / f"Norder={k}" / f"Dir={(p // 10_000) * 10_000}"
        part.mkdir(parents=True, exist_ok=True)
        pq.write_table(sub.drop("_o", "_p").to_arrow(), part / f"Npix={p}.parquet")
        pixels.append(HealpixPixel(int(k), int(p)))
    PartitionInfo.from_healpix(pixels).write_to_file(catalog_path=root)
    write_parquet_metadata(root)
    _properties(root, frame.height)
    return root


def _hats_source(name: str, root: Path) -> CatalogueSource:
    return CatalogueSource(
        name=name, is_local=False, access_method="hats", access_identifier=str(root), path=root
    )


def _assert_conformant(root: Path, rows: int, caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING):
        assert is_valid_catalog(root, strict=True), caplog.text
    catalog = hats.read_hats(root)
    assert catalog.catalog_info.total_rows == rows
    assert pds.parquet_dataset(str(root / "dataset" / "_metadata")).count_rows() == rows
    assert pds.dataset(str(root / "dataset"), format="parquet").count_rows() == rows


def _sky(n: int, seed: int) -> pl.DataFrame:
    rng = np.random.default_rng(seed)
    return pl.DataFrame(
        {
            "id": np.arange(n),
            "ra": rng.uniform(0.0, 360.0, n),
            "dec": np.degrees(np.arcsin(rng.uniform(-1.0, 1.0, n))),
            "mag": rng.normal(20.0, 1.0, n),
        }
    )


# --------------------------------------------------------------------------- #
# writing
# --------------------------------------------------------------------------- #
def test_project_hats_output_is_a_strictly_valid_hats_catalogue(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    source = _reference_tree(tmp_path / "src", _sky(500, 1), {b: 1 + b % 2 for b in range(12)})
    out = tmp_path / "narrow"
    stats = project_hats(_hats_source("src", source), out, ["id", "ra", "dec"], sync=False)

    assert stats["rows"] == 500
    _assert_conformant(out, 500, caplog)
    assert sorted(hats.read_hats(out).get_healpix_pixels()) == sorted(
        hats.read_hats(source).get_healpix_pixels()
    )
    assert sorted(p.name for p in (out / "dataset").iterdir() if not p.is_dir()) == [
        "_common_metadata",
        "_metadata",
    ]


def test_project_hats_of_npix_directory_tree_keeps_hats_paths_valid(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """``hats_npix_suffix=/`` sources (Npix=P/ dirs) project to Npix=P.parquet files."""
    frame = _sky(60, 2).filter(pl.col("dec") > 0)
    ra = frame["ra"].to_numpy()
    dec = frame["dec"].to_numpy()
    frame = frame.with_columns(
        pl.Series("_healpix_29", compute_spatial_index(ra, dec)).cast(pl.Int64),
        pl.Series("_p", _pixels(ra, dec, 0)),
    )
    root = tmp_path / "dirs"
    pixels = []
    for (p,), sub in frame.group_by(["_p"]):
        part = root / "dataset" / "Norder=0" / "Dir=0" / f"Npix={p}"
        part.mkdir(parents=True)
        sub = sub.drop("_p")
        half = sub.height // 2
        pq.write_table(sub.head(half).to_arrow(), part / "part0.parquet")
        pq.write_table(sub.tail(sub.height - half).to_arrow(), part / "part1.parquet")
        pixels.append(HealpixPixel(0, int(p)))
    PartitionInfo.from_healpix(pixels).write_to_file(catalog_path=root)
    write_parquet_metadata(root)
    _properties(root, frame.height, npix_suffix="/")

    out = tmp_path / "narrow"
    project_hats(_hats_source("dirs", root), out, ["id", "ra", "dec"], sync=False)

    _assert_conformant(out, frame.height, caplog)
    info = hats.read_hats(out).catalog_info
    assert info.npix_suffix == ".parquet"
    # The projection dropped _healpix_29, so it must not advertise it.
    assert info.healpix_column is None
    assert info.healpix_order is None


def test_write_hats_tree_metadata_lists_every_partition(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    out = tmp_path / "tree"
    info = []
    for pix, rows in ((0, 3), (1, 0), (44, 2)):
        rel = f"Norder=1/Dir=0/Npix={pix}"
        path = out / "dataset" / f"{rel}.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame(
            {"ra": [1.0] * rows, "dec": [2.0] * rows}, schema={"ra": pl.Float64, "dec": pl.Float64}
        ).write_parquet(path)
        info.append(
            {
                "Norder": 1,
                "Dir": 0,
                "Npix": pix,
                "Nfiles": 1,
                "file_loc": rel,
                "file_size": path.stat().st_size,
                "count": rows,
            }
        )
    props = {"obs_collection": "t", "hats_col_ra": "ra", "hats_col_dec": "dec"}
    assert _write_hats_tree(out, info, props) == {"rows": 5, "partitions": 3}
    _assert_conformant(out, 5, caplog)


def test_write_hats_tree_rejects_partitions_with_different_schemas(tmp_path: Path) -> None:
    out = tmp_path / "tree"
    info = []
    for pix, dtype in ((0, pl.Float64), (1, pl.Float32)):
        rel = f"Norder=1/Dir=0/Npix={pix}"
        path = out / "dataset" / f"{rel}.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({"ra": [1.0], "dec": [2.0]}, schema={"ra": dtype, "dec": dtype}).write_parquet(
            path
        )
        info.append(
            {
                "Norder": 1,
                "Dir": 0,
                "Npix": pix,
                "Nfiles": 1,
                "file_loc": rel,
                "file_size": 1,
                "count": 1,
            }
        )
    with pytest.raises(CrossMatchError, match="different schema"):
        _write_hats_tree(out, info, {"obs_collection": "t"})


# --------------------------------------------------------------------------- #
# margins: hats_native_crossmatch against brute force
# --------------------------------------------------------------------------- #
def _brute_pairs(left: pl.DataFrame, right: pl.DataFrame, radius_arcsec: float) -> set:
    def xyz(frame: pl.DataFrame) -> np.ndarray:
        ra = np.radians(frame["ra"].to_numpy())
        dec = np.radians(frame["dec"].to_numpy())
        return np.c_[np.cos(dec) * np.cos(ra), np.cos(dec) * np.sin(ra), np.sin(dec)]

    chord = 2.0 * np.sin(np.radians(radius_arcsec / 3600.0) / 2.0)
    tree = cKDTree(xyz(right))
    lid = left["id"].to_list()
    rid = right["id"].to_list()
    return {
        (lid[i], rid[j])
        for i, hits in enumerate(tree.query_ball_point(xyz(left), chord))
        for j in hits
    }


def _boundary_pairs(n: int, radius_arcsec: float, seed: int) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Pairs within ``radius`` everywhere, plus RA=0/360 and both poles."""
    rng = np.random.default_rng(seed)
    ra = rng.uniform(0.0, 360.0, n)
    dec = np.degrees(np.arcsin(rng.uniform(-1.0, 1.0, n)))
    ra[:100] = rng.uniform(-0.01, 0.01, 100) % 360.0
    dec[100:200] = rng.uniform(89.99, 90.0, 100)
    dec[200:300] = rng.uniform(-90.0, -89.99, 100)
    dec[300], dec[301], ra[302] = 90.0, -90.0, 0.0
    angle = rng.uniform(0.0, 2.0 * np.pi, n)
    sep = rng.uniform(0.0, 0.95 * radius_arcsec / 3600.0, n)
    dec2 = np.clip(dec + sep * np.sin(angle), -90.0, 90.0)
    ra2 = (ra + sep * np.cos(angle) / np.maximum(np.cos(np.radians(dec)), 1e-3)) % 360.0
    left = pl.DataFrame({"id": np.arange(n), "ra": ra, "dec": dec})
    right = pl.DataFrame({"id": np.arange(n) + 1_000_000, "ra": ra2, "dec": dec2})
    return left, right


def _pairs(out: pl.DataFrame) -> list[tuple[int, int]]:
    return list(zip(out["id"].to_list(), out["id_2"].to_list(), strict=True))


@pytest.mark.parametrize(
    ("left_orders", "right_orders"),
    [
        (4, 4),
        ({b: 3 + b % 3 for b in range(12)}, {b: 4 - b % 2 for b in range(12)}),
    ],
    ids=["same-order", "mixed-order"],
)
def test_hats_native_matches_brute_force_across_boundaries(
    tmp_path: Path, left_orders, right_orders
) -> None:
    radius = 30.0
    left, right = _boundary_pairs(6000, radius, seed=3)
    lroot = _reference_tree(tmp_path / "left", left, left_orders)
    rroot = _reference_tree(tmp_path / "right", right, right_orders)
    spec = MatchSpec(radius_arcsec=radius, join_type="1and2", find="all")

    got = _pairs(
        hats_native.hats_native_crossmatch(_hats_source("L", lroot), _hats_source("R", rroot), spec)
    )

    assert len(got) == len(set(got)), "duplicate pairs"
    assert set(got) == _brute_pairs(left, right, radius)


def test_hats_native_left_mixed_orders_against_local_right(tmp_path: Path) -> None:
    radius = 30.0
    left, right = _boundary_pairs(6000, radius, seed=4)
    lroot = _reference_tree(tmp_path / "left", left, {b: 3 + b % 3 for b in range(12)})
    rsrc = CatalogueSource(name="R", is_local=True, ra_column="ra", dec_column="dec")
    spec = MatchSpec(radius_arcsec=radius, join_type="1and2", find="all")

    got = _pairs(
        hats_native.hats_native_crossmatch(
            _hats_source("L", lroot), rsrc, spec, local_lf2=right.lazy()
        )
    )

    assert len(got) == len(set(got))
    assert set(got) == _brute_pairs(left, right, radius)


@pytest.mark.parametrize("join_type", ["all1", "1not2"])
def test_hats_native_left_join_keeps_rows_far_from_a_local_right(
    tmp_path: Path, join_type: str
) -> None:
    left = _sky(2000, 5)
    lroot = _reference_tree(tmp_path / "left", left, 3)
    # One right source: most left pixels are nowhere near it.
    right = pl.DataFrame({"id": [9_999_999], "ra": [left["ra"][0]], "dec": [left["dec"][0]]})
    rsrc = CatalogueSource(name="R", is_local=True, ra_column="ra", dec_column="dec")
    spec = MatchSpec(radius_arcsec=1.0, join_type=join_type)

    out = hats_native.hats_native_crossmatch(
        _hats_source("L", lroot), rsrc, spec, local_lf2=right.lazy()
    )

    expected = left.height if join_type == "all1" else left.height - 1
    assert out["id"].n_unique() == expected


def test_write_hats_output_is_a_strictly_valid_hats_catalogue(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """`.hats` output and TAP mirrors (``mirror._write_hats_native``) use the shared writer."""
    from xmatcher.io_utils import write_hats

    frame = _sky(5000, seed=11)
    out = tmp_path / "out.hats"
    write_hats(frame, out, threshold=800)
    _assert_conformant(out, frame.height, caplog)
    assert not (out / "dataset" / "properties").exists()
    assert not (out / "dataset" / "partition_info.csv").exists()


def test_write_hats_of_an_empty_frame_keeps_its_schema(tmp_path: Path) -> None:
    from xmatcher.io_utils import write_hats

    frame = _sky(10, seed=3).head(0)
    out = tmp_path / "empty.hats"
    write_hats(frame, out)
    assert pq.read_schema(out / "dataset" / "_common_metadata").names == frame.columns
    assert "hats_nrows=0" in (out / "properties").read_text()


def _local_collection(tmp_path: Path, primary: str) -> Path:
    """An LSDB-style download: collection.properties, the tree, a margin cache."""
    from xmatcher.io_utils import write_hats

    collection = tmp_path / "gaia_dr3"
    write_hats(_sky(200, seed=5), collection / "gaia")
    write_hats(_sky(20, seed=6), collection / "gaia_10arcs")
    (collection / "collection.properties").write_text(
        f"obs_collection=gaia_dr3\nhats_primary_table_url={primary}\nall_margins=gaia_10arcs\n"
    )
    return collection


@pytest.mark.parametrize("spelling", ["{p}", "hats:{p}"])
def test_local_collection_resolves_to_its_primary_tree(tmp_path: Path, spelling: str) -> None:
    from xmatcher import CrossMatch

    collection = _local_collection(tmp_path, "gaia")
    source = CrossMatch().resolve_source(spelling.format(p=collection), {})
    assert Path(source.path) == collection / "gaia"
    assert hats_native.load_hats_all(source).height == 200


@pytest.mark.parametrize("primary", ["../elsewhere", "missing"])
def test_local_collection_without_a_readable_primary_is_refused(
    tmp_path: Path, primary: str
) -> None:
    from xmatcher import CrossMatch
    from xmatcher.exceptions import InputError

    collection = _local_collection(tmp_path, primary)
    with pytest.raises(InputError, match="does not name a readable primary"):
        CrossMatch().resolve_source(str(collection), {})
