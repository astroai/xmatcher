from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from xmatcher.cli import main
from xmatcher.exceptions import CrossMatchError, InputError
from xmatcher.hats_export import export_hats, scan_hats
from xmatcher.io_utils import scan_frame
from xmatcher.mirror import _write_hats_native
from xmatcher.sources import CatalogueSource


def _source(path: Path) -> CatalogueSource:
    return CatalogueSource(
        name="tiny",
        is_local=True,
        path=path,
        access_method="hats",
        access_identifier=str(path),
        ra_column="ra",
        dec_column="dec",
        id_column="id",
    )


@pytest.fixture()
def tiny_hats(tmp_path: Path) -> Path:
    frame = pl.DataFrame(
        {
            "id": [1, 2, 3],
            "ra": [10.0, 20.0, 30.0],
            "dec": [0.0, 1.0, -1.0],
            "mag": [12.0, 13.0, 14.0],
        }
    )
    out = tmp_path / "tiny"
    _write_hats_native(frame, out, ra_column="ra", dec_column="dec")
    return out


def test_export_hats_to_parquet_round_trip(tiny_hats: Path, tmp_path: Path) -> None:
    out = tmp_path / "flat.parquet"
    stats = export_hats(_source(tiny_hats), out)
    assert stats["rows"] == 3
    assert stats["partitions"] >= 1
    assert set(stats["columns"]) >= {"id", "ra", "dec", "mag"}
    back = pl.read_parquet(out).sort("id")
    assert back.height == 3
    assert back["id"].to_list() == [1, 2, 3]
    assert back["mag"].to_list() == [12.0, 13.0, 14.0]


def test_export_hats_to_fits_round_trip(tiny_hats: Path, tmp_path: Path) -> None:
    out = tmp_path / "flat.fits"
    stats = export_hats(_source(tiny_hats), out)
    assert stats["rows"] == 3
    back = scan_frame(out).collect().sort("id")
    assert back.height == 3
    assert back["id"].to_list() == [1, 2, 3]
    assert set(back.columns) >= {"id", "ra", "dec", "mag"}


def test_export_hats_column_projection(tiny_hats: Path, tmp_path: Path) -> None:
    out = tmp_path / "narrow.parquet"
    stats = export_hats(_source(tiny_hats), out, columns=["id", "ra", "dec"])
    assert stats["columns"] == ["id", "ra", "dec"]
    back = pl.read_parquet(out)
    assert back.columns == ["id", "ra", "dec"]
    assert back.height == 3


def test_export_hats_rejects_missing_column(tiny_hats: Path, tmp_path: Path) -> None:
    with pytest.raises(CrossMatchError, match="not in the HATS schema"):
        export_hats(_source(tiny_hats), tmp_path / "x.parquet", columns=["id", "nope"])


def test_export_hats_rejects_bad_suffix(tiny_hats: Path, tmp_path: Path) -> None:
    with pytest.raises(InputError, match=r"\.parquet or \.fits"):
        export_hats(_source(tiny_hats), tmp_path / "x.csv")


def test_scan_hats_lazy_row_count(tiny_hats: Path) -> None:
    lf = scan_hats(tiny_hats)
    assert int(lf.select(pl.len()).collect().item()) == 3


def test_cli_export_hats(tiny_hats: Path, tmp_path: Path, capsys) -> None:
    out = tmp_path / "cli.parquet"
    assert main(["export-hats", str(tiny_hats), str(out), "--columns", "id,ra,dec"]) == 0
    captured = capsys.readouterr().out
    assert "3 rows" in captured
    back = pl.read_parquet(out).sort("id")
    assert back.columns == ["id", "ra", "dec"]
    assert back.height == 3
