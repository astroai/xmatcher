from __future__ import annotations

import functools
import threading
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import polars as pl
import pytest

from xmatcher.exceptions import CrossMatchError
from xmatcher.hats_project import project_hats
from xmatcher.mirror import _write_hats_native
from xmatcher.sources import CatalogueSource


def _source(path: Path) -> CatalogueSource:
    return CatalogueSource(
        name="wide",
        is_local=True,
        path=path,
        access_method="hats",
        ra_column="ra",
        dec_column="dec",
        id_column="id",
    )


@pytest.fixture()
def wide_hats(tmp_path: Path) -> Path:
    frame = pl.DataFrame(
        {
            "id": [1, 2, 3],
            "ra": [10.0, 20.0, 30.0],
            "dec": [0.0, 1.0, -1.0],
            "mag": [12.0, 13.0, 14.0],
            "extra": ["a", "b", "c"],
        }
    )
    _write_hats_native(frame, tmp_path / "wide", ra_column="ra", dec_column="dec")
    return tmp_path / "wide"


def test_project_hats_keeps_rows_and_selected_columns(wide_hats: Path, tmp_path: Path) -> None:
    out = tmp_path / "narrow"
    stats = project_hats(_source(wide_hats), out, ["id", "ra", "dec"], sync=False)
    assert stats["rows"] == 3
    result = pl.read_parquet(next((out / "dataset").glob("Norder=*/*/*.parquet"))).sort("id")
    assert result.columns == ["id", "ra", "dec"]
    assert result["id"].to_list() == [1, 2, 3]
    assert "hats_nrows=3" in (out / "properties").read_text()
    assert (out / "partition_info.csv").is_file()
    assert (out / "dataset" / "partition_info.parquet").is_file()


def test_project_hats_rejects_missing_column(wide_hats: Path, tmp_path: Path) -> None:
    with pytest.raises(CrossMatchError, match="not in the HATS schema"):
        project_hats(_source(wide_hats), tmp_path / "narrow", ["id", "nope"], sync=False)


def test_project_hats_rejects_output_overlap(wide_hats: Path) -> None:
    with pytest.raises(CrossMatchError, match="must not overlap"):
        project_hats(_source(wide_hats), wide_hats, ["id", "ra", "dec"], sync=False)


def test_project_hats_reads_https_tree_without_full_mirror(wide_hats: Path, tmp_path: Path) -> None:
    handler = functools.partial(SimpleHTTPRequestHandler, directory=str(wide_hats))
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        remote = CatalogueSource(
            name="remote",
            is_local=False,
            access_method="hats",
            access_identifier=f"http://127.0.0.1:{server.server_port}",
            ra_column="ra",
            dec_column="dec",
            id_column="id",
        )
        out = tmp_path / "remote_narrow"
        stats = project_hats(remote, out, ["id", "ra", "dec"], rate_limit_rps=0.0)
        assert stats["rows"] == 3
        result = pl.read_parquet(next((out / "dataset").glob("Norder=*/*/*.parquet")))
        assert result.columns == ["id", "ra", "dec"]
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
