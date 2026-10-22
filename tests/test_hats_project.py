from __future__ import annotations

import functools
import threading
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import polars as pl
import pytest
from polars.testing import assert_frame_equal

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


def _properties(out: Path) -> dict[str, str]:
    return dict(
        line.split("=", 1) for line in (out / "properties").read_text().splitlines() if "=" in line
    )


def test_project_hats_drops_coordinate_properties_it_no_longer_carries(
    wide_hats: Path, tmp_path: Path
) -> None:
    out = tmp_path / "narrow"
    project_hats(_source(wide_hats), out, ["id", "mag"], sync=False)
    props = _properties(out)
    assert "hats_col_ra" not in props
    assert "hats_col_dec" not in props
    assert props["hats_cols_default"] == "id mag"
    assert props["hats_nrows"] == "3"


def test_project_hats_refuses_a_non_empty_output(wide_hats: Path, tmp_path: Path) -> None:
    out = tmp_path / "narrow"
    project_hats(_source(wide_hats), out, ["id", "ra", "dec"], sync=False)
    with pytest.raises(CrossMatchError, match="already exists and is not empty"):
        project_hats(_source(wide_hats), out, ["id"], sync=False)


def _multi_partition_tree(path: Path) -> Path:
    frame = pl.DataFrame(
        {
            "id": list(range(6)),
            "ra": [10.0, 20.0, 30.0, 40.0, 50.0, 60.0],
            "dec": [0.0, 1.0, -1.0, 2.0, -2.0, 3.0],
            "mag": [12.0, 13.0, 14.0, 15.0, 16.0, 17.0],
        }
    )
    _write_hats_native(frame, path, ra_column="ra", dec_column="dec", threshold=1)
    return path


def test_project_hats_resume_reuses_finished_partitions(tmp_path: Path) -> None:
    tree = _multi_partition_tree(tmp_path / "wide")
    out = tmp_path / "narrow"
    columns = ["id", "ra", "dec"]
    project_hats(_source(tree), out, columns, sync=False)
    files = sorted((out / "dataset").glob("Norder=*/*/*.parquet"))
    assert len(files) > 1, "the fixture must produce more than one partition"
    before = {path: path.stat().st_mtime_ns for path in files}

    removed = files[0]
    removed.unlink()
    stats = project_hats(_source(tree), out, columns, sync=False, resume=True)

    after = {
        path: path.stat().st_mtime_ns
        for path in sorted((out / "dataset").glob("Norder=*/*/*.parquet"))
    }
    assert removed in after
    for path, stamp in before.items():
        if path != removed:
            assert after[path] == stamp, f"{path.name} was rewritten on resume"
    assert stats["rows"] == 6


def test_project_hats_resume_requires_the_same_projection(wide_hats: Path, tmp_path: Path) -> None:
    out = tmp_path / "narrow"
    project_hats(_source(wide_hats), out, ["id", "ra", "dec"], sync=False)
    with pytest.raises(CrossMatchError, match="does not match"):
        project_hats(_source(wide_hats), out, ["id"], sync=False, resume=True)


def test_project_hats_resume_without_state_is_an_error(tmp_path: Path) -> None:
    out = tmp_path / "occupied"
    out.mkdir()
    (out / "stray.txt").write_text("not a projection")
    tree = _multi_partition_tree(tmp_path / "wide")
    with pytest.raises(CrossMatchError, match="no resume state"):
        project_hats(_source(tree), out, ["id"], sync=False, resume=True)


def test_project_hats_resume_skips_completed_partitions(wide_hats: Path, tmp_path: Path) -> None:
    out = tmp_path / "narrow"
    first = project_hats(_source(wide_hats), out, ["id", "ra", "dec"], sync=False, resume=True)
    parts = sorted((out / "dataset").glob("Norder=*/*/*.parquet"))
    assert parts
    parts[0].unlink()
    resumed = project_hats(_source(wide_hats), out, ["id", "ra", "dec"], sync=False, resume=True)
    assert resumed["rows"] == first["rows"]
    assert (out / "project.state").is_file()
    with pytest.raises(CrossMatchError, match="resume state"):
        project_hats(_source(wide_hats), out, ["id", "mag"], sync=False, resume=True)


def test_project_hats_no_sync_names_the_missing_mirror(tmp_path: Path) -> None:
    remote = CatalogueSource(
        name="remote",
        is_local=False,
        access_method="hats",
        access_identifier="https://example.invalid/hats/survey",
        ra_column="ra",
        dec_column="dec",
        id_column="id",
    )
    with pytest.raises(CrossMatchError, match="not mirrored yet"):
        project_hats(
            remote, tmp_path / "out", ["id"], sync=False, cache_root=str(tmp_path / "cache")
        )


def test_project_hats_local_and_https_trees_agree(wide_hats: Path, tmp_path: Path) -> None:
    columns = ["id", "ra", "dec", "mag"]
    local_out = tmp_path / "local"
    local_stats = project_hats(_source(wide_hats), local_out, columns, sync=False)

    handler = functools.partial(SimpleHTTPRequestHandler, directory=str(wide_hats))
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        remote = CatalogueSource(
            name="wide",
            is_local=False,
            access_method="hats",
            access_identifier=f"http://127.0.0.1:{server.server_port}",
            ra_column="ra",
            dec_column="dec",
            id_column="id",
        )
        remote_out = tmp_path / "remote"
        remote_stats = project_hats(remote, remote_out, columns, rate_limit_rps=0.0)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    assert local_stats == remote_stats
    local_files = sorted((local_out / "dataset").glob("Norder=*/*/*.parquet"))
    remote_files = sorted((remote_out / "dataset").glob("Norder=*/*/*.parquet"))
    assert [p.relative_to(local_out) for p in local_files] == [
        p.relative_to(remote_out) for p in remote_files
    ]
    for local_file, remote_file in zip(local_files, remote_files, strict=True):
        assert_frame_equal(pl.read_parquet(local_file), pl.read_parquet(remote_file))
    assert _properties(local_out) == _properties(remote_out)
    assert_frame_equal(
        pl.read_parquet(local_out / "dataset" / "partition_info.parquet"),
        pl.read_parquet(remote_out / "dataset" / "partition_info.parquet"),
    )


def test_project_hats_uses_the_remote_column_projection(tmp_path: Path) -> None:
    """CDS serves ``?columns=``; a server that honours it must still match local.

    The reader batches at most eight columns per request, so a 20-column
    selection exercises the multi-request hstack path as well as the
    server-ignores-columns fallback covered by the other HTTP test.
    """
    import io
    import urllib.parse

    frame = pl.DataFrame(
        {f"c{i}": list(range(4)) for i in range(20)}
        | {"ra": [10.0, 20.0, 30.0, 40.0], "dec": [0.0] * 4}
    )
    tree = tmp_path / "wide"
    _write_hats_native(frame, tree, ra_column="ra", dec_column="dec")

    class ColumnHandler(SimpleHTTPRequestHandler):
        def do_GET(self) -> None:
            parsed = urllib.parse.urlparse(self.path)
            if parsed.path.endswith(".parquet") and parsed.query.startswith("columns="):
                wanted = [
                    urllib.parse.unquote(part)
                    for part in parsed.query.removeprefix("columns=").split(",")
                ]
                selected = pl.read_parquet(
                    Path(self.directory) / parsed.path.lstrip("/"), columns=wanted
                )
                buffer = io.BytesIO()
                selected.write_parquet(buffer)
                body = buffer.getvalue()
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            super().do_GET()

    handler = functools.partial(ColumnHandler, directory=str(tree))
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        remote = CatalogueSource(
            name="wide",
            is_local=False,
            access_method="hats",
            access_identifier=f"http://127.0.0.1:{server.server_port}",
            ra_column="ra",
            dec_column="dec",
        )
        columns = [*[f"c{i}" for i in range(12)], "ra", "dec"]
        out = tmp_path / "remote"
        stats = project_hats(remote, out, columns, rate_limit_rps=0.0)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    assert stats["rows"] == 4
    result = pl.read_parquet(next((out / "dataset").glob("Norder=*/*/*.parquet")))
    assert result.columns == columns
    assert result["c11"].to_list() == [0, 1, 2, 3]


def test_project_hats_keeps_zero_row_partitions(wide_hats: Path, tmp_path: Path) -> None:
    # A source tree can carry an advertised pixel with no rows; dropping it
    # would silently change the partition layout project-hats promises to keep.
    source_schema = pl.read_parquet(
        next((wide_hats / "dataset").glob("Norder=*/*/*.parquet"))
    ).schema
    empty_rel = "Norder=1/Dir=0/Npix=1.parquet"
    empty_path = wide_hats / "dataset" / empty_rel
    empty_path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(schema=source_schema).write_parquet(empty_path)

    out = tmp_path / "narrow"
    project_hats(_source(wide_hats), out, ["id", "ra", "dec"], sync=False)
    projected = out / "dataset" / empty_rel
    assert projected.is_file()
    info = pl.read_parquet(out / "dataset" / "partition_info.parquet")
    assert info.filter(pl.col("file_loc") == "Norder=1/Dir=0/Npix=1")["count"].to_list() == [0]
