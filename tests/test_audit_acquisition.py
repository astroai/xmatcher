"""Remote HATS acquisition audit: integrity, resume and resolution regressions.

Every test is offline: raw sockets or the S3 / Hub / HTTP fakes on 127.0.0.1
stand in for the remote services.
"""

from __future__ import annotations

import contextlib
import functools
import shutil
import socket
import threading
from collections.abc import Iterator
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import polars as pl
import pytest

from xmatcher import hats_native, mirror
from xmatcher.exceptions import CrossMatchError
from xmatcher.hf import HFRemote
from xmatcher.s3 import S3Remote
from xmatcher.sources import CatalogueSource
from xmatcher.storage import LocalStorage


@contextlib.contextmanager
def short_body_server(advertised: int, sent: int) -> Iterator[str]:
    """Answer every request with ``Content-Length: advertised`` but ``sent`` bytes."""
    server = socket.socket()
    server.bind(("127.0.0.1", 0))
    server.listen(8)
    stop = threading.Event()

    def serve() -> None:
        while not stop.is_set():
            try:
                conn, _ = server.accept()
            except OSError:
                return
            with conn:
                conn.recv(65536)
                conn.sendall(
                    f"HTTP/1.1 200 OK\r\nContent-Length: {advertised}\r\n"
                    "Connection: close\r\n\r\n".encode()
                    + b"x" * sent
                )

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.getsockname()[1]}"
    finally:
        stop.set()
        server.close()
        thread.join(timeout=5)


@contextlib.contextmanager
def serve_directory(directory: Path) -> Iterator[str]:
    """Serve ``directory`` over plain HTTP; yield its base URL."""
    server = ThreadingHTTPServer(
        ("127.0.0.1", 0), functools.partial(SimpleHTTPRequestHandler, directory=str(directory))
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def _frame(rows: int, offset: int = 0) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "id": [offset + i for i in range(rows)],
            "ra": [10.0 + (i % 40) * 0.001 for i in range(rows)],
            "dec": [-5.0 + (i % 30) * 0.001 for i in range(rows)],
        }
    )


def _write_csv_only_tree(tree: Path, frame: pl.DataFrame) -> None:
    """The LSDB gaia_dr3 layout: partition_info.csv only, no parquet table."""
    shutil.rmtree(tree, ignore_errors=True)
    mirror._write_hats_native(frame, tree, ra_column="ra", dec_column="dec", threshold=50)
    (tree / "dataset" / "partition_info.parquet").unlink()
    for name in ("dataset/partition_info.csv", "partition_info.csv"):
        csv = tree / name
        pl.read_csv(csv).select(["Norder", "Npix"]).write_csv(csv)


# --------------------------------------------------------------------------- #
# truncated transfers
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("kind", ["s3", "hf"])
def test_reader_rejects_a_truncated_body(tmp_path: Path, kind: str) -> None:
    """A connection that closes early must not publish a short file."""
    with short_body_server(advertised=1000, sent=100) as endpoint:
        reader = (
            S3Remote("s3://bucket/tree", endpoint=endpoint)
            if kind == "s3"
            else HFRemote("hf://datasets/owner/name", endpoint=endpoint)
        )
        dest = tmp_path / "part.parquet"
        with pytest.raises(OSError, match="100 of 1000 bytes"):
            reader.stage_in("Norder=0/Dir=0/Npix=1.parquet", dest)
        assert not dest.exists()
        assert list(tmp_path.iterdir()) == []


def test_mirror_rejects_a_download_shorter_than_the_listing(tmp_path: Path, monkeypatch) -> None:
    """A body without Content-Length that ends early is caught by the listed size."""
    listing = [
        {"rel": "properties", "size": 10},
        {"rel": "dataset/Norder=0/Dir=0/Npix=1.parquet", "size": 1000},
    ]
    monkeypatch.setattr(mirror, "_remote_hats_listing", lambda src, bucket=None: listing)

    def fake_request(url: str, bucket, **_kwargs):
        if url.endswith("properties"):
            return b"x" * 10, None
        return b"x" * 100, None  # truncated, and no Content-Length to notice it

    monkeypatch.setattr(mirror, "_http_request", fake_request)
    src = CatalogueSource(
        name="short",
        is_local=False,
        access_method="hats",
        access_identifier="https://example.test/tree",
    )
    cache = tmp_path / "cache"
    stats = mirror.SyncStats()
    mirror._mirror_remote_hats(
        src,
        LocalStorage(cache),
        bucket=mirror.TokenBucket(0),
        force=False,
        workers=1,
        progress_cb=None,
        stats=stats,
    )
    assert stats.failed == 1
    prefix = cache / "short" / mirror._version_dir(src)
    assert not (prefix / "dataset/Norder=0/Dir=0/Npix=1.parquet").exists()


# --------------------------------------------------------------------------- #
# incremental sync of size-less (partition_info.csv) listings
# --------------------------------------------------------------------------- #
def test_csv_listed_tree_resync_skips_unchanged_pixels(tmp_path: Path) -> None:
    """Pixels listed without sizes must not be re-downloaded on every sync."""
    serve = tmp_path / "serve"
    tree = serve / "cat"
    _write_csv_only_tree(tree, _frame(120))
    with serve_directory(serve) as base:
        src = CatalogueSource(
            name="csv-tree",
            is_local=False,
            access_method="hats",
            access_identifier=f"{base}/cat",
        )
        cache = str(tmp_path / "cache")
        first = mirror.sync_catalogue(src, cache_root=cache, rate_limit_rps=0.0)
        assert first.failed == 0
        pixels = len(list(tree.glob("dataset/Norder=*/Dir=*/Npix=*.parquet")))
        assert pixels > 1

        again = mirror.sync_catalogue(src, cache_root=cache, rate_limit_rps=0.0)
        assert again.failed == 0
        assert again.files_downloaded == 0
        assert again.files_skipped == first.files_downloaded

        # An upstream rebuild changes the properties; every pixel is refetched.
        _write_csv_only_tree(tree, _frame(130, offset=1000))
        rebuilt = mirror.sync_catalogue(src, cache_root=cache, rate_limit_rps=0.0)
        assert rebuilt.failed == 0
        assert rebuilt.files_downloaded >= pixels
        root, rel = mirror.locate_mirrored(src, cache_root=cache)
        rows = hats_native.load_hats_all(
            CatalogueSource(name="m", is_local=True, path=Path(root) / rel)
        )
        assert int(rows["id"].min()) == 1000


# --------------------------------------------------------------------------- #
# collection resolution
# --------------------------------------------------------------------------- #
_TREE_PROPERTIES = b"#HATS catalog\nhats_nrows=3\nhats_col_ra=ra\n"


def test_http_collection_landing_page_resolves_through_collection_properties(
    tmp_path: Path,
) -> None:
    """LSDB collection roots are landing pages with absolute links only."""
    serve = tmp_path / "serve"
    collection = serve / "hats" / "gaia_dr3"
    for tree in ("gaia", "gaia_10arcs", "gaia_5arcmin", "gaia_source_id"):
        (collection / tree).mkdir(parents=True)
        (collection / tree / "properties").write_bytes(_TREE_PROPERTIES)
    (collection / "collection.properties").write_bytes(
        b"#HATS Collection\nhats_primary_table_url=gaia\n"
        b"all_margins=gaia_10arcs gaia_5arcmin\nall_indexes=source_id gaia_source_id\n"
    )
    with serve_directory(serve) as base:
        links = "".join(
            f'<a href="{base}/hats/gaia_dr3/{name}">{name}</a>'
            for name in ("collection.properties", "gaia", "gaia_10arcs", "gaia_source_id")
        )
        (collection / "index.html").write_text(f"<html><body>{links}</body></html>")
        resolved = mirror.resolve_hats_tree_root(
            f"{base}/hats/gaia_dr3", bucket=mirror.TokenBucket(0)
        )
    assert resolved == f"{base}/hats/gaia_dr3/gaia"


@pytest.mark.parametrize("primary", ["../escape", "https://elsewhere.test/tree", "a//b"])
def test_unsafe_collection_primary_is_not_followed(primary: str) -> None:
    class Reader:
        def __init__(self, files: dict[str, bytes]) -> None:
            self.files = files

        def exists(self, rel: str) -> bool:
            return rel in self.files

        def read_bytes(self, rel: str) -> bytes:
            return self.files[rel]

        def child(self, name: str) -> Reader:
            raise AssertionError(f"followed unsafe primary {name!r}")

        def list(self, rel: str = "") -> list[str]:
            return []

    reader = Reader({"collection.properties": f"hats_primary_table_url={primary}\n".encode()})
    assert mirror._resolve_reader_tree_root("s3://bucket/hats", reader) == "s3://bucket/hats"


# --------------------------------------------------------------------------- #
# interrupted and partial syncs
# --------------------------------------------------------------------------- #
def _http_tree_source(base: str) -> CatalogueSource:
    return CatalogueSource(
        name="resumable",
        is_local=False,
        access_method="hats",
        access_identifier=f"{base}/cat",
    )


def test_interrupted_sync_resumes_from_its_checkpoint(tmp_path: Path, monkeypatch) -> None:
    """Progress made before a crash is in the manifest; the rerun fetches the rest."""
    serve = tmp_path / "serve"
    mirror._write_hats_native(
        _frame(120), serve / "cat", ra_column="ra", dec_column="dec", threshold=50
    )
    pixels = len(list((serve / "cat").glob("dataset/Norder=*/Dir=*/Npix=*.parquet")))
    assert pixels >= 2
    monkeypatch.setattr(mirror, "_MANIFEST_CHECKPOINT_S", 0.0)
    real_request = mirror._http_request
    cache = str(tmp_path / "cache")
    seen: list[str] = []
    on_disk: list[dict] = []

    def interrupt_second_pixel(url, bucket, **kwargs):
        if "/Npix=" in url and not kwargs.get("head") and not kwargs.get("range_probe"):
            seen.append(url)
            if len(seen) == 2:
                src = _http_tree_source(url.split("/cat/")[0])
                rel = f"resumable/{mirror._version_dir(src)}/{mirror._MANIFEST_NAME}"
                on_disk.append(mirror._read_json(LocalStorage(cache), rel))
                raise KeyboardInterrupt
        return real_request(url, bucket, **kwargs)

    with serve_directory(serve) as base:
        src = _http_tree_source(base)
        monkeypatch.setattr(mirror, "_http_request", interrupt_second_pixel)
        with pytest.raises(KeyboardInterrupt):
            mirror.sync_catalogue(src, cache_root=cache, rate_limit_rps=0.0, workers=1)
        # The periodic checkpoint had already recorded the first pixel.
        assert any("Npix=" in rel for rel in on_disk[0]["files"])
        assert "fetched_at" not in on_disk[0]
        monkeypatch.setattr(mirror, "_http_request", real_request)
        resumed = mirror.sync_catalogue(src, cache_root=cache, rate_limit_rps=0.0, workers=1)
    assert resumed.failed == 0
    assert resumed.files_skipped >= 1
    root, rel = mirror.locate_mirrored(src, cache_root=cache)
    rows = hats_native.load_hats_all(
        CatalogueSource(name="m", is_local=True, path=Path(root) / rel)
    )
    assert rows.height == 120


def test_locate_mirrored_refuses_a_partially_updated_tree(tmp_path: Path, monkeypatch) -> None:
    """A failed re-sync after an upstream rebuild leaves a mixed tree; do not serve it."""
    serve = tmp_path / "serve"
    tree = serve / "cat"
    mirror._write_hats_native(_frame(120), tree, ra_column="ra", dec_column="dec", threshold=50)
    cache = str(tmp_path / "cache")
    with serve_directory(serve) as base:
        src = _http_tree_source(base)
        assert mirror.sync_catalogue(src, cache_root=cache, rate_limit_rps=0.0).failed == 0
        assert mirror.locate_mirrored(src, cache_root=cache)

        shutil.rmtree(tree)
        mirror._write_hats_native(
            _frame(120, offset=1000), tree, ra_column="ra", dec_column="dec", threshold=50
        )
        real_request = mirror._http_request
        fetched: list[str] = []

        def fail_after_first_pixel(url, bucket, **kwargs):
            if "/Npix=" in url and not kwargs.get("head") and not kwargs.get("range_probe"):
                fetched.append(url)
                if len(fetched) > 1:
                    raise OSError("connection reset")
            return real_request(url, bucket, **kwargs)

        monkeypatch.setattr(mirror, "_http_request", fail_after_first_pixel)
        with pytest.raises(OSError):
            mirror.sync_catalogue(src, cache_root=cache, rate_limit_rps=0.0, workers=1)
    with pytest.raises(CrossMatchError, match="incomplete"):
        mirror.locate_mirrored(src, cache_root=cache)
