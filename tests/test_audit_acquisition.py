"""Remote HATS acquisition audit: integrity, resume and resolution regressions.

Every test is offline: raw sockets or the S3 / Hub / HTTP fakes on 127.0.0.1
stand in for the remote services.
"""

from __future__ import annotations

import contextlib
import socket
import threading
from collections.abc import Iterator
from pathlib import Path

import pytest

from xmatcher import mirror
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

