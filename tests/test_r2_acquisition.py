"""Round-2 acquisition fixes (remote HATS sync, storage)."""

from __future__ import annotations

import email.utils
import io
import urllib.error
import urllib.request

import pytest

from xmatcher import http_retry, mirror
from xmatcher.hf import HFRemote
from xmatcher.s3 import S3Remote


# --------------------------------------------------------------------------- #
# Retry-After
# --------------------------------------------------------------------------- #
def test_retry_after_accepts_seconds_and_http_dates() -> None:
    now = 1_800_000_000.0
    date = email.utils.formatdate(now + 42, usegmt=True)
    assert http_retry.retry_after_seconds("7") == 7.0
    assert http_retry.retry_after_seconds(date, now=now) == pytest.approx(42.0)
    past = email.utils.formatdate(now - 600, usegmt=True)
    assert http_retry.retry_after_seconds(past, now=now) == 0.0
    assert http_retry.retry_after_seconds(None) == http_retry.DEFAULT_DELAY_S
    assert http_retry.retry_after_seconds("soon") == http_retry.DEFAULT_DELAY_S


class _Response(io.BytesIO):
    headers: dict[str, str] = {}


def _throttle_once(monkeypatch, retry_after: str) -> list[float]:
    """Make the first urlopen answer 429 with ``retry_after``; record sleeps."""
    calls: list[str] = []
    sleeps: list[float] = []

    def flaky(request, timeout=None):
        calls.append(request.full_url)
        if len(calls) == 1:
            raise urllib.error.HTTPError(
                request.full_url,
                429,
                "slow down",
                {"Retry-After": retry_after},  # type: ignore[arg-type]
                io.BytesIO(),
            )
        return _Response(b"ok")

    monkeypatch.setattr(urllib.request, "urlopen", flaky)
    monkeypatch.setattr(http_retry.time, "sleep", sleeps.append)
    return sleeps


@pytest.mark.parametrize("reader", ["http", "s3", "hf"])
def test_http_date_retry_after_is_honoured(monkeypatch, reader: str) -> None:
    """A CDN's ``Retry-After: <HTTP-date>`` used to fall back to a 1 s wait."""
    date = email.utils.formatdate(http_retry.time.time() + 30, usegmt=True)
    sleeps = _throttle_once(monkeypatch, date)
    if reader == "http":
        body, _ = mirror._http_request("https://example.test/properties", mirror.TokenBucket(0))
        assert body == b"ok"
    elif reader == "s3":
        S3Remote("s3://bucket/cat")._open("https://bucket.s3.amazonaws.com/cat/x").close()
    else:
        HFRemote("hf://datasets/o/n")._open("https://huggingface.co/x").close()
    assert len(sleeps) == 1
    assert 25.0 < sleeps[0] <= 31.0


# --------------------------------------------------------------------------- #
# hf:// revisions
# --------------------------------------------------------------------------- #
def test_hf_uri_revision_is_parsed_not_part_of_the_repo() -> None:
    """``name@rev`` used to become repo ``owner/name@rev`` and 404 on the Hub."""
    from xmatcher.hf import split_hf_uri

    assert split_hf_uri("hf://datasets/owner/name@v1.0/hats") == ("owner/name", "hats", "v1.0")
    assert split_hf_uri("hf://datasets/owner/name@refs%2Fpr%2F3") == (
        "owner/name",
        "",
        "refs/pr/3",
    )
    remote = HFRemote("hf://datasets/owner/name@v1.0/hats", endpoint="https://hub.test")
    assert remote.repo == "owner/name"
    assert remote.revision == "v1.0"
    assert remote.resolve_url("properties") == (
        "https://hub.test/datasets/owner/name/resolve/v1.0/hats/properties"
    )
    assert "/tree/v1.0/hats?" in remote._tree_url(recursive=True)
    child = remote.child("cat")
    assert child.revision == "v1.0"
    assert child.resolve_url("properties").endswith("/resolve/v1.0/hats/cat/properties")
    assert remote.display(remote.resolve_url("x")) == "hf://datasets/owner/name@v1.0/hats/x"
    # An explicit revision argument still wins over the URI.
    assert HFRemote("hf://datasets/owner/name@v1.0", revision="dev").revision == "dev"


# --------------------------------------------------------------------------- #
# storage
# --------------------------------------------------------------------------- #
def test_headroom_on_a_vos_root_neither_creates_a_local_dir_nor_checks_local_disk(
    tmp_path, monkeypatch
) -> None:
    """``vos:`` used to be makedirs'd in the cwd and checked against local disk."""
    from xmatcher import storage

    monkeypatch.chdir(tmp_path)

    def no_statvfs(path):
        raise AssertionError(f"local disk_usage probed for {path}")

    monkeypatch.setattr(storage.shutil, "disk_usage", no_statvfs)
    storage.assert_headroom("vos:cadc/projects/hats", 1e9, "cache root")
    assert list(tmp_path.iterdir()) == []


def test_local_storage_fsyncs_before_publishing(tmp_path, monkeypatch) -> None:
    """A rename without fsync can publish an empty file after a crash."""
    import os

    import polars as pl

    from xmatcher import storage

    events: list[str] = []
    real_fsync, real_replace = os.fsync, os.replace

    def fsync(fd):
        events.append("fsync")
        real_fsync(fd)

    def replace(src, dst):
        events.append("replace")
        real_replace(src, dst)

    monkeypatch.setattr(storage.os, "fsync", fsync)
    monkeypatch.setattr(storage.os, "replace", replace)
    cache = storage.LocalStorage(tmp_path / "cache")
    local = tmp_path / "part.bin"
    local.write_bytes(b"x" * 100)
    cache.stage_out(local, "a/part.bin")
    assert events == ["fsync", "replace"]
    events.clear()
    cache.write_parquet(pl.DataFrame({"a": [1]}), "a/p.parquet")
    assert events == ["fsync", "replace"]
    assert cache.size("a/part.bin") == 100


# --------------------------------------------------------------------------- #
# streamed HTTP partition downloads
# --------------------------------------------------------------------------- #
def test_streamed_http_download_rejects_a_short_body(tmp_path, monkeypatch) -> None:
    """A dropped connection mid-body must not publish a short partition."""
    import http.client

    attempts: list[int] = []

    class Short(io.BytesIO):
        headers = {"Content-Length": "100"}

    def short(request, timeout=None):
        attempts.append(1)
        return Short(b"x" * 60)

    monkeypatch.setattr(urllib.request, "urlopen", short)
    monkeypatch.setattr(http_retry.time, "sleep", lambda _s: None)
    dest = tmp_path / "Npix=1.parquet"
    with pytest.raises(http.client.IncompleteRead):
        mirror._http_request(
            "https://example.test/Npix=1.parquet", mirror.TokenBucket(0), dest=dest
        )
    assert len(attempts) == mirror._HTTP_RETRIES


def test_streamed_http_download_writes_the_body(tmp_path, monkeypatch) -> None:
    class Full(io.BytesIO):
        headers = {"Content-Length": "5"}

    monkeypatch.setattr(urllib.request, "urlopen", lambda request, timeout=None: Full(b"abcde"))
    dest = tmp_path / "f"
    body, size = mirror._http_request("https://example.test/f", mirror.TokenBucket(0), dest=dest)
    assert body is None and size == 5
    assert dest.read_bytes() == b"abcde"


# --------------------------------------------------------------------------- #
# concurrent syncs and remaining-bytes headroom
# --------------------------------------------------------------------------- #
def _remote_src(ident: str = "https://example.test/cat"):
    from xmatcher.sources import CatalogueSource

    return CatalogueSource(
        name="remote-hats",
        is_local=False,
        access_method="hats",
        access_identifier=ident,
    )


def test_a_second_concurrent_sync_of_one_tree_fails_fast(tmp_path, monkeypatch) -> None:
    """Two syncs of one tree used to interleave; the last sync.json write won."""
    import fcntl

    from xmatcher.exceptions import CrossMatchError
    from xmatcher.storage import LocalStorage

    src = _remote_src()
    cache_root = tmp_path / "cache"
    ran: list[str] = []
    monkeypatch.setattr(mirror, "_sync_with_failover", lambda *_a, **_k: ran.append("sync"))
    lock = cache_root / "remote-hats" / f".{mirror._version_dir(src)}.lock"
    lock.parent.mkdir(parents=True)
    with open(lock, "a") as held:
        fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)  # the other sync
        with pytest.raises(CrossMatchError, match="Another 'xmatcher sync'"):
            mirror.sync_catalogue(src, cache_root=str(cache_root), min_free_gb=0)
    assert ran == []
    # Released (as when the other process exits): the sync runs.
    mirror.sync_catalogue(src, cache_root=str(cache_root), min_free_gb=0)
    assert ran == ["sync"]
    # A different tree is not blocked by this one's lock.
    with mirror._sync_lock(LocalStorage(cache_root), src):
        other = _remote_src("https://example.test/other")
        mirror.sync_catalogue(other, cache_root=str(cache_root), min_free_gb=0)
    assert ran == ["sync", "sync"]


def test_sync_refuses_when_remaining_downloads_cross_the_floor(tmp_path, monkeypatch) -> None:
    """The floor was checked once against today's free space, not the bytes to come."""
    import collections

    from xmatcher.exceptions import CrossMatchError

    gib = 1024**3
    usage = collections.namedtuple("usage", "total used free")
    monkeypatch.setattr(mirror.shutil, "disk_usage", lambda _p: usage(100 * gib, 95 * gib, 5 * gib))
    listing = [
        {"rel": "properties", "size": 100},
        {"rel": "dataset/Norder=1/Dir=0/Npix=1.parquet", "size": 3 * gib},
        {"rel": "dataset/Norder=1/Dir=0/Npix=2.parquet", "size": 2 * gib},
    ]
    monkeypatch.setattr(mirror, "_remote_hats_listing", lambda *_a, **_k: listing)

    def no_fetch(*_a, **_k):
        raise AssertionError("nothing may be fetched")

    monkeypatch.setattr(mirror, "_http_request", no_fetch)
    with pytest.raises(CrossMatchError, match="5.0 GiB still to download"):
        mirror.sync_catalogue(_remote_src(), cache_root=str(tmp_path / "cache"), min_free_gb=1)
    # Within the floor, the same listing is fetched normally.
    fetched: list[str] = []

    def fetch(url, bucket, *, dest=None, **_k):
        fetched.append(url)
        dest.write_bytes(b"x")
        return None, 1

    monkeypatch.setattr(mirror, "_http_request", fetch)
    monkeypatch.setattr(
        mirror.shutil, "disk_usage", lambda _p: usage(100 * gib, 80 * gib, 20 * gib)
    )
    stats = mirror.sync_catalogue(_remote_src(), cache_root=str(tmp_path / "cache"), min_free_gb=1)
    assert stats.files_downloaded == 3 and len(fetched) == 3
