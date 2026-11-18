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
