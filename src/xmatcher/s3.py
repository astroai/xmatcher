"""Anonymous S3 reads for HATS trees, with no extra dependency.

The IVOA HATS registry publishes several catalogues only on S3
(``s3://stpubdata/gaia/gaia_dr3/public/hats``, IRSA's ``ipac-irsa-*`` and
``nasa-irsa-*`` buckets).  Public buckets answer unsigned requests, so this
module speaks plain HTTPS: ListObjectsV2 for listings and object sizes,
HEAD/GET for metadata and partitions.  No credential chain and no ``boto3``.

Reads only.  A mirror is always written to the local cache (or VOSpace), never
back into the bucket: S3 has no rename, so publishing a partially transferred
tree would expose a half-written catalogue to every other reader.

A bucket that needs signed or requester-pays access is reported with that
reason.  It is *not* silently treated as missing data, because an empty HATS
catalogue and a denied bucket look identical to a reader that swallows 403.

``XMATCHER_S3_ENDPOINT`` (or ``S3Remote(..., endpoint=...)``) points the reader
at an S3-compatible service (MinIO, Ceph RGW, a test server).  URL building is
then path-style, ``{endpoint}/{bucket}/{key}``; without it, the AWS
virtual-hosted endpoint ``https://{bucket}.s3.amazonaws.com/{key}`` is used and
S3's 301 region redirect is followed by ``urllib``.
"""

from __future__ import annotations

import io
import os
import shutil
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

import polars as pl

#: Environment variable selecting an S3-compatible endpoint (path-style URLs).
ENDPOINT_ENV = "XMATCHER_S3_ENDPOINT"

_PREFIX = "s3://"
_RETRIES = 3
_BACKOFF_CAP_S = 60.0
_MAX_LIST_PAGES = 10_000
_TIMEOUT_S = 120


def is_s3_uri(text: object) -> bool:
    """True for ``s3://bucket/key`` (and ``s3://bucket``)."""
    return isinstance(text, str) and text.startswith(_PREFIX)


def split_s3_uri(uri: str) -> tuple[str, str]:
    """Split ``s3://bucket/some/prefix`` into ``(bucket, "some/prefix")``."""
    if not is_s3_uri(uri):
        raise ValueError(f"not an s3:// URI: {uri!r}")
    rest = uri[len(_PREFIX) :]
    bucket, _, prefix = rest.partition("/")
    bucket = bucket.strip()
    if not bucket:
        raise ValueError(f"s3:// URI has no bucket: {uri!r}")
    if any(ch.isspace() for ch in bucket) or "/" in bucket:
        raise ValueError(f"invalid S3 bucket name in {uri!r}")
    cleaned = prefix.strip("/")
    parts = cleaned.split("/") if cleaned else []
    if any(part in ("", ".", "..") for part in parts):
        raise ValueError(f"s3:// URI has an unsafe prefix: {uri!r}")
    return bucket, cleaned


class _Missing(Exception):
    """The requested object does not exist (HTTP 404)."""


def _child_text(node: ET.Element, name: str) -> str:
    """Text of the first child element called ``name``, ignoring namespaces."""
    for child in node:
        if child.tag.rsplit("}", 1)[-1] == name:
            return (child.text or "").strip()
    return ""


def _parse_page(body: bytes) -> tuple[ET.Element, str | None]:
    """Parse one ListObjectsV2 page into its root element and next token."""
    try:
        root = ET.fromstring(body)
    except ET.ParseError as exc:
        raise OSError(f"the S3 endpoint did not return a ListBucketResult document: {exc}") from exc
    if root.tag.rsplit("}", 1)[-1] != "ListBucketResult":
        raise OSError(
            "the S3 endpoint answered with "
            f"<{root.tag.rsplit('}', 1)[-1]}> instead of a ListBucketResult document; "
            "check XMATCHER_S3_ENDPOINT and the bucket name"
        )
    token = _child_text(root, "NextContinuationToken")
    truncated = _child_text(root, "IsTruncated").lower() == "true"
    return root, (token or None) if truncated else None


def parse_s3_listing(body: bytes) -> tuple[list[str], str | None]:
    """Parse a ListObjectsV2 page into ``(names, next_token)``.

    Names keep a trailing ``/`` for common prefixes (directories) and are bare
    object names otherwise, matching :meth:`xmatcher.storage.Storage.list`.
    """
    root, token = _parse_page(body)
    names: list[str] = []
    for node in root:
        tag = node.tag.rsplit("}", 1)[-1]
        if tag == "CommonPrefixes":
            prefix = _child_text(node, "Prefix").strip("/")
            if prefix:
                names.append(prefix.rsplit("/", 1)[-1] + "/")
        elif tag == "Contents":
            raw = _child_text(node, "Key")
            key = raw.strip("/")
            if not key:
                continue
            # A zero-byte "directory marker" (`prefix/`) is a directory, not a
            # file: report it with the trailing slash so walkers do not probe
            # it as a partition.
            names.append(key.rsplit("/", 1)[-1] + ("/" if raw.endswith("/") else ""))
    return names, token


def parse_s3_objects(body: bytes) -> tuple[dict[str, int], str | None]:
    """Parse a ListObjectsV2 page into ``{key: size}`` and the next token."""
    root, token = _parse_page(body)
    objects: dict[str, int] = {}
    for node in root:
        if node.tag.rsplit("}", 1)[-1] != "Contents":
            continue
        key = _child_text(node, "Key")
        if not key or key.endswith("/"):
            # Directory markers carry no data; including them would put an
            # empty child in every derived directory listing.
            continue
        size = _child_text(node, "Size")
        objects[key] = int(size) if size.isdigit() else -1
    return objects, token


class S3Remote:
    """Reader for one ``s3://`` prefix, exposing the Storage read surface.

    ``exists``, ``size``, ``list``, ``stage_in`` and ``read_parquet`` are the
    operations the mirror needs from a remote tree root, so an S3 catalogue is
    listed and fetched by the same code path as a ``vos:`` one.  ``limiter``
    (anything with ``acquire()``) shares the sync's per-host rate limit.
    """

    def __init__(
        self,
        uri: str,
        *,
        endpoint: str | None = None,
        limiter: Any = None,
    ) -> None:
        bucket, prefix = split_s3_uri(uri)
        self.bucket = bucket
        self.prefix = prefix
        self.uri = f"s3://{bucket}/{prefix}" if prefix else f"s3://{bucket}"
        self.endpoint = (
            endpoint if endpoint is not None else os.environ.get(ENDPOINT_ENV, "")
        ).rstrip("/")
        self._limiter = limiter

    # -- URLs ------------------------------------------------------------- #
    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"S3Remote({self.uri!r})"

    def _base(self) -> str:
        if self.endpoint:
            return f"{self.endpoint}/{self.bucket}"
        return f"https://{self.bucket}.s3.amazonaws.com"

    def key(self, rel: str = "") -> str:
        """Full object key for a tree-relative path (validated, no traversal)."""
        rel = rel.strip("/")
        if rel:
            if any(part in ("", ".", "..") for part in rel.split("/")):
                raise ValueError(f"unsafe S3 key {rel!r}")
            return f"{self.prefix}/{rel}" if self.prefix else rel
        return self.prefix

    def object_url(self, rel: str = "") -> str:
        """HTTPS URL of the object at ``rel``."""
        return f"{self._base()}/{urllib.parse.quote(self.key(rel), safe='/')}"

    def child(self, name: str) -> S3Remote:
        """A reader rooted at the child prefix ``name`` of this tree."""
        return S3Remote(f"{self.uri.rstrip('/')}/{name.strip('/')}", limiter=self._limiter)

    def _list_url(self, rel: str, token: str | None, *, recursive: bool = False) -> str:
        prefix = self.key(rel)
        if prefix and not prefix.endswith("/"):
            prefix += "/"
        params = {"list-type": "2", "prefix": prefix}
        if not recursive:
            params["delimiter"] = "/"
        if token:
            params["continuation-token"] = token
        query = urllib.parse.urlencode(params, quote_via=urllib.parse.quote)
        return f"{self._base()}/?{query}"

    def display(self, url: str) -> str:
        """``s3://…`` form of a request URL, for error messages."""
        base = f"{self._base()}/"
        if url.startswith(base):
            key = urllib.parse.unquote(url[len(base) :].split("?", 1)[0])
            return f"s3://{self.bucket}/{key}"
        return url

    # -- HTTP ------------------------------------------------------------- #
    def _open(self, url: str, *, method: str = "GET"):
        """Open ``url``, retrying throttling/connection errors.

        404 raises :class:`_Missing`; a denied request raises :class:`OSError`
        naming the likely cause instead of masquerading as an absent object.
        """
        last: Exception | None = None
        for attempt in range(_RETRIES):
            if self._limiter is not None:
                self._limiter.acquire()
            request = urllib.request.Request(url, method=method)  # noqa: S310 - https/s3 only
            try:
                return urllib.request.urlopen(request, timeout=_TIMEOUT_S)  # noqa: S310
            except urllib.error.HTTPError as exc:
                if exc.code == 404:
                    exc.close()
                    raise _Missing(url) from exc
                if exc.code == 403:
                    exc.close()
                    raise OSError(
                        f"S3 denied anonymous access to {self.display(url)} (HTTP 403); "
                        "the bucket or key needs credentials or requester-pays access, "
                        "which xmatcher does not send"
                    ) from exc
                if exc.code not in (429, 503):
                    exc.close()
                    raise OSError(
                        f"S3 request failed for {self.display(url)} (HTTP {exc.code} {exc.reason})"
                    ) from exc
                last = exc
                retry_after = exc.headers.get("Retry-After") if exc.headers else None
                try:
                    delay = float(retry_after) if retry_after else 1.0
                except ValueError:
                    delay = 1.0
                exc.close()
            except (urllib.error.URLError, OSError) as exc:
                last = exc
                delay = 1.0
            if attempt < _RETRIES - 1:
                time.sleep(min(delay * (2**attempt) + 0.1, _BACKOFF_CAP_S))
        raise OSError(f"S3 request failed for {self.display(url)}: {last}") from last

    def _read_url(self, url: str) -> bytes:
        with self._open(url) as resp:
            return resp.read()

    # -- Storage read surface --------------------------------------------- #
    def exists(self, rel: str = "") -> bool:
        """True when ``rel`` names an object; a 403 is an error, not ``False``."""
        try:
            with self._open(self.object_url(rel), method="HEAD") as resp:
                return int(getattr(resp, "status", 200) or 200) < 400
        except _Missing:
            return False

    def size(self, rel: str = "") -> int:
        """Object size in bytes, or ``-1`` when unknown/absent."""
        try:
            with self._open(self.object_url(rel), method="HEAD") as resp:
                length = resp.headers.get("Content-Length")
        except _Missing:
            return -1
        return int(length) if length and str(length).isdigit() else -1

    def list(self, rel: str = "") -> list[str]:
        """Immediate children of ``rel``: object names and ``dir/`` prefixes."""
        names: set[str] = set()
        token: str | None = None
        for _ in range(_MAX_LIST_PAGES):
            body = self._read_url(self._list_url(rel, token))
            page_names, token = parse_s3_listing(body)
            names.update(page_names)
            if token is None:
                break
        return sorted(names)

    def index(self, rel: str = "") -> dict[str, int]:
        """Recursive ``{tree-relative key: size}`` map under ``rel``.

        One paginated ListObjectsV2 walk replaces two HEAD probes per file.
        The mirror needs every file's size anyway, so this is both fewer
        requests and the only practical way to list IRSA/LSDB trees that
        hold thousands of partitions.
        """
        base = self.key(rel)
        if base and not base.endswith("/"):
            base += "/"
        objects: dict[str, int] = {}
        token: str | None = None
        for _ in range(_MAX_LIST_PAGES):
            body = self._read_url(self._list_url(rel, token, recursive=True))
            page, token = parse_s3_objects(body)
            for key, size in page.items():
                relative = key[len(base) :] if key.startswith(base) else key
                if relative:
                    objects[relative] = size
            if token is None:
                break
        return objects

    def stage_in(self, rel: str, dest: Path) -> None:
        """Download the object at ``rel`` to ``dest`` (streamed, atomic rename)."""
        url = self.object_url(rel)
        dest = Path(dest)
        dest.parent.mkdir(parents=True, exist_ok=True)
        partial = dest.with_name(f".{dest.name}.s3part")
        try:
            with self._open(url) as resp, partial.open("wb") as out:
                shutil.copyfileobj(resp, out, length=1 << 20)
            os.replace(partial, dest)
        finally:
            partial.unlink(missing_ok=True)

    def read_bytes(self, rel: str = "") -> bytes:
        """Whole object as bytes."""
        return self._read_url(self.object_url(rel))

    def read_parquet(self, rel: str) -> pl.DataFrame:
        """Read a Parquet object without staging it to disk."""
        return pl.read_parquet(io.BytesIO(self.read_bytes(rel)))


__all__ = [
    "ENDPOINT_ENV",
    "S3Remote",
    "is_s3_uri",
    "parse_s3_listing",
    "parse_s3_objects",
    "split_s3_uri",
]
