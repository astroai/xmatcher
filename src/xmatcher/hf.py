"""Anonymous Hugging Face dataset reads for HATS trees, with no extra dependency.

The ``mmu_*`` Multimodal Universe catalogues are published on the Hugging Face
Hub, and the IVOA registry names them ``hf://datasets/<owner>/<name>``.  A Hub
dataset is a git repository served over HTTP, so this module uses the public
API directly: the tree endpoint lists a directory (``recursive=true`` and
``expand=true`` include file sizes) and ``/resolve/<revision>/<path>`` serves
the bytes.  No ``huggingface_hub`` dependency, no token, and no clone of a
multi-gigabyte repository.

Reads only, like the S3 reader: a mirror is written to the local cache (or
VOSpace), never back into the Hub.

A gated or private dataset answers 401/403; that is reported as such rather
than mistaken for an empty catalogue.  ``XMATCHER_HF_ENDPOINT`` (or
``HFRemote(..., endpoint=...)``) points the reader at a Hub-compatible service
(internal mirror, or a test server).

The tree endpoint paginates with ``Link: <…>; rel="next"`` and a HATS tree
holds thousands of partitions, so every listing walks the cursor instead of
trusting the first page.
"""

from __future__ import annotations

import io
import json
import os
import shutil
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

import polars as pl

#: Environment variable selecting a Hub-compatible endpoint.
ENDPOINT_ENV = "XMATCHER_HF_ENDPOINT"
#: Public Hugging Face Hub.
DEFAULT_ENDPOINT = "https://huggingface.co"
#: Revision read when the URI names none.
DEFAULT_REVISION = "main"

_PREFIX = "hf://"
_RETRIES = 3
_BACKOFF_CAP_S = 60.0
_MAX_LIST_PAGES = 1000
_PAGE_LIMIT = 100
_TIMEOUT_S = 120


def is_hf_uri(text: object) -> bool:
    """True for ``hf://datasets/<owner>/<name>[/<path>]``."""
    return isinstance(text, str) and text.startswith(_PREFIX)


def split_hf_uri(uri: str) -> tuple[str, str]:
    """Split ``hf://datasets/owner/name/some/path`` into ``(repo, "some/path")``."""
    if not is_hf_uri(uri):
        raise ValueError(f"not an hf:// URI: {uri!r}")
    rest = uri[len(_PREFIX) :]
    kind, _, tail = rest.partition("/")
    if kind != "datasets":
        raise ValueError(
            f"{uri!r} does not name a dataset; only hf://datasets/<owner>/<name> is read"
        )
    parts = [part for part in tail.split("/") if part]
    if len(parts) < 2:
        raise ValueError(f"hf:// URI needs an <owner>/<name> dataset: {uri!r}")
    if any(part in (".", "..") for part in parts):
        raise ValueError(f"hf:// URI has an unsafe path: {uri!r}")
    return "/".join(parts[:2]), "/".join(parts[2:])


def parse_hf_tree(body: bytes) -> list[dict[str, Any]]:
    """Parse one tree-endpoint page into ``{type, path, size}`` entries."""
    try:
        payload = json.loads(body)
    except ValueError as exc:
        raise OSError(f"the Hugging Face API returned invalid JSON: {exc}") from exc
    if isinstance(payload, dict):
        reason = payload.get("error") or payload.get("message") or payload
        raise OSError(f"the Hugging Face API returned an error: {reason}")
    if not isinstance(payload, list):
        raise OSError("the Hugging Face API did not return a tree listing")
    entries: list[dict[str, Any]] = []
    for item in payload:
        if not isinstance(item, dict) or not isinstance(item.get("path"), str):
            continue
        size: Any = item.get("size")
        lfs = item.get("lfs")
        if isinstance(lfs, dict) and isinstance(lfs.get("size"), int):
            size = lfs["size"]
        entries.append(
            {
                "type": str(item.get("type") or "file"),
                "path": item["path"],
                "size": int(size) if isinstance(size, int) else -1,
            }
        )
    return entries


def next_page_url(header: str | None) -> str | None:
    """Extract the ``rel="next"`` target from a ``Link`` header, if any."""
    if not header:
        return None
    for part in header.split(","):
        segments = part.split(";")
        if len(segments) < 2:
            continue
        target = segments[0].strip()
        if not (target.startswith("<") and target.endswith(">")):
            continue
        relations = " ".join(segment.strip() for segment in segments[1:])
        if 'rel="next"' in relations or "rel=next" in relations:
            return target[1:-1]
    return None


class _Missing(Exception):
    """The requested entry does not exist (HTTP 404)."""


class HFRemote:
    """Reader for one Hugging Face dataset prefix (or one directory inside it).

    ``exists``, ``size``, ``list``, ``index``, ``stage_in`` and
    ``read_parquet`` are the operations the mirror needs from a remote tree
    root, so an ``hf://`` catalogue is listed and fetched by the same code path
    as an ``s3://`` one.  ``limiter`` (anything with ``acquire()``) shares the
    sync's per-host rate limit.
    """

    def __init__(
        self,
        uri: str,
        *,
        endpoint: str | None = None,
        revision: str = DEFAULT_REVISION,
        limiter: Any = None,
    ) -> None:
        repo, path = split_hf_uri(uri)
        self.repo = repo
        self._repo_q = urllib.parse.quote(repo, safe="/")
        self.path = path
        self.revision = (revision or DEFAULT_REVISION).strip() or DEFAULT_REVISION
        selected = endpoint if endpoint is not None else os.environ.get(ENDPOINT_ENV, "")
        self.endpoint = (selected or DEFAULT_ENDPOINT).rstrip("/")
        self._limiter = limiter
        self.uri = f"hf://datasets/{repo}" + (f"/{path}" if path else "")

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"HFRemote({self.uri!r})"

    # -- paths ------------------------------------------------------------ #
    def key(self, rel: str = "") -> str:
        """Repo-relative path for a tree-relative path (validated, no traversal)."""
        rel = rel.strip("/")
        if rel:
            if any(part in ("", ".", "..") for part in rel.split("/")):
                raise ValueError(f"unsafe Hugging Face path {rel!r}")
            return f"{self.path}/{rel}" if self.path else rel
        return self.path

    def _quote(self, text: str) -> str:
        return urllib.parse.quote(text, safe="/")

    def _tree_url(self, rel: str = "", *, recursive: bool) -> str:
        url = (
            f"{self.endpoint}/api/datasets/{self._repo_q}/tree/"
            f"{urllib.parse.quote(self.revision, safe='')}"
        )
        key = self.key(rel)
        if key:
            url += "/" + self._quote(key)
        params = {"expand": "true", "limit": str(_PAGE_LIMIT)}
        if recursive:
            params["recursive"] = "true"
        return f"{url}?{urllib.parse.urlencode(params)}"

    def resolve_url(self, rel: str = "") -> str:
        """Download URL of the file at ``rel`` (follows the Hub's redirect)."""
        return (
            f"{self.endpoint}/datasets/{self._repo_q}/resolve/"
            f"{urllib.parse.quote(self.revision, safe='')}/{self._quote(self.key(rel))}"
        )

    def child(self, name: str) -> HFRemote:
        """A reader rooted at the child directory ``name`` of this dataset."""
        return HFRemote(
            f"{self.uri.rstrip('/')}/{name.strip('/')}",
            endpoint=self.endpoint,
            revision=self.revision,
            limiter=self._limiter,
        )

    def display(self, url: str) -> str:
        """``hf://…`` form of a request URL, for error messages."""
        api = f"{self.endpoint}/api/datasets/{self.repo}/tree/"
        resolve = f"{self.endpoint}/datasets/{self.repo}/resolve/"
        for prefix in (api, resolve):
            if url.startswith(prefix):
                rest = url[len(prefix) :].split("?", 1)[0]
                parts = rest.split("/", 1)
                key = self._unquote(parts[1]) if len(parts) > 1 else ""
                return f"hf://datasets/{self.repo}" + (f"/{key}" if key else "")
        return url

    @staticmethod
    def _unquote(text: str) -> str:
        return urllib.parse.unquote(text)

    # -- HTTP ------------------------------------------------------------- #
    def _open(self, url: str, *, method: str = "GET"):
        """Open ``url``, retrying throttling and connection errors.

        404 raises :class:`_Missing`; a denied request raises :class:`OSError`
        naming the likely cause instead of looking like absent data.
        """
        last: Exception | None = None
        for attempt in range(_RETRIES):
            if self._limiter is not None:
                self._limiter.acquire()
            request = urllib.request.Request(url, method=method)  # noqa: S310 - https only
            try:
                return urllib.request.urlopen(request, timeout=_TIMEOUT_S)  # noqa: S310
            except urllib.error.HTTPError as exc:
                if exc.code == 404:
                    exc.close()
                    raise _Missing(url) from exc
                if exc.code in (401, 403):
                    exc.close()
                    raise OSError(
                        f"Hugging Face denied anonymous access to {self.display(url)} "
                        f"(HTTP {exc.code}); the dataset is private or gated, and "
                        "xmatcher sends no token"
                    ) from exc
                if exc.code not in (429, 500, 502, 503, 504):
                    exc.close()
                    raise OSError(
                        f"Hugging Face request failed for {self.display(url)} "
                        f"(HTTP {exc.code} {exc.reason})"
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
        raise OSError(f"Hugging Face request failed for {self.display(url)}: {last}") from last

    def _pages(self, url: str) -> list[dict[str, Any]]:
        """Every entry reachable from ``url``, following ``Link`` pagination."""
        entries: list[dict[str, Any]] = []
        seen: set[str] = set()
        for _ in range(_MAX_LIST_PAGES):
            if url in seen:
                raise OSError(f"Hugging Face pagination repeated {self.display(url)}")
            seen.add(url)
            with self._open(url) as resp:
                body = resp.read()
                link = resp.headers.get("Link") if resp.headers else None
            entries.extend(parse_hf_tree(body))
            following = next_page_url(link)
            if not following:
                return entries
            url = following
        raise OSError(f"the Hugging Face listing for {self.uri} did not terminate")

    # -- Storage read surface --------------------------------------------- #
    def exists(self, rel: str = "") -> bool:
        """True when ``rel`` names a file; a 401/403 is an error, not ``False``."""
        try:
            with self._open(self.resolve_url(rel), method="HEAD") as resp:
                return int(getattr(resp, "status", 200) or 200) < 400
        except _Missing:
            return False

    def size(self, rel: str = "") -> int:
        """File size in bytes, or ``-1`` when unknown/absent."""
        try:
            with self._open(self.resolve_url(rel), method="HEAD") as resp:
                headers = resp.headers
        except _Missing:
            return -1
        for header in ("Content-Length", "X-Linked-Size"):
            value = headers.get(header) if headers else None
            if value and str(value).isdigit():
                return int(value)
        return -1

    def list(self, rel: str = "") -> list[str]:
        """Immediate children of ``rel``: file names and ``dir/`` prefixes."""
        base = self.key(rel)
        try:
            entries = self._pages(self._tree_url(rel, recursive=False))
        except _Missing:
            return []
        names: set[str] = set()
        for entry in entries:
            path = entry["path"].strip("/")
            if base:
                if not path.startswith(f"{base}/"):
                    continue
                path = path[len(base) + 1 :]
            if not path or "/" in path:
                continue
            names.add(path + ("/" if entry["type"] == "directory" else ""))
        return sorted(names)

    def index(self, rel: str = "") -> dict[str, int]:
        """Recursive ``{tree-relative path: size}`` map of the files under ``rel``.

        One paginated walk supplies every size the mirror needs, replacing two
        HEAD probes per partition.
        """
        base = self.key(rel)
        try:
            entries = self._pages(self._tree_url(rel, recursive=True))
        except _Missing:
            return {}
        prefix = f"{base}/" if base else ""
        objects: dict[str, int] = {}
        for entry in entries:
            if entry["type"] != "file":
                continue
            path = entry["path"].strip("/")
            if prefix:
                if not path.startswith(prefix):
                    continue
                path = path[len(prefix) :]
            if path:
                objects[path] = entry["size"]
        return objects

    def stage_in(self, rel: str, dest: Path) -> None:
        """Download the file at ``rel`` to ``dest`` (streamed, atomic rename)."""
        dest = Path(dest)
        dest.parent.mkdir(parents=True, exist_ok=True)
        partial = dest.with_name(f".{dest.name}.hfpart")
        try:
            with self._open(self.resolve_url(rel)) as resp, partial.open("wb") as out:
                shutil.copyfileobj(resp, out, length=1 << 20)
            os.replace(partial, dest)
        finally:
            partial.unlink(missing_ok=True)

    def read_bytes(self, rel: str = "") -> bytes:
        """Whole file as bytes."""
        with self._open(self.resolve_url(rel)) as resp:
            return resp.read()

    def read_parquet(self, rel: str) -> pl.DataFrame:
        """Read a Parquet file without staging it to disk."""
        return pl.read_parquet(io.BytesIO(self.read_bytes(rel)))


__all__ = [
    "DEFAULT_ENDPOINT",
    "DEFAULT_REVISION",
    "ENDPOINT_ENV",
    "HFRemote",
    "is_hf_uri",
    "next_page_url",
    "parse_hf_tree",
    "split_hf_uri",
]
