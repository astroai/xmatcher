"""Anonymous Hugging Face reads: HATS listing, mirroring, collection resolution.

The fake Hub speaks just enough of the public API for the reader: paginated
tree listings (``Link: <…>; rel="next"``), file sizes, ``/resolve/`` downloads,
404 for absent paths and 403 for a gated dataset.  The end-to-end test mirrors
a directory-partition tree served that way into the cache and reads it back as
a HATS catalogue.
"""

from __future__ import annotations

import contextlib
import functools
import json
import threading
import urllib.parse
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import polars as pl
import pytest

from xmatcher import hats_native, mirror
from xmatcher.hf import (
    HFRemote,
    is_hf_uri,
    next_page_url,
    parse_hf_tree,
    split_hf_uri,
)
from xmatcher.sources import CatalogueSource, is_remote_hats_uri

REPO = "UniverseTBD/mmu_test"


def _entries(files: dict[str, bytes], base: str, *, recursive: bool) -> list[dict]:
    """Tree entries under ``base`` the way the Hub reports them."""
    prefix = f"{base}/" if base else ""
    directories: set[str] = set()
    found: list[dict] = []
    for key, payload in sorted(files.items()):
        if not key.startswith(prefix):
            continue
        rest = key[len(prefix) :]
        if not rest:
            continue
        if not recursive:
            head, sep, _tail = rest.partition("/")
            if sep:
                directories.add(prefix + head)
                continue
        else:
            parts = rest.split("/")
            for depth in range(1, len(parts)):
                directories.add(prefix + "/".join(parts[:depth]))
        found.append({"type": "file", "path": prefix + rest, "size": len(payload)})
    entries = [{"type": "directory", "path": path, "size": 0} for path in sorted(directories)]
    return sorted(entries, key=lambda entry: str(entry["path"])) + found


class _HubHandler(BaseHTTPRequestHandler):
    """Minimal Hugging Face Hub stub backed by an in-memory file map."""

    protocol_version = "HTTP/1.1"

    def __init__(
        self,
        *args,
        files: dict[str, bytes] | None = None,
        denied: tuple[str, ...] = (),
        page_size: int = 100,
        **kwargs,
    ) -> None:
        self.files = dict(files or {})
        self.denied = set(denied)
        self.page_size = page_size
        super().__init__(*args, **kwargs)

    def log_message(self, *args) -> None:  # keep pytest output clean
        return

    def _denied(self, target: str) -> bool:
        return any(target == entry or target.startswith(f"{entry}/") for entry in self.denied)

    def _send(self, status: int, body: bytes = b"", headers: dict[str, str] | None = None) -> None:
        self.send_response(status)
        for name, value in (headers or {}).items():
            self.send_header(name, value)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if body and self.command != "HEAD":
            self.wfile.write(body)

    def _split(self) -> tuple[str, str, str]:
        """``(kind, path, query)`` for the tree and resolve endpoints."""
        parsed = urllib.parse.urlsplit(self.path)
        path = urllib.parse.unquote(parsed.path)
        for kind, marker in (("tree", "/api/datasets/"), ("resolve", "/datasets/")):
            if not path.startswith(marker):
                continue
            rest = path[len(marker) :]
            repo, _, tail = rest.partition("/")
            owner = repo
            name, _, tail = tail.partition("/")
            if kind == "tree":
                verb, _, tail = tail.partition("/")
                if verb != "tree":
                    return "", "", ""
            else:
                verb, _, tail = tail.partition("/")
                if verb != "resolve":
                    return "", "", ""
            _revision, _, rel = tail.partition("/")
            return kind, f"{owner}/{name}/{rel}".rstrip("/"), parsed.query
        return "", "", ""

    def do_HEAD(self) -> None:  # noqa: N802 - http.server API
        kind, target, _query = self._split()
        if kind != "resolve" or not target:
            self._send(404)
            return
        if self._denied(target):
            self._send(403)
            return
        payload = self.files.get(target)
        if payload is None:
            self._send(404)
        else:
            self._send(200, headers={"Content-Length": str(len(payload))})

    def do_GET(self) -> None:  # noqa: N802 - http.server API
        kind, target, query = self._split()
        if kind == "tree":
            self._tree(target, query)
            return
        if kind == "resolve" and target:
            if self._denied(target):
                self._send(403)
                return
            payload = self.files.get(target)
            if payload is None:
                self._send(404)
            else:
                self._send(200, payload)
            return
        self._send(404)

    def _tree(self, target: str, query: str) -> None:
        repo, _, base = target.partition("/")
        repo = f"{repo}/{base.partition('/')[0]}" if base else repo
        if base and "/" in base:
            base = base.partition("/")[2]
        else:
            base = ""
        if repo in self.denied:
            self._send(403)
            return
        params = urllib.parse.parse_qs(query)
        recursive = params.get("recursive", ["false"])[0].lower() in ("1", "true", "yes")
        names = {
            key[len(repo) + 1 :]: payload
            for key, payload in self.files.items()
            if key.startswith(f"{repo}/")
        }
        entries = _entries(names, base, recursive=recursive)
        start = int(params.get("cursor", ["0"])[0] or 0)
        page = entries[start : start + self.page_size]
        body = json.dumps(page).encode()
        headers = {"Content-Type": "application/json"}
        if start + self.page_size < len(entries):
            cursor = start + self.page_size
            headers["Link"] = (
                f"<http://{self.headers['Host']}{self.path.split('?')[0]}"
                f'?recursive={"true" if recursive else "false"}&cursor={cursor}>; rel="next"'
            )
        self._send(200, body, headers)


@contextlib.contextmanager
def fake_hub(
    files: dict[str, bytes],
    *,
    denied: tuple[str, ...] = (),
    page_size: int = 100,
) -> Iterator[str]:
    """Serve ``files`` as a Hub-compatible endpoint; yield its base URL."""
    handler = functools.partial(_HubHandler, files=files, denied=denied, page_size=page_size)
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def _tree_files(tree: Path) -> dict[str, bytes]:
    """Map every file under ``tree`` to a ``REPO/relative`` dataset path."""
    return {
        f"{REPO}/{path.relative_to(tree).as_posix()}": path.read_bytes()
        for path in sorted(tree.rglob("*"))
        if path.is_file()
    }


# --------------------------------------------------------------------------- #
# URI handling
# --------------------------------------------------------------------------- #
def test_scheme_helpers_agree_on_hugging_face_identifiers() -> None:
    assert is_hf_uri("hf://datasets/owner/name")
    assert not is_hf_uri("s3://bucket/prefix")
    assert is_remote_hats_uri("hf://datasets/owner/name/hats/cat")
    assert not is_remote_hats_uri("/data/hats/cat")


def test_split_hf_uri_normalizes_the_dataset_path() -> None:
    assert split_hf_uri("hf://datasets/owner/name") == ("owner/name", "")
    assert split_hf_uri("hf://datasets/owner/name/hats/cat/") == (
        "owner/name",
        "hats/cat",
    )
    assert split_hf_uri("hf://datasets//owner//name//hats") == ("owner/name", "hats")


@pytest.mark.parametrize(
    "uri",
    [
        "hf://models/owner/name",
        "hf://datasets/owner",
        "hf://datasets/owner/name/../other",
        "hf://datasets/owner/name/./cat",
        "s3://bucket/key",
    ],
)
def test_split_hf_uri_rejects_unsupported_identifiers(uri: str) -> None:
    with pytest.raises(ValueError):
        split_hf_uri(uri)


def test_parse_hf_tree_reads_sizes_and_reports_api_errors() -> None:
    body = json.dumps(
        [
            {"type": "directory", "path": "cat/dataset", "size": 0},
            {"type": "file", "path": "cat/properties", "size": 31},
            {
                "type": "file",
                "path": "cat/skymap.fits",
                "size": 134,
                "lfs": {"size": 4096, "pointerSize": 134},
            },
        ]
    ).encode()
    assert parse_hf_tree(body) == [
        {"type": "directory", "path": "cat/dataset", "size": 0},
        {"type": "file", "path": "cat/properties", "size": 31},
        {"type": "file", "path": "cat/skymap.fits", "size": 4096},
    ]
    with pytest.raises(OSError, match="gated"):
        parse_hf_tree(b'{"error": "Repository is gated"}')


def test_next_page_url_picks_the_next_relation() -> None:
    header = (
        '<https://hub.test/api/a?cursor=1>; rel="prev", '
        '<https://hub.test/api/a?cursor=2>; rel="next"'
    )
    assert next_page_url(header) == "https://hub.test/api/a?cursor=2"
    assert next_page_url('<https://hub.test/api/a>; rel="next"') == "https://hub.test/api/a"
    assert next_page_url(None) is None
    assert next_page_url('<https://hub.test/api/a>; rel="prev"') is None


# --------------------------------------------------------------------------- #
# reader surface
# --------------------------------------------------------------------------- #
def test_hf_reader_lists_pages_sizes_and_stages_files(tmp_path: Path) -> None:
    files = {
        f"{REPO}/cat/properties": b"hats_nrows=2\n",
        f"{REPO}/cat/dataset/Norder=0/Dir=0/Npix=5.parquet": b"payload",
        f"{REPO}/cat/dataset/Norder=0/Dir=0/Npix=6.parquet": b"payload!",
    }
    with fake_hub(files, page_size=2) as endpoint:
        remote = HFRemote(f"hf://datasets/{REPO}/cat", endpoint=endpoint)
        assert remote.list() == ["dataset/", "properties"]
        assert remote.list("dataset/Norder=0/Dir=0") == [
            "Npix=5.parquet",
            "Npix=6.parquet",
        ]
        assert remote.exists("properties")
        assert not remote.exists("missing")
        assert remote.size("dataset/Norder=0/Dir=0/Npix=6.parquet") == 8
        assert remote.size("missing") == -1
        assert remote.read_bytes("properties") == b"hats_nrows=2\n"
        target = tmp_path / "nested" / "props"
        remote.stage_in("properties", target)
        assert target.read_bytes() == b"hats_nrows=2\n"


def test_hf_index_walks_every_file_across_pages() -> None:
    files = {
        f"{REPO}/cat/properties": b"hats_nrows=2\n",
        f"{REPO}/cat/dataset/Norder=0/Dir=0/Npix=5/part0.parquet": b"abc",
        f"{REPO}/cat/dataset/Norder=0/Dir=0/Npix=5/part1.parquet": b"abcd",
    }
    with fake_hub(files, page_size=1) as endpoint:
        index = HFRemote(f"hf://datasets/{REPO}/cat", endpoint=endpoint).index()
    assert index == {
        "properties": 13,
        "dataset/Norder=0/Dir=0/Npix=5/part0.parquet": 3,
        "dataset/Norder=0/Dir=0/Npix=5/part1.parquet": 4,
    }


def test_hf_denied_dataset_is_an_error_not_missing_data() -> None:
    files = {f"{REPO}/cat/properties": b"hats_nrows=2\n"}
    with fake_hub(files, denied=(REPO,)) as endpoint:
        remote = HFRemote(f"hf://datasets/{REPO}/cat", endpoint=endpoint)
        with pytest.raises(OSError, match="denied anonymous access"):
            remote.list()
        with pytest.raises(OSError, match="private or gated"):
            remote.read_bytes("properties")


def test_hf_reader_quotes_paths_that_need_escaping() -> None:
    remote = HFRemote("hf://datasets/owner/a bcat", endpoint="https://hub.example.test")
    assert remote.resolve_url("part 1.parquet") == (
        "https://hub.example.test/datasets/owner/a%20bcat/resolve/main/part%201.parquet"
    )


# --------------------------------------------------------------------------- #
# collection roots
# --------------------------------------------------------------------------- #
def _minimal_tree(rows: int = 3) -> bytes:
    return f"#HATS catalog\nhats_nrows={rows}\nhats_col_ra=ra\n".encode()


def test_hf_collection_resolves_to_its_primary_table(monkeypatch) -> None:
    files = {
        f"{REPO}/collection.properties": (
            b"hats_primary_table_url=catalogue\n"
            b"all_margins=catalogue_margin_10arcsec\n"
            b"all_indexes=catalogue_index_source_id\n"
        ),
        f"{REPO}/catalogue/properties": _minimal_tree(),
        f"{REPO}/catalogue_margin_10arcsec/properties": _minimal_tree(),
        f"{REPO}/catalogue_index_source_id/properties": _minimal_tree(),
    }
    with fake_hub(files) as endpoint:
        monkeypatch.setenv("XMATCHER_HF_ENDPOINT", endpoint)
        assert mirror.resolve_hats_tree_root(f"hf://datasets/{REPO}") == (
            f"hf://datasets/{REPO}/catalogue"
        )


def test_hf_collection_is_not_guessed_when_it_holds_two_catalogues(monkeypatch) -> None:
    files = {
        f"{REPO}/first/properties": _minimal_tree(),
        f"{REPO}/second/properties": _minimal_tree(),
    }
    with fake_hub(files) as endpoint:
        monkeypatch.setenv("XMATCHER_HF_ENDPOINT", endpoint)
        assert mirror.resolve_hats_tree_root(f"hf://datasets/{REPO}") == (f"hf://datasets/{REPO}")


def test_hf_tree_root_is_returned_unchanged(monkeypatch) -> None:
    files = {f"{REPO}/catalogue/properties": _minimal_tree()}
    with fake_hub(files) as endpoint:
        monkeypatch.setenv("XMATCHER_HF_ENDPOINT", endpoint)
        assert mirror.resolve_hats_tree_root(f"hf://datasets/{REPO}/catalogue") == (
            f"hf://datasets/{REPO}/catalogue"
        )


# --------------------------------------------------------------------------- #
# end-to-end mirror
# --------------------------------------------------------------------------- #
def test_hats_over_hf_mirrors_directory_partitions_and_reads_back(
    tmp_path: Path, monkeypatch
) -> None:
    frame = pl.DataFrame(
        {
            "id": list(range(120)),
            "ra": [10.0 + (i % 40) * 0.001 for i in range(120)],
            "dec": [-5.0 + (i % 30) * 0.001 for i in range(120)],
        }
    )
    tree = tmp_path / "tree"
    mirror._write_hats_native(frame, tree, ra_column="ra", dec_column="dec", threshold=50)

    # The mmu_* layout: single-file pixels become `Npix=N/part0.snappy.parquet`,
    # and the CSV keeps only the pixel numbers (no file_loc, no Nfiles).
    (tree / "dataset" / "partition_info.parquet").unlink()
    for pixel in sorted((tree / "dataset").glob("Norder=*/Dir=*/Npix=*.parquet")):
        target = pixel.with_suffix("") / "part0.snappy.parquet"
        target.parent.mkdir(parents=True, exist_ok=True)
        pixel.replace(target)
    for name in ("dataset/partition_info.csv", "partition_info.csv"):
        csv = tree / name
        pl.read_csv(csv).select(["Norder", "Dir", "Npix", "count"]).write_csv(csv)

    files = _tree_files(tree)
    with fake_hub(files, page_size=2) as endpoint:
        monkeypatch.setenv("XMATCHER_HF_ENDPOINT", endpoint)
        src = CatalogueSource(
            name="hf-hats",
            is_local=False,
            access_method="hats",
            access_identifier=f"hf://datasets/{REPO}",
            ra_column="ra",
            dec_column="dec",
        )
        cache = str(tmp_path / "cache")
        stats = mirror.sync_catalogue(src, cache_root=cache, rate_limit_rps=0.0)
        assert stats.failed == 0
        assert stats.files_downloaded >= 2  # properties/partition_info + pixels
        assert stats.bytes_downloaded > 0

        root, rel = mirror.locate_mirrored(src, cache_root=cache)
        mirrored = Path(root) / rel
        assert (mirrored / "properties").is_file()
        assert list(mirrored.glob("dataset/Norder=*/Dir=*/Npix=*/part*.parquet"))

        rows = hats_native.load_hats_all(
            CatalogueSource(name="mirrored", is_local=True, path=mirrored)
        )
        assert rows.height == 120
        assert sorted(rows["id"].to_list()) == list(range(120))

        # Second run is incremental: the manifest sizes already match.
        again = mirror.sync_catalogue(src, cache_root=cache, rate_limit_rps=0.0)
        assert again.failed == 0
        assert again.files_downloaded == 0
        assert again.files_skipped == stats.files_downloaded
