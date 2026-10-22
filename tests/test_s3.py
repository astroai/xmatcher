"""Anonymous S3 reads: HATS listing, mirroring, and collection resolution.

The fake S3 endpoint speaks just enough of the object API for the reader:
path-style URLs, ListObjectsV2 XML (with pagination and delimiters), HEAD/GET
objects, 404 for absent keys and 403 for a denied prefix.  It also proves the
mirror path end to end: an S3 tree published as directory partitions (the
IRSA/LSDB layout) is synced into the cache and read back as a HATS catalogue.
"""

from __future__ import annotations

import contextlib
import functools
import threading
import urllib.parse
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import polars as pl
import pytest

from xmatcher import hats_native, mirror
from xmatcher.s3 import (
    S3Remote,
    is_s3_uri,
    parse_s3_listing,
    parse_s3_objects,
    split_s3_uri,
)
from xmatcher.sources import CatalogueSource, is_remote_hats_uri

BUCKET = "catalogue"


class _S3Handler(BaseHTTPRequestHandler):
    """Minimal path-style S3 stub backed by an in-memory object map."""

    protocol_version = "HTTP/1.1"

    def __init__(
        self,
        *args,
        objects: dict[str, bytes] | None = None,
        forbidden: tuple[str, ...] = (),
        page_size: int = 1000,
        **kwargs,
    ) -> None:
        self.objects = dict(objects or {})
        self.forbidden = set(forbidden)
        self.page_size = page_size
        super().__init__(*args, **kwargs)

    def log_message(self, *args) -> None:  # keep pytest output clean
        return

    def _target(self) -> tuple[urllib.parse.SplitResult, str]:
        parsed = urllib.parse.urlsplit(self.path)
        path = parsed.path.lstrip("/")
        _bucket, _, key = path.partition("/")
        return parsed, urllib.parse.unquote(key)

    def _send(self, status: int, body: bytes = b"", headers: dict[str, str] | None = None) -> None:
        self.send_response(status)
        for name, value in (headers or {}).items():
            self.send_header(name, value)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if body and self.command != "HEAD":
            self.wfile.write(body)

    def do_HEAD(self) -> None:  # noqa: N802 - http.server API
        _, key = self._target()
        if key in self.forbidden:
            self._send(403)
        elif key in self.objects:
            self._send(200, headers={"Content-Length": str(len(self.objects[key]))})
        else:
            self._send(404)

    def do_GET(self) -> None:  # noqa: N802 - http.server API
        parsed, key = self._target()
        if parsed.query:
            self._list(parsed)
            return
        if key in self.forbidden:
            self._send(403)
        elif key in self.objects:
            self._send(200, self.objects[key])
        else:
            self._send(404)

    def _list(self, parsed: urllib.parse.SplitResult) -> None:
        query = urllib.parse.parse_qs(parsed.query)
        prefix = query.get("prefix", [""])[0]
        delimiter = query.get("delimiter", [None])[0]
        token = query.get("continuation-token", [None])[0]
        if any(key.startswith(prefix) for key in self.forbidden):
            self._send(403)
            return

        keys = sorted(key for key in self.objects if key.startswith(prefix))
        start = int(token) if token else 0
        page = keys[start : start + self.page_size]
        contents: list[str] = []
        prefixes: list[str] = []
        for key in page:
            rest = key[len(prefix) :]
            if delimiter and delimiter in rest:
                child = prefix + rest.split(delimiter, 1)[0] + delimiter
                if child not in prefixes:
                    prefixes.append(child)
            else:
                contents.append(key)
        truncated = start + self.page_size < len(keys)
        parts = [
            '<?xml version="1.0" encoding="UTF-8"?>',
            '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">',
            f"<Prefix>{prefix}</Prefix>",
            f"<IsTruncated>{'true' if truncated else 'false'}</IsTruncated>",
        ]
        if truncated:
            parts.append(f"<NextContinuationToken>{start + self.page_size}</NextContinuationToken>")
        for child in prefixes:
            parts.append(f"<CommonPrefixes><Prefix>{child}</Prefix></CommonPrefixes>")
        for key in contents:
            parts.append(
                f"<Contents><Key>{key}</Key><Size>{len(self.objects[key])}</Size></Contents>"
            )
        parts.append("</ListBucketResult>")
        self._send(200, "".join(parts).encode())


@contextlib.contextmanager
def fake_s3(
    objects: dict[str, bytes],
    *,
    forbidden: tuple[str, ...] = (),
    page_size: int = 1000,
) -> Iterator[str]:
    """Serve ``objects`` as an S3-compatible endpoint; yield its base URL."""
    handler = functools.partial(
        _S3Handler, objects=objects, forbidden=forbidden, page_size=page_size
    )
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def _tree_objects(root: Path, prefix: str) -> dict[str, bytes]:
    """Map every file under ``root`` to a ``prefix/relative`` S3 object."""
    return {
        f"{prefix}/{path.relative_to(root).as_posix()}": path.read_bytes()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


# --------------------------------------------------------------------------- #
# URI handling
# --------------------------------------------------------------------------- #
def test_scheme_helpers_agree_on_s3_identifiers() -> None:
    assert is_s3_uri("s3://bucket/prefix")
    assert not is_s3_uri("https://bucket/prefix")
    assert is_remote_hats_uri("s3://bucket/prefix")
    assert is_remote_hats_uri("https://example.org/hats/cat")
    assert not is_remote_hats_uri("/data/hats/cat")


def test_split_s3_uri_normalizes_the_prefix() -> None:
    assert split_s3_uri("s3://bucket/hats/cat/") == ("bucket", "hats/cat")
    assert split_s3_uri("s3://bucket//hats") == ("bucket", "hats")
    assert split_s3_uri("s3://bucket") == ("bucket", "")


@pytest.mark.parametrize(
    "uri",
    ["s3:///hats", "s3://bucket/hats/../other", "s3://bucket/hats/./cat", "https://bucket/x"],
)
def test_split_s3_uri_rejects_unsafe_identifiers(uri: str) -> None:
    with pytest.raises(ValueError):
        split_s3_uri(uri)


def test_parse_s3_listing_marks_directories_and_reads_the_token() -> None:
    body = (
        b'<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
        b"<IsTruncated>true</IsTruncated><NextContinuationToken>abc</NextContinuationToken>"
        b"<Contents><Key>hats/cat/properties</Key><Size>12</Size></Contents>"
        b"<CommonPrefixes><Prefix>hats/cat/dataset/</Prefix></CommonPrefixes>"
        b"</ListBucketResult>"
    )
    assert parse_s3_listing(body) == (["properties", "dataset/"], "abc")


def test_parse_s3_listing_treats_directory_markers_as_directories() -> None:
    body = (
        b'<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
        b"<IsTruncated>false</IsTruncated>"
        b"<Contents><Key>hats/cat/dataset/</Key><Size>0</Size></Contents>"
        b"<Contents><Key>hats/cat/dataset/Npix=5.parquet</Key><Size>9</Size></Contents>"
        b"</ListBucketResult>"
    )
    assert parse_s3_listing(body) == (["dataset/", "Npix=5.parquet"], None)


def test_parse_s3_objects_skips_directory_markers() -> None:
    body = (
        b'<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
        b"<IsTruncated>false</IsTruncated>"
        b"<Contents><Key>hats/cat/dataset/</Key><Size>0</Size></Contents>"
        b"<Contents><Key>hats/cat/properties</Key><Size>12</Size></Contents>"
        b"</ListBucketResult>"
    )
    assert parse_s3_objects(body) == ({"hats/cat/properties": 12}, None)


def test_parse_s3_listing_reports_a_non_s3_endpoint() -> None:
    with pytest.raises(OSError, match="ListBucketResult"):
        parse_s3_listing(b"<html>login page</html>")


# --------------------------------------------------------------------------- #
# reader surface
# --------------------------------------------------------------------------- #
def test_s3_reader_lists_pages_sizes_and_stages_objects(tmp_path: Path) -> None:
    objects = {
        "hats/cat/properties": b"hats_nrows=1\n",
        "hats/cat/dataset/Norder=0/Dir=0/Npix=5.parquet": b"payload",
        "hats/cat/dataset/Norder=0/Dir=0/Npix=6.parquet": b"payload!",
    }
    with fake_s3(objects, page_size=1) as endpoint:
        remote = S3Remote("s3://catalogue/hats/cat", endpoint=endpoint)
        assert remote.list() == ["dataset/", "properties"]
        assert remote.list("dataset/Norder=0/Dir=0") == ["Npix=5.parquet", "Npix=6.parquet"]
        assert remote.exists("properties")
        assert not remote.exists("missing")
        assert remote.size("dataset/Norder=0/Dir=0/Npix=6.parquet") == 8
        assert remote.size("missing") == -1
        assert remote.read_bytes("properties") == b"hats_nrows=1\n"
        target = tmp_path / "nested" / "props"
        remote.stage_in("properties", target)
        assert target.read_bytes() == b"hats_nrows=1\n"


def test_s3_index_walks_every_object_and_size_across_pages() -> None:
    objects = {
        "hats/cat/properties": b"hats_nrows=1\n",
        "hats/cat/dataset/Norder=0/Dir=0/Npix=5/part0.parquet": b"abc",
        "hats/cat/dataset/Norder=0/Dir=0/Npix=5/part1.parquet": b"abcd",
    }
    with fake_s3(objects, page_size=1) as endpoint:
        index = S3Remote("s3://catalogue/hats/cat", endpoint=endpoint).index()
    assert index == {
        "properties": 13,
        "dataset/Norder=0/Dir=0/Npix=5/part0.parquet": 3,
        "dataset/Norder=0/Dir=0/Npix=5/part1.parquet": 4,
    }


def test_s3_denied_access_is_an_error_not_missing_data() -> None:
    objects = {"hats/cat/properties": b"hats_nrows=1\n"}
    with fake_s3(objects, forbidden=("hats/cat/properties",)) as endpoint:
        remote = S3Remote("s3://catalogue/hats/cat", endpoint=endpoint)
        with pytest.raises(OSError, match="403"):
            remote.exists("properties")
        with pytest.raises(OSError, match="denied anonymous access"):
            remote.size("properties")


def test_s3_reader_quotes_keys_that_need_escaping() -> None:
    remote = S3Remote("s3://bucket/hats/a b", endpoint="https://s3.example.test")
    assert remote.object_url("part 1.parquet") == (
        "https://s3.example.test/bucket/hats/a%20b/part%201.parquet"
    )


# --------------------------------------------------------------------------- #
# collection roots
# --------------------------------------------------------------------------- #
def _minimal_tree(rows: int = 3) -> bytes:
    return f"#HATS catalog\nhats_nrows={rows}\nhats_col_ra=ra\n".encode()


def test_s3_collection_resolves_to_its_primary_table(monkeypatch) -> None:
    objects = {
        "hats/collection.properties": (
            b"hats_primary_table_url=catalogue\n"
            b"all_margins=catalogue_margin_10arcsec\n"
            b"all_indexes=source_id catalogue_index_source_id\n"
        ),
        "hats/catalogue/properties": _minimal_tree(),
        "hats/catalogue_margin_10arcsec/properties": _minimal_tree(),
        "hats/catalogue_index_source_id/properties": _minimal_tree(),
    }
    with fake_s3(objects) as endpoint:
        monkeypatch.setenv("XMATCHER_S3_ENDPOINT", endpoint)
        assert mirror.resolve_hats_tree_root("s3://catalogue/hats") == (
            "s3://catalogue/hats/catalogue"
        )


def test_s3_collection_falls_back_to_the_single_primary_tree(monkeypatch) -> None:
    objects = {
        "hats/catalogue/properties": _minimal_tree(),
        "hats/catalogue_margin_10arcsec/properties": _minimal_tree(),
        "hats/catalogue_index_source_id/properties": _minimal_tree(),
    }
    with fake_s3(objects) as endpoint:
        monkeypatch.setenv("XMATCHER_S3_ENDPOINT", endpoint)
        assert mirror.resolve_hats_tree_root("s3://catalogue/hats") == (
            "s3://catalogue/hats/catalogue"
        )


def test_s3_tree_root_is_returned_unchanged(monkeypatch) -> None:
    objects = {"hats/catalogue/properties": _minimal_tree()}
    with fake_s3(objects) as endpoint:
        monkeypatch.setenv("XMATCHER_S3_ENDPOINT", endpoint)
        assert mirror.resolve_hats_tree_root("s3://catalogue/hats/catalogue") == (
            "s3://catalogue/hats/catalogue"
        )


# --------------------------------------------------------------------------- #
# end-to-end mirror
# --------------------------------------------------------------------------- #
def test_hats_over_s3_mirrors_directory_partitions_and_reads_back(
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

    # Publish the IRSA/LSDB shape: single-file pixels become
    # `Npix=N/part0.snappy.parquet`, and the CSV keeps only Norder/Dir/Npix/count
    # (no file_loc, no Nfiles, no partition_info.parquet).
    (tree / "dataset" / "partition_info.parquet").unlink()
    for pixel in sorted((tree / "dataset").glob("Norder=*/Dir=*/Npix=*.parquet")):
        target = pixel.with_suffix("") / "part0.snappy.parquet"
        target.parent.mkdir(parents=True, exist_ok=True)
        pixel.replace(target)
    for name in ("dataset/partition_info.csv", "partition_info.csv"):
        csv = tree / name
        pl.read_csv(csv).select(["Norder", "Dir", "Npix", "count"]).write_csv(csv)

    objects = _tree_objects(tree, "hats/cat")
    with fake_s3(objects, page_size=2) as endpoint:
        monkeypatch.setenv("XMATCHER_S3_ENDPOINT", endpoint)
        src = CatalogueSource(
            name="s3-hats",
            is_local=False,
            access_method="hats",
            access_identifier="s3://catalogue/hats/cat",
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
