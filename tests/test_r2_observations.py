"""Round-2 audit: observation identity hashing and release verification."""

from __future__ import annotations

import hashlib
import json

import polars as pl
import pytest

from xmatcher import CrossMatch
from xmatcher.observations import (
    _digest,
    namespaced_source_id,
    normalize_photometry,
    normalize_property_evidence,
    source_inventory,
    verify_observation_release,
    write_observation_release,
)

_TEXT_IDS = [
    "17",
    "a:b",
    'quote"back\\slash',
    "tab\tinside",
    "é",
    "中文",
    "sep\u2028\u2029x",
    "emoji\U0001f600",
    "nul\x00inside",
    "é",  # NFD spelling stays distinct from NFC "é"
]
_INT_IDS = [0, -1, 1, 2**53 + 1, 2**63 - 1, -(2**63)]


def _source(frame: pl.DataFrame | pl.LazyFrame, **metadata: object):
    return CrossMatch().resolve_source(
        frame,
        {"id_column": "id", "release_namespace": 'ns:"é"\\1', **metadata},
    )


@pytest.mark.parametrize("values", [_TEXT_IDS, _INT_IDS], ids=["text", "int"])
def test_batched_source_ids_equal_the_reference_definition(values) -> None:
    source = _source(pl.DataFrame({"id": values}))
    ids = source_inventory(source).collect(engine="streaming")["source_id"].to_list()
    assert ids == [namespaced_source_id('ns:"é"\\1', value) for value in values]


def test_source_ids_are_pinned() -> None:
    assert namespaced_source_id("ns:1", 9007199254740993) == (
        "xmatch.source.v1:44bab4f0bfee791ab95d148364d85823da0264e38b9c3ea5f8d67536d72cd967"
    )
    assert namespaced_source_id("ns:1", 'é"x') == (
        "xmatch.source.v1:71c576d9b81f1e0eba73a20c86a67411e905a244d94f766db2fe05639e528afa"
    )


def test_unsigned_and_narrow_integer_ids_hash_like_int64() -> None:
    wide = source_inventory(_source(pl.DataFrame({"id": [7, 2**30]}))).collect()
    for dtype in (pl.UInt64, pl.Int32, pl.UInt32):
        narrow = source_inventory(
            _source(pl.DataFrame({"id": pl.Series([7, 2**30], dtype=dtype)}))
        ).collect()
        assert narrow["source_id"].to_list() == wide["source_id"].to_list()


def test_measurement_ids_equal_the_reference_definition() -> None:
    mapping = {"passband": "s:g", "value_column": "g", "unit": "uJy"}
    evidence = {"property": "z", "value_column": "g", "category": "measured", "method": "m"}
    source = _source(
        pl.DataFrame({"id": [3, 1, 2], "g": [1.0, 2.0, None]}),
        photometry=[mapping],
        property_evidence=[evidence],
    )
    photometry = normalize_photometry(source).collect()
    expected = [
        f"xmatch.photometry.v1:{_digest([sid, _digest(mapping)])}"
        for sid in photometry["source_id"]
    ]
    assert photometry["measurement_id"].to_list() == expected
    rows = normalize_property_evidence(source).collect()
    assert rows["evidence_id"].to_list() == [
        f"xmatch.property.evidence.v1:{_digest([sid, _digest(evidence)])}"
        for sid in rows["source_id"]
    ]


def test_identity_is_invariant_to_row_order_and_chunking() -> None:
    n = 5000
    frame = pl.DataFrame({"id": list(range(n)), "g": [float(i) for i in range(n)]})
    mapping = {"passband": "s:g", "value_column": "g", "unit": "Jy"}
    reference = normalize_photometry(_source(frame, photometry=[mapping])).collect()
    chunked = pl.concat([frame.slice(i, 97) for i in range(0, n, 97)], rechunk=False)
    shuffled = chunked.sample(fraction=1.0, shuffle=True, seed=3)
    other = (
        normalize_photometry(_source(shuffled.lazy(), photometry=[mapping]))
        .collect(engine="streaming")
        .sort("native_value")
    )
    assert other.select("source_id", "measurement_id").equals(
        reference.select("source_id", "measurement_id")
    )


def test_release_verification_detects_shard_tampering(tmp_path) -> None:
    source = _source(
        pl.DataFrame({"id": [1, 2, 3], "g": [1.0, 2.0, 3.0]}),
        photometry=[{"passband": "s:g", "value_column": "g", "unit": "Jy"}],
    )
    target = tmp_path / "release"
    manifest = write_observation_release(target, [source], release_id="r:1", provenance={})
    shard = target / manifest["sources"][0]["photometry_path"]
    original = shard.read_bytes()

    # Reordered rows are other bytes, even with identical content.
    frame = pl.read_parquet(shard)
    frame.reverse().write_parquet(shard)
    with pytest.raises(ValueError, match="checksum"):
        verify_observation_release(target)

    # Dropping a row and recording its new checksum still trips the row count.
    frame.head(2).write_parquet(shard)
    entry = json.loads((target / "manifest.json").read_text())
    entry["sources"][0]["photometry_sha256"] = (
        f"sha256:{hashlib.sha256(shard.read_bytes()).hexdigest()}"
    )
    (target / "manifest.json").write_text(json.dumps(entry))
    with pytest.raises(ValueError, match="count"):
        verify_observation_release(target)

    shard.write_bytes(original)
    (target / "manifest.json").write_text(json.dumps(manifest))
    assert verify_observation_release(target) == manifest
