"""Round-2 audit: association identity, verification and component construction."""

from __future__ import annotations

import hashlib
import json
import math
import random
import time
from importlib.resources import files
from pathlib import Path

import polars as pl
import pytest

from xmatcher.association import (
    ASSOCIATION_RELEASE_SCHEMA_VERSION,
    AssociationComponent,
    AssociationComponentMember,
    AssociationComponentReleaseManifest,
    AssociationDecision,
    AssociationProvenance,
    AssociationRecord,
    AssociationReleaseManifest,
    ScoreSemantics,
    _canonical_json,
    _component_release_id,
    _release_id,
    construct_association_components_from_edge_shards,
    iter_association_release,
    load_association_component_release_manifest,
    load_association_release_manifest,
    verify_association_component_release,
    verify_association_release,
    write_association_component_release,
    write_association_release,
)

_RELEASE_ID = f"{ASSOCIATION_RELEASE_SCHEMA_VERSION}:{'a' * 64}"


def _shard_components(tmp_path: Path, edges: list[tuple[str, str]], shards: int) -> list[str]:
    paths = []
    for index in range(shards):
        part = edges[index::shards]
        path = tmp_path / f"shard-{shards}-{index}.parquet"
        pl.DataFrame(
            {"source_id": [s for s, _ in part], "candidate_id": [c for _, c in part]},
            schema={"source_id": pl.String, "candidate_id": pl.String},
        ).write_parquet(path)
        paths.append(path)
    components = construct_association_components_from_edge_shards(
        paths,
        association_release_id=_RELEASE_ID,
        source_input_release_id="cat:a",
        candidate_input_release_id="cat:b",
    )
    return [component.component_id for component in components]


def test_long_chain_components_are_not_quadratic(tmp_path) -> None:
    # Each edge introduces the smallest key, the order that made the previous
    # union-find (min-key roots, broken path compression) walk the whole chain
    # for every member: 8000 nodes took 12 s, 20000 nodes over a minute.
    n = 20_000
    source, candidate = [], []
    for k in range(n, 0, -1):
        source += [f"{k:09d}", f"{k:09d}"]
        candidate += [f"{k:09d}", f"{k + 1:09d}"]
    shard = tmp_path / "chain.parquet"
    pl.DataFrame({"source_id": source, "candidate_id": candidate}).write_parquet(shard)

    start = time.perf_counter()
    components = construct_association_components_from_edge_shards(
        [shard],
        association_release_id=_RELEASE_ID,
        source_input_release_id="cat:a",
        candidate_input_release_id="cat:a",
    )
    elapsed = time.perf_counter() - start

    assert len(components) == 1
    assert len(components[0].members) == n + 1
    assert elapsed < 10


def test_components_do_not_depend_on_edge_order_or_sharding(tmp_path) -> None:
    rng = random.Random(7)
    edges = [(f"s{rng.randrange(300)}", f"c{rng.randrange(300)}") for _ in range(400)]
    expected = _shard_components(tmp_path, edges, 1)
    shuffled = edges[:]
    rng.shuffle(shuffled)
    assert _shard_components(tmp_path, shuffled, 5) == expected
    assert _shard_components(tmp_path, list(reversed(edges)), 3) == expected


def test_edge_shards_keep_isolated_members_without_shards() -> None:
    components = construct_association_components_from_edge_shards(
        [],
        association_release_id=_RELEASE_ID,
        source_input_release_id="cat:a",
        candidate_input_release_id="cat:b",
        additional_members=iter([AssociationComponentMember("cat:a", "solo")]),
    )
    assert [[member.member_id for member in c.members] for c in components] == [["solo"]]


def test_edge_shards_reject_float_ids_and_spell_int64_exactly(tmp_path) -> None:
    big = 2**53 + 1  # not representable as float64
    exact = tmp_path / "int.parquet"
    pl.DataFrame({"source_id": [big], "candidate_id": [7]}).write_parquet(exact)
    components = construct_association_components_from_edge_shards(
        [exact],
        association_release_id=_RELEASE_ID,
        source_input_release_id="cat:a",
        candidate_input_release_id="cat:b",
    )
    assert {m.member_id for m in components[0].members} == {str(big), "7"}

    # Before: a float column was cast to "9007199254740992.0"-style member IDs.
    floats = tmp_path / "float.parquet"
    pl.DataFrame({"source_id": [float(big)], "candidate_id": [7]}).write_parquet(floats)
    with pytest.raises(ValueError, match="string or integer"):
        construct_association_components_from_edge_shards(
            [floats],
            association_release_id=_RELEASE_ID,
            source_input_release_id="cat:a",
            candidate_input_release_id="cat:b",
        )


def _fixture_records() -> list[AssociationRecord]:
    payload = json.loads(
        files("xmatcher").joinpath("fixtures/association_v1.json").read_text(encoding="utf-8")
    )
    return sorted(
        (AssociationRecord.from_mapping(item) for item in payload["records"]),
        key=lambda record: record.association_id,
    )


def _release(tmp_path: Path, name: str = "release") -> Path:
    records = _fixture_records()
    target = tmp_path / name
    write_association_release(records, target, provenance=records[0].provenance)
    return target


def _rewrite_release(target: Path, lines: list[bytes]) -> None:
    """Replace the records and re-derive a self-consistent manifest."""
    payload = b"".join(lines)
    (target / "associations.jsonl").write_bytes(payload)
    old = load_association_release_manifest(target)
    identity = {
        "record_count": len(lines),
        "records_sha256": f"sha256:{hashlib.sha256(payload).hexdigest()}",
        "parent_release_id": old.parent_release_id,
        "provenance": old.provenance,
    }
    manifest = AssociationReleaseManifest(release_id=_release_id(**identity), **identity)  # type: ignore[arg-type]
    (target / "manifest.json").write_text(json.dumps(manifest.to_dict()), encoding="utf-8")


def _flip_byte(lines: list[bytes]) -> list[bytes]:
    line = bytearray(lines[0])
    position = line.index(b'"evidence_id":"') + len(b'"evidence_id":"')
    line[position] ^= 0x01
    return [bytes(line), *lines[1:]]


@pytest.mark.parametrize(
    "tamper",
    [
        _flip_byte,
        lambda lines: lines[1:],
        lambda lines: [lines[1], lines[0], *lines[2:]],
        lambda lines: [*lines, lines[-1]],
        lambda lines: [*lines[:-1], lines[-1].rstrip(b"\n")],
    ],
    ids=["flip-byte", "drop-row", "reorder", "duplicate-row", "truncate-newline"],
)
def test_release_verifier_detects_record_tampering(tmp_path, tamper) -> None:
    target = _release(tmp_path)
    lines = (target / "associations.jsonl").read_bytes().splitlines(keepends=True)
    assert len(lines) >= 3
    (target / "associations.jsonl").write_bytes(b"".join(tamper(lines)))
    with pytest.raises(ValueError):
        verify_association_release(target)


def test_release_verifier_rejects_non_canonical_records_with_consistent_manifest(
    tmp_path,
) -> None:
    # Same records, other JSON spelling, manifest re-derived to match: before
    # the fix this verified as a second release ID for identical content.
    target = _release(tmp_path)
    lines = (target / "associations.jsonl").read_bytes().splitlines(keepends=True)
    spaced = [json.dumps(json.loads(line), sort_keys=True).encode() + b"\n" for line in lines]
    _rewrite_release(target, spaced)
    with pytest.raises(ValueError, match="not canonical"):
        verify_association_release(target)
    with pytest.raises(ValueError, match="not canonical"):
        list(iter_association_release(target))


def test_release_verifier_rejects_unsorted_flags_spelling(tmp_path) -> None:
    target = _release(tmp_path)
    lines = (target / "associations.jsonl").read_bytes().splitlines(keepends=True)
    rewritten = []
    for line in lines:
        data = json.loads(line)
        data["flags"] = list(reversed(data["flags"]))
        rewritten.append(json.dumps(data, sort_keys=True, separators=(",", ":")).encode() + b"\n")
    assert rewritten != lines
    _rewrite_release(target, rewritten)
    with pytest.raises(ValueError, match="not canonical"):
        verify_association_release(target)


def test_release_verifier_checksums_valid_canonical_substitution(tmp_path) -> None:
    records = _fixture_records()
    provenance = records[0].provenance
    first, second = tmp_path / "first", tmp_path / "second"
    write_association_release(records[:-1], first, provenance=provenance)
    write_association_release(records[1:], second, provenance=provenance)
    (first / "associations.jsonl").write_bytes((second / "associations.jsonl").read_bytes())
    with pytest.raises(ValueError, match="records_sha256"):
        verify_association_release(first)


def test_consistently_re_derived_subset_is_a_different_release(tmp_path) -> None:
    target = _release(tmp_path)
    original = load_association_release_manifest(target).release_id
    lines = (target / "associations.jsonl").read_bytes().splitlines(keepends=True)
    _rewrite_release(target, lines[1:])
    assert verify_association_release(target).release_id != original


def test_membership_verifier_rejects_non_canonical_rows(tmp_path) -> None:
    association = _release(tmp_path, "association")
    manifest = load_association_release_manifest(association)
    left, right = manifest.provenance.input_release_ids
    component = AssociationComponent(
        manifest.release_id,
        (AssociationComponentMember(left, "a"), AssociationComponentMember(right, "b")),
    )
    target = tmp_path / "components"
    write_association_component_release([component], association, target)
    path = target / "memberships.jsonl"
    lines = path.read_bytes().splitlines(keepends=True)
    spaced = b"".join(
        json.dumps(json.loads(line), sort_keys=True).encode() + b"\n" for line in lines
    )
    path.write_bytes(spaced)
    old = load_association_component_release_manifest(target)
    identity = {
        "association_release_id": old.association_release_id,
        "component_count": old.component_count,
        "membership_count": old.membership_count,
        "memberships_sha256": f"sha256:{hashlib.sha256(spaced).hexdigest()}",
        "parent_component_release_id": None,
        "provenance": old.provenance,
    }
    rewritten = AssociationComponentReleaseManifest(
        component_release_id=_component_release_id(**identity),
        **identity,  # type: ignore[arg-type]
    )
    (target / "manifest.json").write_text(json.dumps(rewritten.to_dict()), encoding="utf-8")
    with pytest.raises(ValueError, match="not canonical"):
        verify_association_component_release(target)


def _record(**changes: object) -> AssociationRecord:
    fields: dict[str, object] = {
        "evidence_id": "évidence:1",
        "source_id": 'source:é中"\\\nx',
        "candidate_id": "candidate:9007199254740993",
        "method": "sky",
        "method_version": "1",
        "separation_arcsec": 0.1 + 0.2,
        "evaluation_epoch_jyear": 2016,
        "decision": AssociationDecision.SELECTED,
        "score_name": "sep",
        "score_value": 1e-300,
        "score_semantics": ScoreSemantics.RANKING_SCORE,
        "calibration_id": None,
        "flags": ("z", "crowded"),
        "provenance": AssociationProvenance(
            ("cat:b", "cat:a"), "xmatcher 0.7.0", "sha256:" + "ab" * 32
        ),
    }
    fields.update(changes)
    return AssociationRecord(**fields)  # type: ignore[arg-type]


def test_content_ids_are_pinned() -> None:
    # Golden values: any change to canonical encoding (key order, float
    # spelling, unicode escaping, int handling) breaks published IDs.
    assert _record().association_id == (
        "xmatch.association.v1:9c10d921e36175d378a7ffe65df540b236e304177c95d77dc303f29556f105ed"
    )
    assert _record(evaluation_epoch_jyear=2016.0).association_id == _record().association_id
    component = AssociationComponent(
        f"{ASSOCIATION_RELEASE_SCHEMA_VERSION}:{'c' * 64}",
        (
            AssociationComponentMember("cat:b", "中"),
            AssociationComponentMember("cat:a", "9007199254740993"),
        ),
    )
    assert component.component_id == (
        "xmatch.association.component.v1:"
        "dbf095beb4c69886fbda7efb9e81e806fb242325b410d44bb413c29732aeab8f"
    )


def test_negative_zero_has_one_canonical_spelling() -> None:
    negative = _record(separation_arcsec=-0.0, score_value=-0.0)
    positive = _record(separation_arcsec=0.0, score_value=0.0)
    assert _canonical_json(negative.to_dict()) == _canonical_json(positive.to_dict())
    assert math.copysign(1.0, negative.separation_arcsec) == 1.0


def test_huge_integer_score_is_a_value_error() -> None:
    data = _record().to_dict()
    data["score"]["value"] = 10**400
    with pytest.raises(ValueError, match="finite number"):
        AssociationRecord.from_mapping(data)
