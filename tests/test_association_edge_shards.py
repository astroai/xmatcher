"""Sharded association construction for pensus tile edge artifacts."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from xmatcher.association import (
    ASSOCIATION_RELEASE_SCHEMA_VERSION,
    AssociationComponentMember,
    construct_association_components_from_edge_shards,
)


def test_construct_from_edge_shards(tmp_path: Path):
    s1 = tmp_path / "t0.parquet"
    s2 = tmp_path / "t1.parquet"
    pl.DataFrame({"source_id": ["a", "b"], "candidate_id": ["1", "2"]}).write_parquet(s1)
    pl.DataFrame({"source_id": ["b", "c"], "candidate_id": ["2", "3"]}).write_parquet(s2)

    release_id = f"{ASSOCIATION_RELEASE_SCHEMA_VERSION}:{'a' * 64}"
    components = construct_association_components_from_edge_shards(
        [s1, s2],
        association_release_id=release_id,
        source_input_release_id="gaia",
        candidate_input_release_id="ps1",
        additional_members=[AssociationComponentMember("gaia", "orphan")],
    )
    # a-1 ; b-2 ; c-3 ; orphan  → b links shards so still separate pairs + orphan
    member_sets = [set(m.member_id for m in c.members) for c in components]
    assert {"a", "1"} in member_sets
    assert {"b", "2"} in member_sets
    assert {"c", "3"} in member_sets
    assert {"orphan"} in member_sets
