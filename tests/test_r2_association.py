"""Round-2 audit: association identity, verification and component construction."""

from __future__ import annotations

import random
import time
from pathlib import Path

import polars as pl

from xmatcher.association import (
    ASSOCIATION_RELEASE_SCHEMA_VERSION,
    AssociationComponentMember,
    construct_association_components_from_edge_shards,
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
