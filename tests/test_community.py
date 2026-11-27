"""Read-only community HATS roots, ``local_paths`` and the write guard rails."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import polars as pl
import pytest

from xmatcher import community, hats_registry, mirror, storage
from xmatcher.community import community_index, draft_hats_entry, find_read_only_copy
from xmatcher.exceptions import CrossMatchError, ReadOnlyRootError
from xmatcher.mirror import _version_dir, _write_hats_native, ensure_mirrored, locate_mirrored
from xmatcher.sources import CatalogueSource
from xmatcher.storage import (
    LocalStorage,
    all_cache_roots,
    assert_headroom,
    assert_writable,
    community_roots,
    read_only_root_for,
    replica_roots,
)

URL = "https://example.invalid/hats/two_mass/two_mass"


def _snapshot(root: Path) -> dict[str, tuple[int, int]]:
    """Every path under ``root`` with (mtime_ns, size): detects any write."""
    out: dict[str, tuple[int, int]] = {}
    for path in [root, *root.rglob("*")]:
        st = path.stat()
        out[str(path.relative_to(root))] = (st.st_mtime_ns, st.st_size if path.is_file() else 0)
    return out


def _remote_src(**kwargs) -> CatalogueSource:
    base = {
        "name": "twomass_psc_hats",
        "is_local": False,
        "access_method": "hats",
        "access_identifier": URL,
        "ra_column": "ra",
        "dec_column": "decl",
        "id_column": "designation",
        "epoch": 2000.0,
        "default_columns": ["designation", "ra", "decl", "j_m"],
    }
    base.update(kwargs)
    return CatalogueSource(**base)


def _fake_tree(
    path: Path,
    *,
    nrows: int = 3,
    obs: str = "two_mass",
    ra: str = "ra",
    dec: str = "decl",
    real: bool = False,
) -> Path:
    if real:
        frame = pl.DataFrame(
            {
                "designation": ["a", "b", "c"],
                ra: [10.0, 10.0002, 20.0],
                dec: [0.0, 0.0, 0.0],
                "j_m": [12.0, 13.0, 14.0],
            }
        )
        _write_hats_native(frame, path, ra_column=ra, dec_column=dec)
        text = (path / "properties").read_text()
        lines = [ln for ln in text.splitlines() if not ln.startswith("obs_collection")]
        (path / "properties").write_text("\n".join([*lines, f"obs_collection={obs}"]) + "\n")
        return path
    path.mkdir(parents=True)
    (path / "properties").write_text(
        f"obs_collection={obs}\nhats_nrows={nrows}\nhats_col_ra={ra}\nhats_col_dec={dec}\n"
    )
    (path / "partition_info.csv").write_text("Norder,Npix\n0,0\n")
    return path


def _sync_record(
    tree: Path,
    *,
    url: str = URL,
    version: str | None = None,
    record_name: str = "sync.json",
    **extra,
) -> None:
    src = _remote_src(access_identifier=url)
    record = {
        "fetched_at": "2026-11-11T20:07:15Z",
        "source": url,
        "version": version or _version_dir(src),
        "files": {
            "properties": {"size": (tree / "properties").stat().st_size},
            "partition_info.csv": {"size": (tree / "partition_info.csv").stat().st_size},
        },
    }
    record.update(extra)
    (tree / record_name).write_text(json.dumps(record))


@pytest.fixture()
def community_root(tmp_path: Path, monkeypatch) -> Path:
    root = tmp_path / "community"
    root.mkdir()
    monkeypatch.setenv("XMATCHER_COMMUNITY_ROOTS", str(root))
    return root


# --------------------------------------------------------------------------- #
# A. community resolution
# --------------------------------------------------------------------------- #
def test_matching_community_tree_is_used_in_place_and_never_written(
    tmp_path: Path, community_root: Path
) -> None:
    tree = _fake_tree(community_root / "2mass" / "psc", nrows=3, real=True)
    _sync_record(tree)
    before = _snapshot(community_root)
    cache = tmp_path / "cache"

    src = _remote_src(expected_hats_nrows=3, expected_obs_collection="two_mass")
    resolved = ensure_mirrored(src, cache_root=str(cache), read_roots=[str(community_root)])

    assert resolved.path == tree
    assert resolved.access_identifier == str(tree)
    assert resolved.read_only_origin == "community"
    # the catalogue entry's metadata is kept
    assert resolved.dec_column == "decl"
    assert resolved.epoch == 2000.0
    assert resolved.default_columns == ["designation", "ra", "decl", "j_m"]
    # no file was created or touched under the community root ...
    assert _snapshot(community_root) == before
    # ... the index lives under the writable cache root, and nothing was mirrored
    assert (cache / "registry" / "community_index.json").is_file()
    assert not (cache / "twomass_psc_hats").exists()

    # --no-sync readers find the same copy
    root, rel = locate_mirrored(src, cache_roots=[str(cache)], read_roots=[str(community_root)])
    assert (root, rel) == (str(tree), "")
    # sync is a no-op
    stats = mirror.sync_catalogue(src, cache_root=str(cache), read_roots=[str(community_root)])
    assert stats.files_downloaded == 0
    assert _snapshot(community_root) == before


@pytest.mark.parametrize(
    "case",
    ["wrong_version", "wrong_url", "no_fetched_at", "no_properties", "nrows", "obs", "size", "dec"],
)
def test_mismatched_or_incomplete_community_tree_falls_back(
    tmp_path: Path, community_root: Path, case: str
) -> None:
    tree = _fake_tree(community_root / "2mass" / "psc", nrows=3)
    extra: dict = {}
    if case == "wrong_version":
        extra["version"] = "hats-http-000000000000"
    if case == "wrong_url":
        extra["url"] = "https://example.invalid/other"
    _sync_record(tree, **extra)
    if case == "no_fetched_at":
        data = json.loads((tree / "sync.json").read_text())
        data.pop("fetched_at")
        (tree / "sync.json").write_text(json.dumps(data))
    if case == "size":
        (tree / "partition_info.csv").write_text("Norder,Npix\n0,0\n0,1\n")
    src = _remote_src(expected_hats_nrows=3, expected_obs_collection="two_mass")
    if case == "no_properties":
        (tree / "properties").rename(tree / "properties.bak")
        (tree / "sync.json").write_text(
            json.dumps({"source": URL, "version": _version_dir(src), "fetched_at": "x"})
        )
        # a tree without properties is not even a tree for the index walk
    if case == "nrows":
        src = _remote_src(expected_hats_nrows=4)
    if case == "obs":
        src = _remote_src(expected_obs_collection="allwise")
    if case == "dec":
        src = _remote_src(dec_column="dec")

    assert (
        find_read_only_copy(src, read_roots=[str(community_root)], cache_root=str(tmp_path / "c"))
        is None
    )
    # the writable cache is probed next: nothing there yet -> actionable error
    with pytest.raises(CrossMatchError, match="not mirrored yet"):
        locate_mirrored(src, cache_roots=[str(tmp_path / "c")], read_roots=[str(community_root)])
    # with a mirrored copy in the writable cache, that copy is used instead
    cache = tmp_path / "c"
    rel = f"{mirror._safe_name(src.name)}/{_version_dir(src)}"
    (cache / rel).mkdir(parents=True)
    (cache / rel / "properties").write_text("hats_nrows=3\n")
    (cache / rel / "sync.json").write_text(json.dumps({"fetched_at": "2026-01-01T00:00:00Z"}))
    assert locate_mirrored(src, cache_roots=[str(cache)], read_roots=[str(community_root)]) == (
        str(cache),
        rel,
    )


def test_xmatcher_sync_json_records_are_indexed(tmp_path: Path, community_root: Path) -> None:
    tree = _fake_tree(community_root / "survey" / "product")
    src = _remote_src()
    (tree / "sync.json").write_text(
        json.dumps(
            {"source": URL, "version": _version_dir(src), "fetched_at": "2026-01-01T00:00:00Z"}
        )
    )
    copy = find_read_only_copy(src, read_roots=[str(community_root)], cache_root=str(tmp_path))
    assert copy is not None and copy.path == tree and copy.origin == "community"


def test_legacy_pensus_sync_json_records_are_indexed(tmp_path: Path, community_root: Path) -> None:
    # Legacy on-disk name still present on some community HATS trees;
    # pensus product code moved to sfabbro/pensus.
    tree = _fake_tree(community_root / "2mass" / "psc", nrows=3)
    _sync_record(tree, record_name="PENSUS_SYNC.json")
    src = _remote_src(expected_hats_nrows=3, expected_obs_collection="two_mass")
    copy = find_read_only_copy(src, read_roots=[str(community_root)], cache_root=str(tmp_path))
    assert copy is not None and copy.path == tree and copy.origin == "community"


def test_community_index_ttl_and_invalidation(
    tmp_path: Path, community_root: Path, monkeypatch
) -> None:
    _fake_tree(community_root / "2mass" / "psc")
    _sync_record(community_root / "2mass" / "psc")
    calls: list[str] = []
    real_scan = community.scan_root

    def counting(root, **kwargs):
        calls.append(str(root))
        return real_scan(root, **kwargs)

    monkeypatch.setattr(community, "scan_root", counting)
    cache = str(tmp_path / "cache")
    assert len(community_index([str(community_root)], cache_root=cache)) == 1
    assert len(community_index([str(community_root)], cache_root=cache)) == 1
    assert len(calls) == 1  # second call served from the cached index
    # a new survey directory changes the signature -> rescan before the TTL
    tree = _fake_tree(community_root / "allwise" / "catalog")
    _sync_record(tree, url="https://example.invalid/allwise")
    os.utime(community_root, ns=(1, 1))
    assert len(community_index([str(community_root)], cache_root=cache)) == 2
    assert len(calls) == 2
    # an expired TTL rescans even without a change
    community_index([str(community_root)], cache_root=cache, ttl_s=0.0)
    assert len(calls) == 3


# --------------------------------------------------------------------------- #
# B. local_paths
# --------------------------------------------------------------------------- #
def _config_with_entry(tmp_path: Path, entry: dict) -> Path:
    import yaml

    from xmatcher.user_config import bundled_config_path, load_yaml_mapping

    bundled = load_yaml_mapping(bundled_config_path())
    cfg = {
        "archives": bundled["archives"],
        "catalogues": {"local_cat": entry},
    }
    path = tmp_path / "cfg.yaml"
    path.write_text(yaml.safe_dump(cfg))
    return path


def test_local_paths_verified_and_metadata_kept(tmp_path: Path, monkeypatch) -> None:
    from xmatcher.crossmatch import CrossMatch

    monkeypatch.setenv("XMATCHER_COMMUNITY_ROOTS", "")
    good = _fake_tree(tmp_path / "trees" / "good", nrows=3, real=True)
    bad = _fake_tree(tmp_path / "trees" / "bad", nrows=99)
    entry = {
        "archive": "hats",
        "service_id": "hats_service",
        "access_identifier": "https://example.invalid/hats/local_cat",
        "local_paths": [str(tmp_path / "missing"), str(bad), str(good)],
        "hats_nrows": 3,
        "obs_collection": "two_mass",
        "ra_column": "ra",
        "dec_column": "decl",
        "id_column": "designation",
        "epoch": 2000.0,
        "pm_ra_column": "pmra",
        "pm_dec_column": "pmdec",
        "default_columns": ["designation", "ra", "decl", "j_m"],
    }
    cm = CrossMatch(config_file=_config_with_entry(tmp_path, entry))
    src = cm.resolve_source("local_cat", {})
    assert src.local_paths[-1] == str(good)
    assert src.expected_hats_nrows == 3
    before = _snapshot(tmp_path / "trees")
    resolved = ensure_mirrored(src, cache_root=str(tmp_path / "cache"), read_roots=[])
    assert resolved.path == good and resolved.read_only_origin == "local_paths"
    assert (resolved.epoch, resolved.pm_ra_column, resolved.id_column) == (
        2000.0,
        "pmra",
        "designation",
    )
    assert _snapshot(tmp_path / "trees") == before

    # every local path failing verification -> fallback to the writable cache
    src.local_paths = [str(bad)]
    with pytest.raises(CrossMatchError, match="not mirrored yet"):
        locate_mirrored(src, cache_roots=[str(tmp_path / "cache")], read_roots=[])


def test_local_paths_cover_an_unset_environment_variable(tmp_path: Path, monkeypatch) -> None:
    from xmatcher.crossmatch import CrossMatch

    monkeypatch.delenv("XMATCHER_TEST_UNSET_TREE", raising=False)
    tree = _fake_tree(tmp_path / "tree")
    entry = {
        "archive": "hats",
        "service_id": "hats_service",
        "access_identifier": "${XMATCHER_TEST_UNSET_TREE}",
        "local_paths": [str(tree)],
        "ra_column": "ra",
        "dec_column": "decl",
    }
    cm = CrossMatch(config_file=_config_with_entry(tmp_path, entry))
    assert cm.resolve_source("local_cat", {}).access_identifier == str(tree)


def test_invalid_local_paths_rejected(tmp_path: Path) -> None:
    from xmatcher.crossmatch import CrossMatch
    from xmatcher.exceptions import ConfigError

    entry = {
        "archive": "hats",
        "service_id": "hats_service",
        "access_identifier": "https://example.invalid/x",
        "local_paths": [3],
        "ra_column": "ra",
        "dec_column": "dec",
    }
    with pytest.raises(ConfigError, match="local_paths"):
        CrossMatch(config_file=_config_with_entry(tmp_path, entry))


def test_community_tree_is_not_self_mirrored_on_the_platform(tmp_path: Path, monkeypatch) -> None:
    real_isdir = os.path.isdir
    monkeypatch.setattr(
        os.path,
        "isdir",
        lambda p: True if str(p) == storage.ARC_RELEASE_ROOT else real_isdir(p),
    )
    src = _remote_src(
        access_identifier="vos://cadc.nrc.ca~arc/projects/hats/vhs/dr7",
        local_paths=[str(tmp_path / "does-not-exist")],
    )
    with pytest.raises(CrossMatchError, match="refusing to copy the community tree"):
        mirror.sync_catalogue(src, cache_root=str(tmp_path / "cache"), read_roots=[])


# --------------------------------------------------------------------------- #
# E. read-only guard rails + replica/community split
# --------------------------------------------------------------------------- #
def test_every_write_path_refuses_a_read_only_root(
    tmp_path: Path, community_root: Path, monkeypatch
) -> None:
    tree = _fake_tree(community_root / "2mass" / "psc")
    before = _snapshot(community_root)
    store = LocalStorage(tree)  # reading is fine
    assert store.exists("properties")
    frame = pl.DataFrame({"a": [1]})
    local = tmp_path / "x.parquet"
    frame.write_parquet(local)
    for call in (
        lambda: store.write_parquet(frame, "new.parquet"),
        lambda: store.mkdir("newdir"),
        lambda: store.stage_out(local, "copied.parquet"),
        lambda: store.rename("properties", "moved"),
        lambda: store.rm("properties"),
        lambda: LocalStorage(community_root / "brand" / "new"),
        lambda: assert_headroom(community_root / "sub", 0.0, "cache root"),
        lambda: mirror.sync_catalogue(_remote_src(), cache_root=str(community_root / "cache")),
        lambda: mirror.sync_catalogue(
            _remote_src(),
            cache_root=str(tmp_path / "cache"),
            replica_roots=[str(community_root / "replica")],
        ),
        lambda: ensure_mirrored(_remote_src(), cache_root=str(community_root / "cache")),
        lambda: hats_registry._write_cache(community_root / "registry" / "r.json", []),
        lambda: all_cache_roots({"root": str(community_root / "cache")}),
        lambda: all_cache_roots({"roots": [str(community_root / "replica")]}),
        lambda: assert_writable(community_root / "out.hats", "output directory"),
    ):
        with pytest.raises(ReadOnlyRootError):
            call()
    assert _snapshot(community_root) == before


def test_replication_skips_a_read_only_replica(
    tmp_path: Path, community_root: Path, caplog
) -> None:
    cache = LocalStorage(tmp_path / "cache")
    src = _remote_src()
    rel = f"{mirror._safe_name(src.name)}/{_version_dir(src)}"
    cache.mkdir(rel)
    (tmp_path / "cache" / rel / "properties").write_text("hats_nrows=1\n")
    before = _snapshot(community_root)
    mirror._replicate_to_roots(cache, src, [str(community_root / "replica")])
    assert _snapshot(community_root) == before
    assert "read-only" in caplog.text


def test_platform_community_root_is_always_protected(monkeypatch) -> None:
    monkeypatch.setenv("XMATCHER_COMMUNITY_ROOTS", "")
    assert read_only_root_for("/arc/projects/hats/2mass/psc") == "/arc/projects/hats"
    assert read_only_root_for("/arc/projects/astroai/hats/x") is None
    assert read_only_root_for("vos://cadc.nrc.ca~arc/projects/hats/vhs/dr7") is not None
    assert read_only_root_for("vos:projects/hats/x") is not None
    assert read_only_root_for("vos:projects/astroai/hats") is None
    with pytest.raises(ReadOnlyRootError):
        assert_writable("/arc/projects/hats/xmatcher", "output directory")


def test_cache_roots_backward_compatible_and_split(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("XMATCHER_CACHE_ROOT", raising=False)
    monkeypatch.setenv("XMATCHER_COMMUNITY_ROOTS", str(tmp_path / "community"))
    a, b, c = (str(tmp_path / n) for n in "abc")
    assert all_cache_roots({"root": a, "roots": [b]}) == [a, b]
    assert all_cache_roots({"root": a, "replica_roots": [c], "roots": [b, c]}) == [a, c, b]
    assert replica_roots({"roots": b}) == [b]
    # community roots never become cache roots
    assert str(tmp_path / "community") not in all_cache_roots({"root": a})


def test_mounted_arc_does_not_choose_output_or_cache_roots(monkeypatch, tmp_path: Path) -> None:
    real_isdir = os.path.isdir
    monkeypatch.setattr(
        os.path,
        "isdir",
        lambda p: True if str(p) in ("/arc", "/arc/projects/hats") else real_isdir(p),
    )
    assert community_roots() == []
    assert storage.default_cache_root() == str(Path.home() / ".cache" / "xmatcher")
    assert storage.default_output_root() is None
    assert community_roots({"community_roots": [str(tmp_path)]}) == [str(tmp_path)]
    monkeypatch.setenv("XMATCHER_COMMUNITY_ROOTS", "")
    assert community_roots({"community_roots": [str(tmp_path)]}) == []


# --------------------------------------------------------------------------- #
# D. adopt hats
# --------------------------------------------------------------------------- #
def test_adopt_hats_dry_run(tmp_path: Path, monkeypatch, capsys) -> None:
    from xmatcher.cli import main

    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setattr(Path, "home", lambda: tmp_path / "home")
    tree = _fake_tree(tmp_path / "2mass" / "psc", real=True)
    _sync_record(tree)
    rc = main(["adopt", "hats", str(tree), "--name", "my_2mass", "--dry-run"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "my_2mass:" in out
    assert f"access_identifier: {URL}" in out
    assert str(tree) in out  # local_paths
    assert "dec_column: decl" in out
    assert "hats_nrows: 3" in out
    assert "id_column: designation" in out
    assert not (tmp_path / "home" / ".config" / "xmatcher" / "xmatcher.yaml").exists()


def test_draft_hats_entry_reads_schema_columns(tmp_path: Path) -> None:
    frame = pl.DataFrame(
        {
            "source_id": [1, 2],
            "ra": [1.0, 2.0],
            "dec": [0.0, 0.0],
            "ref_epoch": [2016.0, 2016.0],
            "pmra": [1.0, 2.0],
            "pmdec": [1.0, 2.0],
            "parallax": [1.0, 1.0],
            "ra_error": [0.1, 0.1],
            "dec_error": [0.1, 0.1],
        }
    )
    _write_hats_native(frame, tmp_path / "gaia", ra_column="ra", dec_column="dec")
    name, entry, notes = draft_hats_entry(str(tmp_path / "gaia"))
    assert entry["access_identifier"] == str((tmp_path / "gaia").resolve())
    assert entry["id_column"] == "source_id"
    assert entry["epoch_column"] == "ref_epoch"
    assert (entry["pm_ra_column"], entry["parallax_column"]) == ("pmra", "parallax")
    assert entry["ra_err_column"] == "ra_error" and entry["pos_err_units"] == "mas"
    assert any("Julian" in n for n in notes)


# --------------------------------------------------------------------------- #
# end to end: a ray-anchor survey read from a community tree
# --------------------------------------------------------------------------- #
def test_anchor_reads_survey_from_community_tree(
    tmp_path: Path, community_root: Path, monkeypatch
) -> None:
    from xmatcher.ray_anchor import ray_anchor_match

    monkeypatch.setitem(sys.modules, "ray", None)
    tree = _fake_tree(community_root / "2mass" / "psc", real=True)
    _sync_record(tree)
    anchor_frame = pl.DataFrame({"id": [1, 2], "ra": [10.0, 20.0], "dec": [0.0, 0.0]})
    _write_hats_native(anchor_frame, tmp_path / "anchor", ra_column="ra", dec_column="dec")
    anchor = CatalogueSource(
        name="anchor",
        is_local=True,
        path=tmp_path / "anchor",
        access_method="hats",
        ra_column="ra",
        dec_column="dec",
        id_column="id",
    )
    before = _snapshot(community_root)
    survey = ensure_mirrored(
        _remote_src(expected_obs_collection="two_mass"),
        cache_root=str(tmp_path / "cache"),
        read_roots=[str(community_root)],
    )
    out = tmp_path / "out"
    ray_anchor_match(anchor, [survey], sep_arcsec=1.5, output_file=str(out))
    result = pl.concat(
        [pl.read_parquet(p) for p in (out / "dataset").rglob("Npix=*.parquet")]
    ).sort("source_id")
    assert result["j_m_twomass_psc_hats"].to_list() == [12.0, 14.0]
    assert _snapshot(community_root) == before
