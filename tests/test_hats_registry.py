"""Tests for ``hats:<short_name>`` registry resolution."""

from __future__ import annotations

import json
import time

import pytest

from xmatcher import hats_registry
from xmatcher.exceptions import InputError

RECORDS = [
    {
        "short_name": "two_mass",
        "title": "2MASS Point Source Catalog HATS",
        "url": "https://data.lsdb.io/hats/two_mass/",
    },
    {
        "short_name": "gaia_dr3",
        "title": "Gaia DR3 HATS",
        "url": "https://data.lsdb.io/hats/gaia_dr3",
    },
    {
        "short_name": "gaia_dr3_uw",
        "title": "Gaia DR3 HATS (UW)",
        "url": "https://data.lsdb.io/hats/gaia_dr3",
    },
    {
        "short_name": "gaia_dr3_stsci",
        "title": "Gaia DR3 HATS (STScI)",
        "url": "s3://stpubdata/gaia/gaia_dr3/public/hats",
        "http_mirror": "",
    },
    {
        "short_name": "ztf_dr23_obje",
        "title": "IRSA ZTF DR23 Objects HATS",
        "url": "s3://ipac-irsa-ztf/contributed/dr23/objects/hats",
    },
    {
        "short_name": "mmu_jwst_gds",
        "title": "MMU JWST GDS",
        "url": "hf://datasets/UniverseTBD/mmu_jwst_gds",
    },
    {
        "short_name": "mmu_jwst_primer_cosmos",
        "title": "MMU JWST PRIMER COSMOS",
        "url": "hf://datasets/UniverseTBD/mmu_jwst_primer_cosmos",
    },
    {
        "short_name": "mmu_jwst_primer_uds",
        "title": "MMU JWST PRIMER UDS",
        "url": "hf://datasets/UniverseTBD/mmu_jwst_primer_uds",
    },
    {
        "short_name": "legacy_ivo_copy",
        "title": "Legacy IVOA-only HATS copy",
        "url": "ivo://example.org/hats/legacy",
    },
]


@pytest.fixture(autouse=True)
def cache_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("XMATCHER_CACHE_ROOT", str(tmp_path / "cache"))
    return tmp_path / "cache"


def _write_snapshot(path, records=RECORDS, *, age_seconds: float = 0.0) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "version": hats_registry._CACHE_VERSION,
                "fetched_at": time.time() - age_seconds,
                "records": records,
            }
        )
    )


def test_resolve_uses_a_fresh_cache_without_touching_the_network(monkeypatch):
    _write_snapshot(hats_registry.cache_file(), age_seconds=5)

    def fail():
        raise AssertionError("registry must not be queried for a fresh snapshot")

    monkeypatch.setattr(hats_registry, "_registry_records", fail)
    assert (
        hats_registry.resolve_hats_short_name("two_mass") == "https://data.lsdb.io/hats/two_mass/"
    )


def test_resolve_is_case_insensitive_and_matches_titles(monkeypatch):
    monkeypatch.setattr(hats_registry, "_registry_records", lambda: RECORDS)
    assert hats_registry.resolve_hats_short_name("GAIA_DR3").endswith("gaia_dr3")
    assert hats_registry.resolve_hats_short_name("gaia_dr3_uw").endswith("gaia_dr3")
    assert (
        hats_registry.resolve_hats_short_name("2MASS Point Source Catalog HATS")
        == "https://data.lsdb.io/hats/two_mass/"
    )


def test_resolve_reports_ambiguous_prefixes(monkeypatch):
    monkeypatch.setattr(hats_registry, "_registry_records", lambda: RECORDS)
    with pytest.raises(InputError) as excinfo:
        hats_registry.resolve_hats_short_name("mmu_jwst")
    message = str(excinfo.value)
    assert "matches 3 registered HATS catalogues" in message
    assert "mmu_jwst_gds" in message


def test_resolve_returns_s3_registered_trees(monkeypatch):
    """S3-only registrations resolve: the mirror reads public buckets anonymously."""
    monkeypatch.setattr(hats_registry, "_registry_records", lambda: RECORDS)
    assert hats_registry.resolve_hats_short_name("ztf_dr23_obje") == (
        "s3://ipac-irsa-ztf/contributed/dr23/objects/hats"
    )
    assert hats_registry.resolve_hats_short_name("gaia_dr3_stsci") == (
        "s3://stpubdata/gaia/gaia_dr3/public/hats"
    )


def test_resolve_returns_hugging_face_registered_trees(monkeypatch):
    """hf:// registrations resolve: the mirror reads public Hub datasets."""
    monkeypatch.setattr(hats_registry, "_registry_records", lambda: RECORDS)
    assert hats_registry.resolve_hats_short_name("mmu_jwst_gds") == (
        "hf://datasets/UniverseTBD/mmu_jwst_gds"
    )


def test_resolve_explains_unreadable_entries(monkeypatch):
    monkeypatch.setattr(hats_registry, "_registry_records", lambda: RECORDS)
    with pytest.raises(InputError) as excinfo:
        hats_registry.resolve_hats_short_name("legacy_ivo_copy")
    message = str(excinfo.value)
    assert "ivo://example.org/hats/legacy" in message
    assert "Hugging Face" in message
    assert "data.lsdb.io" in message


def test_resolve_unknown_name_points_at_the_listing(monkeypatch):
    monkeypatch.setattr(hats_registry, "_registry_records", lambda: RECORDS)
    with pytest.raises(InputError) as excinfo:
        hats_registry.resolve_hats_short_name("not_a_catalogue")
    assert "xmatcher list --hats" in str(excinfo.value)


def test_resolve_falls_back_to_a_stale_snapshot_when_the_registry_fails(monkeypatch):
    _write_snapshot(hats_registry.cache_file(), age_seconds=10 * 24 * 3600)

    def unreachable():
        raise InputError("registry offline")

    monkeypatch.setattr(hats_registry, "_registry_records", unreachable)
    assert hats_registry.resolve_hats_short_name("gaia_dr3").endswith("gaia_dr3")


def test_refresh_rewrites_the_snapshot(monkeypatch):
    monkeypatch.setattr(hats_registry, "_registry_records", lambda: RECORDS)
    hats_registry.hats_catalogues(refresh=True)
    snapshot = json.loads(hats_registry.cache_file().read_text())
    assert snapshot["version"] == hats_registry._CACHE_VERSION
    assert len(snapshot["records"]) == len(RECORDS)


def test_crossmatch_resolves_a_registry_name_to_a_remote_hats_source(monkeypatch):
    import xmatcher.crossmatch as crossmatch_module

    monkeypatch.setattr(
        crossmatch_module, "resolve_hats_short_name", lambda name: "https://example.org/hats/x/"
    )
    source = crossmatch_module.CrossMatch().resolve_source("hats:two_mass", {})
    assert source.access_method == "hats"
    assert source.access_identifier.rstrip("/") == "https://example.org/hats/x"
    assert source.is_local is False


def test_list_hats_prints_registry_names_and_marks_unreadable_ones(monkeypatch, capsys):
    import xmatcher.cli as cli

    monkeypatch.setattr(hats_registry, "hats_catalogues", lambda **_: _by_name(RECORDS))
    assert cli.list_hats_catalogues(cli.Console(enabled=False)) == 0
    out = capsys.readouterr().out
    assert "two_mass" in out
    assert "https://data.lsdb.io/hats/two_mass/" in out
    assert "not readable by the mirror" in out
    assert "9 registered HATS catalogues, 8 readable here." in out


def _by_name(records):
    grouped: dict[str, list[dict[str, str]]] = {}
    for record in records:
        grouped.setdefault(record["short_name"].casefold(), []).append(record)
    return grouped


def test_resolve_survives_a_read_only_cache(monkeypatch):
    """A shared /arc mount can be read-only; the lookup must still answer."""
    monkeypatch.setattr(hats_registry, "_registry_records", lambda: RECORDS)

    def boom(path, records):
        raise OSError("read-only file system")

    monkeypatch.setattr(hats_registry, "_write_cache", boom)
    assert hats_registry.resolve_hats_short_name("two_mass").endswith("two_mass/")
