"""Round-2 audit regressions: local input detection."""

from __future__ import annotations

import pytest


def test_is_hats_dir_requires_hats_properties(tmp_path):
    from xmatcher import io_utils

    plain = tmp_path / "dataset.parquet"
    plain.mkdir()
    (plain / "_metadata").write_bytes(b"PAR1")  # any pyarrow dataset has one
    assert not io_utils.is_hats_dir(plain)
    hipscat = tmp_path / "hipscat"
    hipscat.mkdir()
    (hipscat / "catalog_info.json").write_text("{}")  # no current producer
    assert not io_utils.is_hats_dir(hipscat)
    hats = tmp_path / "cat"
    hats.mkdir()
    (hats / "properties").write_text("dataproduct_type=object\n")
    assert io_utils.is_hats_dir(hats)


@pytest.mark.parametrize("name", ["missing.csv", "data/missing_gaia.parquet", "./gaia"])
def test_missing_path_is_not_offered_catalogue_names(name, tmp_path, monkeypatch):
    from xmatcher import CrossMatch
    from xmatcher.exceptions import InputError

    monkeypatch.chdir(tmp_path)
    with pytest.raises(InputError, match="No such file") as info:
        CrossMatch().resolve_source(name, {})
    assert info.value.source is None


def test_cli_missing_path_has_no_did_you_mean(tmp_path, monkeypatch, capsys):
    from xmatcher.cli import main

    monkeypatch.chdir(tmp_path)
    (tmp_path / "b.csv").write_text("ra,dec\n1,2\n")
    assert main(["missing.csv", "b.csv"]) == 1
    err = capsys.readouterr().err
    assert "missing.csv" in err
    assert "Did you mean" not in err
