"""The streaming validator is the post-run gate for a Gaia master."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType

import polars as pl
import pytest

from xmatcher.mirror import _write_hats_native
from xmatcher.ray_anchor import ray_anchor_match
from xmatcher.sources import CatalogueSource

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "validate_anchor_output.py"


def _validator() -> ModuleType:
    spec = importlib.util.spec_from_file_location("validate_anchor_output", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["validate_anchor_output"] = module
    spec.loader.exec_module(module)
    return module


def _source(name: str, path: Path, **kwargs) -> CatalogueSource:
    return CatalogueSource(
        name=name,
        is_local=True,
        path=path,
        access_method="hats",
        ra_column="ra",
        dec_column="dec",
        **kwargs,
    )


@pytest.fixture()
def master(tmp_path: Path, monkeypatch) -> Path:
    monkeypatch.setitem(sys.modules, "ray", None)
    anchor = pl.DataFrame(
        {
            "source_id": [1, 2, 3, 4],
            "ra": [10.0, 20.0, 30.0, 40.0],
            "dec": [0.0, 1.0, -1.0, 2.0],
            "gmag": [12.0, 13.0, 14.0, 15.0],
        }
    )
    survey = pl.DataFrame(
        {
            "sid": [101, 102],
            "ra": [10.0, 20.0],
            "dec": [0.0, 1.0],
        }
    )
    _write_hats_native(anchor, tmp_path / "anchor", ra_column="ra", dec_column="dec")
    _write_hats_native(survey, tmp_path / "survey", ra_column="ra", dec_column="dec")
    out = tmp_path / "master"
    ray_anchor_match(
        _source("anchor", tmp_path / "anchor", id_column="source_id"),
        [_source("survey", tmp_path / "survey", id_column="sid")],
        sep_arcsec=1.5,
        output_file=str(out),
    )
    return out


def test_validator_reports_coverage_and_quality(master: Path) -> None:
    report = _validator().validate(master, expected_rows=4)
    assert "error" not in report
    assert report["rows"] == 4
    assert report["advertised_rows"] == 4
    assert report["null_ids"] == 0
    assert report["duplicate_ids_within_partition"] == 0
    assert report["surveys"]["survey"]["matched"] == 2
    assert report["surveys"]["survey"]["crowded"] == 0
    assert sum(report["surveys"]["survey"]["quality"].values()) == 4
    assert report["surveys"]["survey"]["quality"]["no_match"] == 2


def test_validator_fails_on_a_row_count_mismatch(master: Path) -> None:
    report = _validator().validate(master, expected_rows=5)
    assert "expected 5 rows" in report["error"]


def test_validator_cli_writes_json_and_exits_nonzero(master: Path, tmp_path: Path, capsys) -> None:
    module = _validator()
    out = tmp_path / "validation.json"
    code = module.main(
        [str(master), "--expected-rows", "4", "--json", str(out), "--progress-every", "0"]
    )
    assert code == 0
    assert json.loads(out.read_text())["rows"] == 4
    assert json.loads(capsys.readouterr().out)["rows"] == 4

    bad = tmp_path / "bad.json"
    assert module.main([str(master), "--expected-rows", "99", "--json", str(bad)]) == 1
    assert "error" in json.loads(bad.read_text())
