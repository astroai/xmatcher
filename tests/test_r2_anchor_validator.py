"""Round-2 audit: the ray-anchor validator fails on every broken contract."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import polars as pl

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "validate_anchor_output.py"


def _validator() -> ModuleType:
    spec = importlib.util.spec_from_file_location("validate_anchor_output_r2", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["validate_anchor_output_r2"] = module
    spec.loader.exec_module(module)
    return module


def _master(root: Path, ids: list[int | None], advertised: int) -> Path:
    partition = root / "dataset" / "Norder=0" / "Dir=0"
    partition.mkdir(parents=True)
    n = len(ids)
    pl.DataFrame(
        {
            "source_id": pl.Series(ids, dtype=pl.Int64),
            "s_n_within": [1] * n,
            "s_crowded": [False] * n,
            "s_pm_warn": [False] * n,
            "s_quality": ["unique"] * n,
        }
    ).write_parquet(partition / "Npix=0.parquet")
    (root / "hats.properties").write_text(f"hats_nrows={advertised}\n")
    return root


def test_duplicate_anchor_ids_fail_the_gate(tmp_path) -> None:
    # Before: duplicates were counted but the report had no error, exit 0.
    master = _master(tmp_path / "master", [1, 2, 2], advertised=3)
    module = _validator()
    report = module.validate(master, progress_every=0)
    assert report["duplicate_ids_within_partition"] == 2
    assert "share a source_id" in report["error"]
    assert module.main([str(master), "--progress-every", "0"]) == 1


def test_every_failure_is_reported(tmp_path) -> None:
    # Before: each check overwrote the previous message, hiding all but one.
    master = _master(tmp_path / "master", [1, None], advertised=3)
    report = _validator().validate(master, expected_rows=4, progress_every=0)
    for message in ("hats_nrows=3", "null source_id", "expected 4 rows"):
        assert message in report["error"]
