from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts.pensus.arc import repair_hats_audit_20261124 as audit


def test_audited_ra_wrap_preserves_all_other_values() -> None:
    ra = np.array([360.0, -0.000001, 0.000002])
    dec = np.array([24.0] * 3)
    pix = audit.healpix29(ra % 360, dec)
    batch = pa.table({"RA": ra, "DEC": dec, audit.INDEX: pix, "id": [9, 1, 5]})
    patched, count = audit.corrected_ra(batch, "RA", "DEC", (0, int(pix[0]) >> 58))
    np.testing.assert_array_equal(patched["RA"].to_numpy(), ra % 360)
    assert count == 2
    assert patched.drop(["RA"]).equals(batch.drop(["RA"]))
    assert patched.schema.equals(batch.schema)


def test_repair_rejects_changed_healpix_partition() -> None:
    batch = pa.table({"RA": [360.0], "DEC": [24.0], audit.INDEX: [0]})
    with pytest.raises(ValueError, match="across partition"):
        audit.corrected_ra(batch, "RA", "DEC", (0, 0))


def test_publication_and_rollback_keep_verified_original(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(audit, "HATS", tmp_path)
    root = tmp_path / audit.PHOTOZ
    root.mkdir(parents=True)
    run = tmp_path / "run"
    run.mkdir()
    relative = Path("hats.properties")
    live = root / relative
    live.write_text("original\n")
    repair = audit.Repair(root, run)
    repair.stage(relative).write_text("updated\n")
    plan = repair.snapshot()
    audit.write_json(run / "plan.json", plan)
    repair.publish(plan)
    assert live.read_text() == "updated\n"
    audit.rollback(audit.PHOTOZ, run)
    assert live.read_text() == "original\n"
    audit.rollback(audit.PHOTOZ, run)


def test_aggregate_metadata_uses_dataset_relative_paths(tmp_path: Path) -> None:
    root = tmp_path / "live"
    repair = audit.Repair(root, tmp_path / "run")
    files = [
        Path("dataset/Norder=0/Dir=0/Npix=0.parquet"),
        Path("dataset/Norder=0/Dir=0/Npix=1.parquet"),
    ]
    for path in files:
        source = root / path
        source.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.table({"id": [1, 2]}), source)
    audit.metadata(repair, files, 4)
    result = pq.read_metadata(repair.outputs[Path("dataset/_metadata")])
    assert result.num_rows == 4
    assert [result.row_group(i).column(0).file_path for i in range(result.num_row_groups)] == [
        str(f.relative_to("dataset")) for f in files
    ]
