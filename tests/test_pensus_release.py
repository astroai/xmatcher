"""Regression coverage for the mounted-HATS release entry point."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import polars as pl
import pytest
import yaml
from astropy.coordinates import SkyCoord

hp = pytest.importorskip("cdshealpix")
hats = pytest.importorskip("hats")
from hats.io.validation import is_valid_catalog  # noqa: E402

from xmatcher.matchers import _pixellate  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from pensus import release  # noqa: E402


def _tree(
    root: Path, frame: pl.DataFrame, order: int, *, indexed: bool = True, multifile: bool = False
) -> None:
    index = _pixellate(hp, frame["ra"].to_numpy(), frame["dec"].to_numpy(), 29, label=str(root))
    if indexed:
        frame = frame.with_columns(pl.Series("_healpix_29", index))
    owner = index >> (2 * (29 - order))
    pixels = np.unique(owner)
    for pixel in pixels:
        stem = root / "dataset" / f"Norder={order}" / "Dir=0" / f"Npix={pixel}"
        rows = frame.filter(owner == pixel)
        if multifile:
            stem.mkdir(parents=True)
            for j, row in enumerate(rows.iter_slices(1)):
                row.write_parquet(stem / f"part-{j}.parquet")
        else:
            stem.parent.mkdir(parents=True, exist_ok=True)
            rows.write_parquet(stem.with_suffix(".parquet"))
    (root / "hats.properties").write_text(
        f"hats_nrows={len(frame)}\nhats_col_ra=ra\nhats_col_dec=dec\n"
        f"hats_max_depth={order}\ndataproduct_type=object\n"
    )
    pl.DataFrame({"Norder": [order] * len(pixels), "Npix": pixels}).write_csv(
        root / "partition_info.csv"
    )


def _example(tmp_path: Path) -> tuple[Path, Path, Path]:
    hats_root = tmp_path / "hats"
    draft = tmp_path / "drafts" / "test_release"
    spec = tmp_path / "spec.yaml"

    # RA=45 at Dec=0 crosses order-zero pixels 4 and 5. A/1 excludes
    # B/3 globally, B/4 survives as its own seed, and C joins B/4.
    for tag, ra, key, order in (
        ("a", [44.9999, 40.0], [1, 2], 0),
        ("b", [45.00002, 45.00002], [3, 4], 1),
        ("c", [45.00023, 270.0], [5, 6], 2),
    ):
        frame = pl.DataFrame(
            {
                "ra": ra,
                "dec": [0.0, 0.0],
                "obj": pl.Series([b"5", b"\xff6"])
                if tag == "c"
                else pl.Series(key, dtype=pl.UInt64),
                "brick": [7, 7],
                "flux": pl.Series([1.2, 3.4], dtype=pl.Float32),
                "ivar": [0.5, None],
                "flags": pl.Series([3, 0], dtype=pl.UInt16),
                "wide_unused": ["abc", "xyz"],
            }
        )
        _tree(hats_root / tag, frame, order, indexed=tag != "c", multifile=tag == "b")
    spec.write_text(
        yaml.safe_dump(
            {
                "order": 0,
                "radius_arcsec": 1.0,
                "pair_radii_arcsec": {"a/c": 0.5},
                "surveys": [
                    {
                        "tag": tag,
                        "release": f"{tag}_dr1",
                        "path": tag,
                        "id": ["brick", "obj"] if tag == "b" else "obj",
                        "bands": {
                            "test": {
                                "filter": f"Test/{tag}",
                                "value": "flux",
                                "unit": "Jy",
                                "uncertainty": "ivar",
                                "uncertainty_kind": "inverse_variance",
                                "flags": ["flags"],
                            }
                        },
                    }
                    for tag in "abc"
                ],
            }
        )
    )
    return hats_root, draft, spec


def test_full_release_conserves_cross_boundary_and_secondary_only_sources(tmp_path):
    hats_root, draft, spec = _example(tmp_path)
    assert (
        release.main(
            ["plan", "--spec", str(spec), "--hats-root", str(hats_root), "--draft", str(draft)]
        )
        == 0
    )
    run = ["run", "--draft", str(draft)]
    release.main([*run, "--pixel", "4", "--scratch", str(tmp_path / "scratch")])
    status = release._status(draft, False)
    assert status["missing"] == 11
    assert {"start": 4, "stop": 5} not in release._pending(draft, 1)["ranges"]
    with pytest.raises(SystemExit):
        release.main(["finalize", "--draft", str(draft)])

    part = release._dest(draft, 0, 4)
    timestamp = part.stat().st_mtime_ns
    release.main(run)
    assert part.stat().st_mtime_ns == timestamp
    assert release._pending(draft, 3)["ranges"] == []
    release.main(["finalize", "--draft", str(draft), "--deep", "--aggregate-metadata"])
    catalog = draft / "catalog"
    receipt = json.loads((catalog / release.READY).read_text())
    assert receipt["complete"] and receipt["verified_sha256"]
    assert receipt["aggregate_metadata"]
    assert receipt["members"] == {"a": 2, "b": 2, "c": 2}
    assert receipt["inputs"][0]["column_map"]["flux"] == "flux_a"
    assert hats.read_hats(catalog).catalog_info.total_rows == 4
    assert is_valid_catalog(catalog, strict=True)
    data = pl.read_parquet(
        str(catalog / "dataset" / "Norder=*" / "Dir=*" / "Npix=*.parquet"),
        hive_partitioning=False,
    )
    assert data.schema["id_a"] == pl.UInt64
    assert data.schema["id_c"] == pl.Binary
    assert data.schema["flux_a"] == pl.Float32
    assert data.schema["flags_c"] == pl.UInt16
    assert "wide_unused_a" not in data.columns
    assert data.select("id_a", "id_b", "id_c").sort("id_a", nulls_last=True).rows()[:2] == [
        (1, '{"brick":7,"obj":3}', None),
        (2, None, None),
    ]
    assert data.filter(pl.col("id_c") == b"5")["id_b"].item() == '{"brick":7,"obj":4}'
    assert data.filter(pl.col("id_c") == b"\xff6")["component_id"].item() == "c_dr1:ff36"
    with pytest.raises(SystemExit):
        release.main([*run, "--pixel", "4", "--repair"])


def test_release_rejects_unreviewed_inventory_and_input_changes(tmp_path):
    hats_root, draft, spec = _example(tmp_path)
    _tree(hats_root / "unreviewed", pl.DataFrame({"ra": [10.0], "dec": [20.0]}), 0)
    with pytest.raises(ValueError, match="unreviewed"):
        release.build_plan(spec, hats_root)
    data = yaml.safe_load(spec.read_text())
    data["exclude"] = {"unreviewed": "Duplicate release"}
    spec.write_text(yaml.safe_dump(data))
    release.main(
        ["plan", "--spec", str(spec), "--hats-root", str(hats_root), "--draft", str(draft)]
    )
    release.main(["run", "--draft", str(draft)])
    changed = next((hats_root / "a").rglob("Npix=*.parquet"))
    changed.touch()
    with pytest.raises(SystemExit):
        release.main(["finalize", "--draft", str(draft)])
    assert not (draft / "catalog" / release.READY).exists()


def test_motion_changes_tile_ownership_and_bound_is_enforced(tmp_path):
    pytest.importorskip("torchsky")
    root = tmp_path / "hats"
    draft = tmp_path / "draft"
    spec = tmp_path / "spec.yaml"
    _tree(
        root / "moving",
        pl.DataFrame(
            {
                "ra": [44.9999],
                "dec": [0.0],
                "id": [1],
                "flux": [2.0],
                "pmra": [720.0],
                "pmdec": [0.0],
            }
        ),
        1,
    )
    data = {
        "order": 0,
        "radius_arcsec": 1,
        "target_epoch_jyear": 2017,
        "surveys": [
            {
                "tag": "a",
                "release": "a_dr1",
                "path": "moving",
                "id": "id",
                "columns": ["flux"],
                "epoch_jyear": 2016,
                "pm": ["pmra", "pmdec"],
                "motion_bound_arcsec": 0.5,
            },
        ],
    }
    spec.write_text(yaml.safe_dump(data))
    plan = ["plan", "--spec", str(spec), "--hats-root", str(root), "--draft"]
    release.main([*plan, str(tmp_path / "small_bound")])
    with pytest.raises(SystemExit):
        release.main(["run", "--draft", str(tmp_path / "small_bound"), "--pixel", "5"])
    data["surveys"][0]["motion_bound_arcsec"] = 1
    spec.write_text(yaml.safe_dump(data))
    release.main([*plan, str(draft)])
    release.main(["run", "--draft", str(draft)])
    release.main(["finalize", "--draft", str(draft)])
    assert not release._dest(draft, 0, 4).exists()
    assert not (draft / "catalog" / "dataset" / "_metadata").exists()
    assert is_valid_catalog(draft / "catalog", strict=True)
    table = pl.read_parquet(release._dest(draft, 0, 5))
    assert table["ra"].item() == pytest.approx(45.0001)
    assert table["ra_a"].item() == pytest.approx(44.9999)
    assert table["position_epoch_jyear"].item() == pytest.approx(2017)


def test_tiled_cascade_agrees_with_a_global_spherical_oracle(tmp_path):
    rng = np.random.default_rng(74)
    root, draft, spec = (tmp_path / name for name in ("hats", "draft", "surveys.yaml"))
    order, radius = 7, 1.3
    surveys, catalogues, ids, pixels = [], [], [], set()
    for i, tag in enumerate(("a", "b", "c", "d")):
        # Dense chains near a tile boundary, the longitude seam, and a pole.
        ra = np.concatenate(
            [
                45 + rng.normal(0, 1.2 / 3600, 20),
                rng.normal(0, 1.2 / 3600, 20) % 360,
                rng.uniform(0, 360, 20),
            ]
        )
        dec = np.concatenate([rng.normal(0, 0.8 / 3600, 40), 90 - rng.uniform(0, 2 / 3600, 20)])
        key = np.arange(100 * (i + 1), 100 * (i + 1) + len(ra))
        ids.append(key)
        pixels.update(_pixellate(hp, ra, dec, order, label=tag).tolist())
        catalogues.append(SkyCoord(ra, dec, unit="deg"))
        frame = pl.DataFrame({"id": key, "ra": ra, "dec": dec, "flux": rng.random(len(ra))})
        _tree(root / tag, frame, 2, indexed=i % 2 == 0)
        surveys.append({"tag": tag, "release": tag, "path": tag, "id": "id", "columns": ["flux"]})
    spec.write_text(yaml.safe_dump({"order": order, "radius_arcsec": radius, "surveys": surveys}))

    active = [np.ones(len(key), dtype=bool) for key in ids]
    expected = {}
    for i, survey in enumerate(surveys):
        left = np.flatnonzero(active[i])
        rows = [[None] * len(surveys) for _ in left]
        for row, idx in zip(rows, left, strict=True):
            row[i] = int(ids[i][idx])
        for j in range(i + 1, len(surveys)):
            right = np.flatnonzero(active[j])
            if not len(left) or not len(right):
                continue
            distance = catalogues[i][left, None].separation(catalogues[j][right]).arcsec
            forward = distance.argmin(axis=1)
            backward = distance.argmin(axis=0)
            for p, q in enumerate(forward):
                if backward[q] == p and distance[p, q] <= radius:
                    rows[p][j] = int(ids[j][right[q]])
                    active[j][right[q]] = False
        expected.update({f"{survey['tag']}:{row[i]}": tuple(row) for row in rows})

    assert (
        release.main(["plan", "--spec", str(spec), "--hats-root", str(root), "--draft", str(draft)])
        == 0
    )
    selection = [arg for pixel in sorted(pixels) for arg in ("--pixel", str(pixel))]
    assert release.main(["run", "--draft", str(draft), *selection]) == 0
    data = pl.read_parquet(
        str(draft / "catalog" / "dataset" / "Norder=*" / "Dir=*" / "Npix=*.parquet"),
        hive_partitioning=False,
    )
    actual = {
        key: tuple(row)
        for key, *row in data.select(
            "component_id", *("id_" + s["tag"] for s in surveys)
        ).iter_rows()
    }
    assert actual == expected
    assert len(actual) == data.height
