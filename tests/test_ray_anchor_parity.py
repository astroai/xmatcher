"""Brute-force parity for the anchor engine.

``ray-anchor`` reads one anchor partition plus a *planned* subset of survey
partitions per task.  A planning bug that drops a candidate partition is
invisible in a single-pixel fixture: the row simply looks unmatched.  These
tests compare every output column against an all-pairs reference over a
multi-partition tree, so a missed candidate fails the parity assertion.

The reference is deliberately naive — sort every separation per anchor row —
and shares only the coordinate helpers (``_radec_to_xyz``/``_chord_to_arcsec``)
with the engine, never its partition planning or KD-tree code.
"""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from xmatcher import matchers
from xmatcher.astro_utils import propagate_proper_motion
from xmatcher.cli import main
from xmatcher.exceptions import CrossMatchError
from xmatcher.mirror import _write_hats_native
from xmatcher.ray_anchor import (
    AnchorPlan,
    build_anchor_plan,
    ray_anchor_match,
    run_anchor_partition,
)
from xmatcher.sources import CatalogueSource

RADIUS = 6.0


def _write(frame: pl.DataFrame, path: Path, *, threshold: int = 3) -> None:
    _write_hats_native(frame, path, ra_column="ra", dec_column="dec", threshold=threshold)


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


def _shift(ra: float, dec: float, east_arcsec: float, north_arcsec: float) -> tuple[float, float]:
    """Move a coordinate by arcseconds on the sky (small-angle approximation)."""
    return (
        ra + east_arcsec / 3600.0 / np.cos(np.radians(dec)),
        dec + north_arcsec / 3600.0,
    )


@pytest.fixture()
def trees(tmp_path: Path) -> dict[str, object]:
    """Three clusters, two surveys, and 24 anchor rows across many pixels."""
    rng = np.random.default_rng(20261022)
    centers = [(10.0, 0.0), (25.0, 15.0), (40.0, -20.0)]

    anchor_rows: list[dict[str, float]] = []
    for center_ra, center_dec in centers:
        for _ in range(8):
            east, north = rng.uniform(-6.0, 6.0, size=2)
            ra, dec = _shift(center_ra, center_dec, east, north)
            anchor_rows.append(
                {
                    "source_id": len(anchor_rows),
                    "ra": ra,
                    "dec": dec,
                    "gmag": 12.0 + len(anchor_rows),
                }
            )

    a_rows: list[dict[str, float]] = []
    for index, row in enumerate(anchor_rows):
        if index >= 20:
            continue
        for east, north in ((0.4, 0.0), (1.6, 0.6)):
            ra, dec = _shift(row["ra"], row["dec"], east, north)
            a_rows.append({"sid": len(a_rows) + 100, "ra": ra, "dec": dec, "mag": float(index)})
        if index % 3 == 0:
            ra, dec = _shift(row["ra"], row["dec"], 3.2, -1.0)
            a_rows.append({"sid": len(a_rows) + 100, "ra": ra, "dec": dec, "mag": 20.0})
    # Background rows far outside every radius; they must never be selected.
    for east, north in ((30.0, 30.0), (-40.0, 5.0), (60.0, -60.0)):
        ra, dec = _shift(centers[0][0], centers[0][1], east, north)
        a_rows.append({"sid": len(a_rows) + 100, "ra": ra, "dec": dec, "mag": 30.0})

    b_rows: list[dict[str, float]] = []
    for index, row in enumerate(anchor_rows):
        if index % 2:
            continue
        ra, dec = _shift(row["ra"], row["dec"], 4.5, 2.0)
        b_rows.append({"bid": 500 + index, "ra": ra, "dec": dec, "flux": float(index)})

    anchor_frame = pl.DataFrame(anchor_rows)
    a_frame = pl.DataFrame(a_rows)
    b_frame = pl.DataFrame(b_rows)
    for name, frame in (("anchor", anchor_frame), ("a", a_frame), ("b", b_frame)):
        _write(frame, tmp_path / name)
    return {
        "anchor": anchor_frame,
        "a": a_frame,
        "b": b_frame,
        "anchor_dir": tmp_path / "anchor",
        "a_dir": tmp_path / "a",
        "b_dir": tmp_path / "b",
        "out": tmp_path / "out",
    }


@dataclass
class _SurveyReference:
    best: np.ndarray
    second: np.ndarray
    counts: np.ndarray
    best_id: list[object]


def _reference(
    anchor: pl.DataFrame, surveys: dict[str, tuple[pl.DataFrame, str]]
) -> dict[str, _SurveyReference]:
    """Every separation per anchor row, no partitioning or KD-tree."""
    anchor_xyz = matchers._radec_to_xyz(anchor["ra"].to_numpy(), anchor["dec"].to_numpy())
    result: dict[str, _SurveyReference] = {}
    for tag, (frame, id_column) in surveys.items():
        ids = frame[id_column].to_list()
        best = np.full(anchor.height, np.nan)
        second = np.full(anchor.height, np.nan)
        counts = np.zeros(anchor.height, dtype=np.int64)
        best_id: list[object] = []
        survey_xyz = matchers._radec_to_xyz(frame["ra"].to_numpy(), frame["dec"].to_numpy())
        for row in range(anchor.height):
            distance = matchers._chord_to_arcsec(
                np.linalg.norm(survey_xyz - anchor_xyz[row], axis=1)
            )
            inside = np.flatnonzero(distance <= RADIUS + 1e-9)
            counts[row] = inside.size
            if not inside.size:
                best_id.append(None)
                continue
            order = inside[np.argsort(distance[inside], kind="stable")]
            best[row] = float(distance[order[0]])
            best_id.append(ids[int(order[0])])
            if order.size > 1:
                second[row] = float(distance[order[1]])
        result[tag] = _SurveyReference(best=best, second=second, counts=counts, best_id=best_id)
    return result


def _quality(counts: np.ndarray, best: np.ndarray) -> list[str]:
    labels: list[str] = []
    for count, separation in zip(counts.tolist(), best.tolist(), strict=True):
        if count == 0:
            labels.append("no_match")
        elif count > 1:
            labels.append("ambiguous")
        elif separation > 0.8 * RADIUS:
            labels.append("poor")
        else:
            labels.append("good")
    return labels


def _read_output(out: Path) -> pl.DataFrame:
    files = sorted((out / "dataset").glob("Norder=*/*/*.parquet"))
    assert files, f"no output partitions under {out}"
    return pl.concat([pl.read_parquet(path) for path in files])


def _plan(trees: dict[str, object], **kwargs) -> AnchorPlan:
    anchor = _source("anchor", trees["anchor_dir"], id_column="source_id")
    survey_a = _source("a", trees["a_dir"], id_column="sid")
    survey_b = _source("b", trees["b_dir"], id_column="bid")
    return build_anchor_plan(
        anchor,
        [survey_a, survey_b],
        sep_arcsec=RADIUS,
        out_dir=str(trees["out"]),
        **kwargs,
    )


def test_anchor_output_matches_bruteforce_on_multiple_partitions(trees) -> None:
    plan = _plan(trees)
    assert len(plan.anchor.partitions) > 1
    assert len(plan.surveys[0].partitions) > 1
    assert len(plan.surveys[1].partitions) > 1

    for index in range(len(plan.anchor.partitions)):
        run_anchor_partition(plan, index)

    result = _read_output(Path(trees["out"])).sort("source_id")
    anchor: pl.DataFrame = trees["anchor"]
    assert result.height == anchor.height
    assert result["source_id"].to_list() == anchor["source_id"].to_list()
    # Anchor payload survives unchanged when no target epoch is requested.
    assert result["gmag"].to_list() == anchor["gmag"].to_list()
    np.testing.assert_allclose(
        result["ra"].to_numpy(), anchor["ra"].to_numpy(), rtol=0.0, atol=1e-9
    )

    reference = _reference(anchor, {"a": (trees["a"], "sid"), "b": (trees["b"], "bid")})
    for tag, expected in reference.items():
        np.testing.assert_allclose(
            result[f"{tag}_sep_arcsec"].to_numpy(),
            expected.best,
            rtol=0.0,
            atol=1e-6,
            equal_nan=True,
        )
        np.testing.assert_allclose(
            result[f"{tag}_sep_second"].to_numpy(),
            expected.second,
            rtol=0.0,
            atol=1e-6,
            equal_nan=True,
        )
        assert result[f"{tag}_n_within"].to_list() == expected.counts.tolist()
        assert result[f"{tag}_crowded"].to_list() == (expected.counts > 1).tolist()
        assert result[f"{tag}_quality"].to_list() == _quality(expected.counts, expected.best)
        # The best counterpart is the nearest row, not merely some row in radius.
        id_column = "sid" if tag == "a" else "bid"
        assert result[f"{id_column}_{tag}"].to_list() == expected.best_id


def test_anchor_single_row_survey_keeps_every_anchor_row(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setitem(sys.modules, "ray", None)
    anchor = pl.DataFrame(
        {
            "source_id": [1, 2, 3],
            "ra": [10.0, 20.0, 30.0],
            "dec": [0.0, 0.0, 0.0],
        }
    )
    survey = pl.DataFrame({"sid": [7], "ra": [10.0], "dec": [0.0]})
    _write(anchor, tmp_path / "anchor")
    _write(survey, tmp_path / "survey")
    ray_anchor_match(
        _source("anchor", tmp_path / "anchor", id_column="source_id"),
        [_source("survey", tmp_path / "survey", id_column="sid")],
        sep_arcsec=1.5,
        output_file=str(tmp_path / "out"),
    )
    result = _read_output(tmp_path / "out").sort("source_id")
    assert result["source_id"].to_list() == [1, 2, 3]
    assert result["survey_n_within"].to_list() == [1, 0, 0]
    assert result["sid_survey"].to_list() == [7, None, None]
    assert result["survey_sep_second"].to_list() == [None, None, None]
    assert result["survey_quality"].to_list() == ["good", "no_match", "no_match"]


def test_anchor_target_epoch_moves_coordinates_and_stamps_metadata(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setitem(sys.modules, "ray", None)
    anchor = pl.DataFrame(
        {
            "source_id": [1, 2, 3],
            "ra": [10.0, 20.0, 30.0],
            "dec": [0.0, 2.0, -2.0],
            "pmra": [1000.0, 800.0, 500.0],
            "pmdec": [0.0, -300.0, 200.0],
        }
    )
    survey = pl.DataFrame({"sid": [9], "ra": [10.0], "dec": [0.0]})
    _write(anchor, tmp_path / "anchor")
    _write(survey, tmp_path / "survey")

    out = tmp_path / "out"
    ray_anchor_match(
        _source(
            "anchor",
            tmp_path / "anchor",
            id_column="source_id",
            epoch=2016.0,
            pm_ra_column="pmra",
            pm_dec_column="pmdec",
        ),
        [
            _source(
                "survey",
                tmp_path / "survey",
                id_column="sid",
                epoch=2012.0,
            )
        ],
        sep_arcsec=1.5,
        output_file=str(out),
        target_epoch=2020.0,
    )

    expected_ra, expected_dec = propagate_proper_motion(
        anchor["ra"].to_numpy(),
        anchor["dec"].to_numpy(),
        anchor["pmra"].to_numpy(),
        anchor["pmdec"].to_numpy(),
        np.full(anchor.height, 2016.0),
        2020.0,
    )
    result = _read_output(out).sort("source_id")
    np.testing.assert_allclose(result["ra"].to_numpy(), expected_ra, rtol=0.0, atol=1e-9)
    np.testing.assert_allclose(result["dec"].to_numpy(), expected_dec, rtol=0.0, atol=1e-9)

    props = dict(
        line.split("=", 1) for line in (out / "properties").read_text().splitlines() if "=" in line
    )
    assert props["hats_epoch"] == "J2020.000"
    assert props["hats_nrows"] == "3"
    # 500-1000 mas/yr over eight years is several arcsec: a warning, not a match.
    assert result["survey_pm_warn"].to_list() == [True, True, True]
    assert result["survey_quality"].to_list() == ["no_match", "no_match", "no_match"]


def test_anchor_resume_reuses_finished_partitions(trees, monkeypatch) -> None:
    monkeypatch.setitem(sys.modules, "ray", None)
    anchor = _source("anchor", trees["anchor_dir"], id_column="source_id")
    survey = _source("a", trees["a_dir"], id_column="sid")
    out = Path(trees["out"])
    ray_anchor_match(anchor, [survey], sep_arcsec=RADIUS, output_file=str(out))
    files = sorted((out / "dataset").glob("Norder=*/*/*.parquet"))
    assert len(files) > 1
    before = {path: path.stat().st_mtime_ns for path in files}
    first_rows = _read_output(out).height

    removed = files[0]
    removed.unlink()
    ray_anchor_match(anchor, [survey], sep_arcsec=RADIUS, output_file=str(out))

    after = {
        path: path.stat().st_mtime_ns
        for path in sorted((out / "dataset").glob("Norder=*/*/*.parquet"))
    }
    assert removed in after
    for path, stamp in before.items():
        if path != removed:
            assert after[path] == stamp, f"{path.name} was rewritten on resume"
    assert _read_output(out).height == first_rows
    state = json.loads((out / "resume.state").read_text())
    assert state["status"] == "done"


def test_anchor_fingerprint_change_clears_stale_output(trees, monkeypatch) -> None:
    monkeypatch.setitem(sys.modules, "ray", None)
    anchor = _source("anchor", trees["anchor_dir"], id_column="source_id")
    survey = _source("a", trees["a_dir"], id_column="sid")
    out = Path(trees["out"])
    ray_anchor_match(anchor, [survey], sep_arcsec=RADIUS, output_file=str(out))
    rows = _read_output(out).height

    stale = out / "dataset" / "Norder=99" / "Dir=0" / "Npix=0.parquet"
    stale.parent.mkdir(parents=True)
    stale.write_bytes(b"not a real partition")
    state = json.loads((out / "resume.state").read_text())
    state["fingerprint"] = {"version": 0, "anchor": "somewhere else"}
    (out / "resume.state").write_text(json.dumps(state))

    ray_anchor_match(anchor, [survey], sep_arcsec=RADIUS, output_file=str(out))
    assert not stale.exists(), "a changed plan must not reuse the old dataset"
    assert _read_output(out).height == rows


def test_anchor_refuses_existing_output_without_fingerprint(trees, monkeypatch) -> None:
    monkeypatch.setitem(sys.modules, "ray", None)
    out = Path(trees["out"])
    (out / "dataset").mkdir(parents=True)
    with pytest.raises(CrossMatchError, match="cannot resume"):
        ray_anchor_match(
            _source("anchor", trees["anchor_dir"], id_column="source_id"),
            [_source("a", trees["a_dir"], id_column="sid")],
            sep_arcsec=RADIUS,
            output_file=str(out),
        )


def test_anchor_requires_an_anchor_identifier(tmp_path: Path) -> None:
    anchor = pl.DataFrame({"ra": [10.0], "dec": [0.0], "gmag": [12.0]})
    survey = pl.DataFrame({"sid": [1], "ra": [10.0], "dec": [0.0]})
    _write(anchor, tmp_path / "anchor")
    _write(survey, tmp_path / "survey")
    with pytest.raises(CrossMatchError, match="needs an identifier"):
        build_anchor_plan(
            _source("anchor", tmp_path / "anchor"),
            [_source("survey", tmp_path / "survey", id_column="sid")],
            sep_arcsec=RADIUS,
            out_dir=str(tmp_path / "out"),
        )


def test_duplicate_survey_names_get_distinct_tags(tmp_path: Path) -> None:
    anchor = pl.DataFrame({"source_id": [1], "ra": [10.0], "dec": [0.0]})
    first = pl.DataFrame({"sid": [1], "ra": [10.0], "dec": [0.0]})
    second = pl.DataFrame({"sid": [2], "ra": [30.0], "dec": [0.0]})
    _write(anchor, tmp_path / "anchor")
    _write(first, tmp_path / "first")
    _write(second, tmp_path / "second")
    plan = build_anchor_plan(
        _source("anchor", tmp_path / "anchor", id_column="source_id"),
        [
            _source("dup", tmp_path / "first", id_column="sid"),
            _source("dup", tmp_path / "second", id_column="sid"),
        ],
        sep_arcsec=1.5,
        out_dir=str(tmp_path / "out"),
    )
    assert plan.survey_tags == ["dup", "dup_2"]
    assert "dup_sep_arcsec" in plan.out_columns
    assert "dup_2_sep_arcsec" in plan.out_columns
    assert "dup_n_within" in plan.out_columns
    assert "dup_2_n_within" in plan.out_columns


def test_anchor_task_guard_names_the_knobs(trees) -> None:
    with pytest.raises(CrossMatchError, match="--task-rows"):
        _plan(trees, task_rows=1)


def test_cli_anchor_requires_output_and_best_find(capsys) -> None:
    assert main(["match", "gaia", "sdss", "--engine", "ray-anchor"]) == 1
    assert "requires an output HATS path" in capsys.readouterr().err

    assert (
        main(["match", "gaia", "sdss", "--engine", "ray-anchor", "-o", "out.hats", "--find", "all"])
        == 1
    )
    assert "always keeps the best counterpart" in capsys.readouterr().err


def test_cli_project_hats_writes_a_narrow_tree(tmp_path: Path, capsys) -> None:
    frame = pl.DataFrame(
        {
            "id": [1, 2],
            "ra": [10.0, 20.0],
            "dec": [0.0, 1.0],
            "mag": [12.0, 13.0],
        }
    )
    _write(frame, tmp_path / "wide")
    out = tmp_path / "narrow"
    assert (
        main(
            [
                "project-hats",
                str(tmp_path / "wide"),
                str(out),
                "--columns",
                "id,ra,dec",
                "--no-sync",
            ]
        )
        == 0
    )
    partition_file = next((out / "dataset").glob("Norder=*/*/*.parquet"))
    result = pl.read_parquet(partition_file)
    assert result.columns == ["id", "ra", "dec"]
    assert "Wrote 2 rows x 3 columns" in capsys.readouterr().out

    # --resume completes a projection whose partition file was lost.
    partition_file.unlink()
    assert (
        main(
            [
                "project-hats",
                str(tmp_path / "wide"),
                str(out),
                "--columns",
                "id,ra,dec",
                "--no-sync",
                "--resume",
            ]
        )
        == 0
    )
    assert pl.read_parquet(next((out / "dataset").glob("Norder=*/*/*.parquet"))).height == 2
