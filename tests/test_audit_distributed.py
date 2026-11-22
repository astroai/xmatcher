"""Audit regressions for the distributed HATS engines (ray-union, ray-anchor)."""

from __future__ import annotations

import importlib.util
import math
import shutil
import sys
from collections import Counter
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl
import pytest

from xmatcher import ray_union
from xmatcher.mirror import _write_hats_native
from xmatcher.ray_anchor import ray_anchor_match
from xmatcher.sources import CatalogueSource

from .test_ray_union import _catalogue, _pixeled_catalogue, _read_output, _sep_arcsec

HAVE_RAY = importlib.util.find_spec("ray") is not None
needs_ray = pytest.mark.skipif(not HAVE_RAY, reason="ray is not installed")


@pytest.fixture()
def small_ray():
    """Two-CPU Ray owned by the test, so the engine does not start its own."""
    import ray

    ray.init(num_cpus=2, logging_level=40, include_dashboard=False, ignore_reinit_error=True)
    yield
    ray.shutdown()


def _oracle_union(frames: list[pl.DataFrame], sep: float) -> Counter:
    """N-way sequential-union row sets as ``(_src_cats, ids)``.

    A row leads a row set unless it matches a leading row of a lower
    catalogue (then it is folded into that set at the lower row's position).
    A leading row stars over every higher-catalogue row within ``sep``.
    """
    rows = [f.to_dicts() for f in frames]
    leads: list[list[bool]] = []
    for k, cat in enumerate(rows):
        leads.append(
            [
                not any(
                    leads[j][i] and _sep_arcsec(lower, row) <= sep
                    for j in range(k)
                    for i, lower in enumerate(rows[j])
                )
                for row in cat
            ]
        )
    out: Counter = Counter()
    for k, cat in enumerate(rows):
        for i, row in enumerate(cat):
            if not leads[k][i]:
                continue
            axes: list[list[str | None]] = [
                [None] + [o["id"] for o in rows[j] if _sep_arcsec(row, o) <= sep]
                for j in range(k + 1, len(rows))
            ]
            combos: list[tuple[str | None, ...]] = [()]
            for axis in axes:
                combos = [c + (v,) for c in combos for v in axis]
            mated = any(len(axis) > 1 for axis in axes)
            for combo in combos:
                if mated and all(v is None for v in combo):
                    continue
                labels = [str(k + 1)] + [
                    str(k + 2 + j) for j, v in enumerate(combo) if v is not None
                ]
                ids = (row["id"],) + tuple(v for v in combo if v is not None)
                out[("+".join(labels), ids)] += 1
    return out


def _ray_sets(df: pl.DataFrame, n: int) -> Counter:
    cols = ["id"] + [f"id_{k}" for k in range(2, n + 1)]
    return Counter(
        (r["_src_cats"], tuple(r[c] for c in cols if r[c] is not None)) for r in df.to_dicts()
    )


@needs_ray
def test_union_keeps_row_whose_only_lower_mate_is_already_folded(tmp_path: Path, small_ray):
    """A(0") - B(0.8") - C(1.6") with r=1": B folds into A at A's position,
    so C is unmatched in the sequential union.  Ray-union gated C on its B
    mate and dropped the C measurement from the output entirely."""
    d = 1.0 / 3600.0
    frames = [
        pl.DataFrame({"ra": [10.0], "dec": [k * 0.8 * d], "id": [name]})
        for k, name in enumerate(("A0", "B0", "C0"))
    ]
    srcs = [_catalogue(tmp_path / n, n, f) for n, f in zip("abc", frames, strict=True)]
    out = tmp_path / "out"
    ray_union.ray_union_match(srcs, sep_arcsec=1.0, output_file=str(out))
    got = _ray_sets(_read_output(out), 3)
    assert got == Counter({("1+2", ("A0", "B0")): 1, ("3", ("C0",)): 1})


@needs_ray
@pytest.mark.parametrize("orders", [(0, 3, 5, 1), (5, 0, 3, 6)])
def test_union_four_catalogues_match_sequential_oracle(tmp_path: Path, small_ray, orders):
    """Dense 4-way field across mixed partition orders, RA=0/360 and a pole:
    every row set exactly once and every input row present."""
    rng = np.random.default_rng(7)
    centres = [(0.0, 0.0), (359.9995, 5.0), (45.0, 89.998)]
    frames = []
    for k in range(4):
        rows = []
        for i in range(36):
            ra0, dec0 = centres[i % len(centres)]
            dec = float(np.clip(dec0 + rng.uniform(-6, 6) / 3600.0, -90, 90))
            ra = (ra0 + rng.uniform(-6, 6) / 3600.0 / max(math.cos(math.radians(dec)), 1e-3)) % 360
            rows.append({"ra": float(ra), "dec": dec, "id": f"{'ABCD'[k]}{i}"})
        frames.append(pl.DataFrame(rows))
    srcs = [
        _pixeled_catalogue(tmp_path / f"c{k}", f"c{k}", f, orders[k]) for k, f in enumerate(frames)
    ]
    out = tmp_path / "out"
    ray_union.ray_union_match(srcs, sep_arcsec=2.0, output_file=str(out), max_tuples=10**6)
    df = _read_output(out)
    assert _ray_sets(df, 4) == _oracle_union(frames, 2.0)
    for k, frame in enumerate(frames):
        column = "id" if k == 0 else f"id_{k + 1}"
        assert set(frame["id"]) <= set(df[column].drop_nulls())


# --------------------------------------------------------------------------- #
# ray-anchor (serial path: the per-partition loop is identical under Ray)
# --------------------------------------------------------------------------- #
def _anchor_inputs(tmp_path: Path) -> None:
    anchor = pl.DataFrame(
        {
            "source_id": [1, 2, 3],
            "ra": [10.0, 20.0, 30.0],
            "dec": [0.0, 2.0, -2.0],
            "pmra": [1.0, None, float("nan")],
            "pmdec": [0.0, None, 0.0],
        }
    )
    survey = pl.DataFrame({"sid": [9, 8], "ra": [10.0, 20.0], "dec": [0.0, 2.0]})
    _write_hats_native(anchor, tmp_path / "anchor", ra_column="ra", dec_column="dec")
    _write_hats_native(survey, tmp_path / "survey", ra_column="ra", dec_column="dec")


def _run_anchor(tmp_path: Path, out: Path, *, survey_epoch: float | None) -> pl.DataFrame:
    common: dict[str, Any] = {
        "is_local": True,
        "access_method": "hats",
        "ra_column": "ra",
        "dec_column": "dec",
    }
    ray_anchor_match(
        CatalogueSource(
            name="anchor",
            path=tmp_path / "anchor",
            id_column="source_id",
            epoch=2016.0,
            pm_ra_column="pmra",
            pm_dec_column="pmdec",
            **common,
        ),
        [
            CatalogueSource(
                name="survey",
                path=tmp_path / "survey",
                id_column="sid",
                epoch=survey_epoch,
                **common,
            )
        ],
        sep_arcsec=1.5,
        output_file=str(out),
    )
    files = sorted((out / "dataset").glob("Norder=*/*/*.parquet"))
    return pl.concat([pl.read_parquet(f) for f in files]).sort("source_id")


def test_anchor_pm_warn_flags_rows_without_usable_motion(tmp_path: Path, monkeypatch) -> None:
    """A null/NaN anchor proper motion over a 16-year baseline is unknown
    motion, not zero motion: it must be flagged like an anchor without PM."""
    monkeypatch.setitem(sys.modules, "ray", None)
    _anchor_inputs(tmp_path)
    result = _run_anchor(tmp_path, tmp_path / "out", survey_epoch=2000.0)
    # 1 mas/yr * 16 yr = 16 mas: no warning; null and NaN motion: warning.
    assert result["survey_pm_warn"].to_list() == [False, True, True]


def test_anchor_resume_fingerprint_tracks_epochs_and_input_content(
    tmp_path: Path, monkeypatch
) -> None:
    """Changing a survey epoch or rewriting an input partition (same row
    count) changes the output, so finished partitions must not be reused."""
    monkeypatch.setitem(sys.modules, "ray", None)
    _anchor_inputs(tmp_path)
    out = tmp_path / "out"
    first = _run_anchor(tmp_path, out, survey_epoch=None)
    assert first["survey_pm_warn"].to_list() == [False, False, False]

    second = _run_anchor(tmp_path, out, survey_epoch=2000.0)
    assert second["survey_pm_warn"].to_list() == [False, True, True]

    shutil.rmtree(tmp_path / "survey")
    moved = pl.DataFrame({"sid": [9, 8], "ra": [10.0, 20.0005], "dec": [0.0, 2.0]})
    _write_hats_native(moved, tmp_path / "survey", ra_column="ra", dec_column="dec")
    third = _run_anchor(tmp_path, out, survey_epoch=2000.0)
    assert third["sid_survey"].to_list() == [9, None, None]


@needs_ray
def test_union_resume_survives_raising_sizing_knobs(tmp_path: Path, small_ray) -> None:
    """The run-time guard tells the user to raise --chunk-memory-gb; doing so
    (or changing --task-rows/--hats-threshold) does not change the output and
    must not wipe the finished chunks of a day-scale run."""
    frames = [
        pl.DataFrame({"ra": [10.0, 10.1], "dec": [0.0, 0.0], "id": [f"{n}0", f"{n}1"]})
        for n in "AB"
    ]
    srcs = [_catalogue(tmp_path / n, n, f) for n, f in zip("ab", frames, strict=True)]
    out = tmp_path / "out"
    ray_union.ray_union_match(srcs, sep_arcsec=1.0, output_file=str(out))
    stamps = {p.name: p.stat().st_mtime_ns for p in (out / "chunks").glob("*.parquet")}
    assert stamps
    ray_union.ray_union_match(
        srcs,
        sep_arcsec=1.0,
        output_file=str(out),
        chunk_memory_gb=16.0,
        task_rows=4_000_000,
        hats_threshold=50_000,
    )
    assert {p.name: p.stat().st_mtime_ns for p in (out / "chunks").glob("*.parquet")} == stamps
