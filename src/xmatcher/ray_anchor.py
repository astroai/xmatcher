"""Gaia-anchored crossmatch on a Ray cluster.

``engine="ray-anchor"`` treats the first catalogue as the *anchor* (normally
Gaia DR3) and every later catalogue as a survey.  The output has exactly one
row per anchor source: the anchor's selected columns are kept and each survey
contributes its best counterpart plus a small ambiguity block.  Anchor and
survey coordinates are propagated to ``target_epoch`` when one is requested,
so the anchor's coordinate columns hold the epoch-adjusted position:

``<survey>_sep_arcsec``
    Separation of the best counterpart, arcsec (null when unmatched).
``<survey>_sep_second``
    Separation of the second nearest counterpart inside the radius, arcsec
    (null when fewer than two candidates).
``<survey>_n_within``
    Number of survey sources inside the match radius.
``<survey>_crowded``
    ``n_within > 1``.
``<survey>_pm_warn``
    The anchor's proper motion moves it by more than half the match radius
    over the anchor-survey epoch baseline (or the epoch baseline is large and
    the anchor has no usable proper motion).
``<survey>_quality``
    ``good``, ``poor``, ``ambiguous``, ``pm_warn``, ``ambiguous+pm_warn``, or
    ``no_match``.

The pipeline mirrors every input into a local HATS tree (the same cache and
resume machinery as ``ray-union``), builds a partition plan once, and runs one
Ray task per anchor partition.  Each task writes its output partition directly
to ``<out>/dataset/Norder=…/Dir=…/Npix=….parquet``; a completed partition is
skipped on a rerun, so an interrupted run resumes without re-reading the sky.
"""

from __future__ import annotations

import importlib
import json
import logging
import math
import os
import re
import shutil
import time
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass, field, replace
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl

from . import matchers
from .exceptions import CrossMatchError
from .ray_union import (
    CataloguePlan,
    PartitionPlan,
    _atomic_write_parquet,
    _atomic_write_text,
    _catalogue_root,
    _catalogue_schema,
    _check_coords,
    _cone_ranges,
    _est_rows,
    _footer_row_count,
    _gather_rows,
    _hats_dir,
    _list_partitions,
    _partition_index,
    _pixel_diagonal_deg,
    _plan_source,
    _read_hats_properties,
    _write_hats_tree,
)
from .sources import CatalogueSource
from .storage import LocalStorage, open_storage

logger = logging.getLogger(__name__)

DEFAULT_TASK_ROWS = 2_000_000
DEFAULT_CHUNK_MEMORY_GB = 8.0
_BYTES_PER_ROW = 96.0
_STATE_NAME = "resume.state"

_LAST_PLAN: AnchorPlan | None = None


def last_plan() -> AnchorPlan | None:
    """Return the most recently built anchor plan (tests / ``doctor``)."""
    return _LAST_PLAN


# --------------------------------------------------------------------------- #
# plan data
# --------------------------------------------------------------------------- #
@dataclass
class AnchorPlan:
    """Picklable plan for one anchor catalogue and N surveys."""

    anchor: CataloguePlan
    surveys: list[CataloguePlan]
    survey_tags: list[str]
    sep_arcsec: float
    delta_deg: float
    out_dir: str
    anchor_id: str
    out_columns: list[str]
    candidates: list[dict[str, list[int]]] = field(default_factory=list)
    target_epoch: float | None = None
    anchor_epoch: float | None = None
    matcher: str = "sky"
    pm_prior: bool = False
    pm_prior_magnitude_column: str | None = None
    fallback_policy: str = "warn"
    max_error: float = 1.0
    chunk_memory_gb: float = DEFAULT_CHUNK_MEMORY_GB
    task_rows: int = DEFAULT_TASK_ROWS


def _safe_tag(name: str, used: set[str]) -> str:
    base = re.sub(r"[^0-9A-Za-z]+", "_", name).strip("_").lower() or "survey"
    tag = base
    n = 2
    while tag in used:
        tag = f"{base}_{n}"
        n += 1
    used.add(tag)
    return tag


def _required_columns(src: CatalogueSource) -> list[str]:
    out: list[str] = []
    for value in (
        src.id_column,
        src.ra_column,
        src.dec_column,
        src.epoch_column,
        src.pm_ra_column,
        src.pm_dec_column,
        src.parallax_column,
        src.radial_velocity_column,
        src.ra_err_column,
        src.dec_err_column,
        src.corr_column,
    ):
        if value:
            out.append(value)
    if src.astrometric_covariance_columns:
        out.extend(src.astrometric_covariance_columns.values())
    return out


def _selected_columns(
    src: CatalogueSource, schema: dict[str, pl.DataType], *, label: str
) -> list[str]:
    columns = list(src.default_columns or schema)
    missing = sorted({c for c in columns if c not in schema})
    if missing:
        raise CrossMatchError(
            f"catalogue '{label}': default_columns name columns missing from the HATS "
            f"schema: {', '.join(missing)}"
        )
    for column in _required_columns(src):
        if column not in columns:
            if column not in schema:
                raise CrossMatchError(
                    f"catalogue '{label}': required column '{column}' is missing from the "
                    "HATS schema"
                )
            columns.append(column)
    # Keep the source order after the requested/default projection.
    selected = set(columns)
    return [c for c in schema if c in selected]


def _build_catalogue_plan(
    src: CatalogueSource,
    *,
    tag: str,
    is_anchor: bool,
    anchor_id_name: str,
    cache_root: str | None,
    hats_threshold: int,
) -> CataloguePlan:
    root, rel = _catalogue_root(src, cache_root)
    storage = open_storage(root)
    parts = _list_partitions(rel, storage)
    props = _read_hats_properties(storage, rel)
    if not parts:
        raise CrossMatchError(
            f"catalogue '{src.name}' has no HATS partitions under {root!r}/{rel!r}"
        )
    ordering = next((v for k, v in props.items() if k.lower() == "hats_ordering"), "")
    if ordering.upper() == "RING":
        from .ray_union import _cdshealpix

        cds = _cdshealpix()
        for part in parts:
            part.pix = int(cds.from_ring(np.asarray([part.pix], dtype=np.uint64), part.order)[0])
    parts.sort(key=lambda p: (p.order, p.pix))
    for part in parts:
        _est_rows(part, storage, hats_threshold)

    schema_rel = parts[0].rel
    schema = _catalogue_schema(storage, schema_rel)
    if not schema:
        raise CrossMatchError(
            f"cannot read the HATS schema of catalogue '{src.name}' ({root!r}/{schema_rel!r})"
        )

    # Explicit `hats:<url>` sources carry no column metadata.  Infer the
    # geometry and a sensible ID from the HATS properties / schema so the
    # engine works without requiring a YAML entry for every URL.
    inferred_ra = src.ra_column or props.get("hats_col_ra")
    inferred_dec = src.dec_column or props.get("hats_col_dec")
    if not inferred_ra or not inferred_dec:
        from .astro_utils import find_coord_columns

        try:
            inferred_ra, inferred_dec = find_coord_columns(list(schema))
        except Exception as exc:  # noqa: BLE001 - name the catalogue below
            raise CrossMatchError(
                f"catalogue '{src.name}': cannot infer RA/Dec columns; pass --ra1/--dec1 "
                "or a configured catalogue entry"
            ) from exc
    inferred_id = src.id_column
    if inferred_id is None:
        for candidate in (
            "source_id",
            "Source",
            "object_id",
            "objID",
            "id",
            "designation",
            "ID",
        ):
            if candidate in schema:
                inferred_id = candidate
                break
    if is_anchor and inferred_id is None:
        raise CrossMatchError(
            f"catalogue '{src.name}': engine='ray-anchor' needs an identifier for the "
            "anchor, so every master row can be keyed. Set `id_column` for this "
            "catalogue (CLI: `--id1 <column>`) or project one in."
        )
    src = replace(
        src,
        ra_column=inferred_ra,
        dec_column=inferred_dec,
        id_column=inferred_id,
    )
    cols = _selected_columns(src, schema, label=src.name)
    schema = {c: schema[c] for c in cols}

    final_cols: list[str] = []
    used: set[str] = set()
    for column in cols:
        if is_anchor and column == src.id_column and column != anchor_id_name:
            final = anchor_id_name
        elif is_anchor:
            final = column
        else:
            final = f"{column}_{tag}"
        while final in used:
            final = f"{final}_x"
        used.add(final)
        final_cols.append(final)

    mapping = dict(zip(cols, final_cols, strict=True))

    def mapped(value: str | None) -> str | None:
        return mapping.get(value, value) if value else None

    ra = mapped(src.ra_column or "ra")
    dec = mapped(src.dec_column or "dec")
    if ra not in final_cols or dec not in final_cols:
        raise CrossMatchError(
            f"catalogue '{src.name}': RA/Dec columns '{src.ra_column}'/'{src.dec_column}' "
            "are not in the selected projection"
        )
    id_col = mapped(src.id_column)
    if is_anchor and src.id_column is not None and id_col != anchor_id_name:
        raise CrossMatchError(
            f"catalogue '{src.name}': anchor id column '{src.id_column}' did not map to "
            f"'{anchor_id_name}'"
        )

    cov = (
        {k: mapping.get(v, v) for k, v in src.astrometric_covariance_columns.items()}
        if src.astrometric_covariance_columns
        else None
    )
    return CataloguePlan(
        name=src.name,
        root=str(storage.root) if isinstance(storage, LocalStorage) else root,
        rel=rel,
        ra=ra,
        dec=dec,
        partitions=parts,
        cols=cols,
        final_cols=final_cols,
        dtypes={final: schema[orig] for orig, final in zip(cols, final_cols, strict=True)},
        ra_err_column=mapped(src.ra_err_column),
        dec_err_column=mapped(src.dec_err_column),
        corr_column=mapped(src.corr_column),
        astrometric_covariance_columns=cov,
        pos_err_units=src.pos_err_units or "arcsec",
        default_pos_error_arcsec=src.default_pos_error_arcsec,
        epoch=src.epoch,
        epoch_column=mapped(src.epoch_column),
        pm_ra_column=mapped(src.pm_ra_column),
        pm_dec_column=mapped(src.pm_dec_column),
        parallax_column=mapped(src.parallax_column),
        radial_velocity_column=mapped(src.radial_velocity_column),
        frame=src.frame or "icrs",
    )


def _survey_meta_columns(tag: str) -> list[str]:
    return [
        f"{tag}_sep_arcsec",
        f"{tag}_sep_second",
        f"{tag}_n_within",
        f"{tag}_crowded",
        f"{tag}_pm_warn",
        f"{tag}_quality",
    ]


def _candidate_partitions(
    anchor_part: PartitionPlan,
    survey: CataloguePlan,
    *,
    sep_arcsec: float,
    delta_deg: float,
) -> list[int]:
    if not survey.partitions:
        return []
    depth = max(p.order for p in survey.partitions)
    starts, ends, indices = _partition_index(survey, depth)
    cone_radius = min(180.0, 2.0 * (sep_arcsec / 3600.0 + delta_deg))
    seen: set[int] = set()
    for lo, hi in _cone_ranges(anchor_part.order, anchor_part.pix, cone_radius, depth):
        first = int(np.searchsorted(ends, lo, side="right"))
        last = int(np.searchsorted(starts, hi, side="left"))
        seen.update(indices[first:last].tolist())
    return sorted(seen)


def build_anchor_plan(
    anchor: CatalogueSource,
    surveys: Sequence[CatalogueSource],
    *,
    sep_arcsec: float,
    out_dir: str,
    cache_root: str | None = None,
    hats_threshold: int = 100_000,
    task_rows: int = DEFAULT_TASK_ROWS,
    chunk_memory_gb: float = DEFAULT_CHUNK_MEMORY_GB,
    target_epoch: float | None = None,
    anchor_id_name: str = "source_id",
    matcher: str = "sky",
    fallback_policy: str = "warn",
) -> AnchorPlan:
    """Build a picklable anchor plan; every source must point at a local HATS tree."""
    if not surveys:
        raise CrossMatchError("ray-anchor needs at least one survey after the anchor")
    if not math.isfinite(sep_arcsec) or sep_arcsec <= 0:
        raise CrossMatchError(f"radius_arcsec must be positive, got {sep_arcsec}")
    if matcher != "sky":
        raise CrossMatchError(
            "engine='ray-anchor' currently supports matcher='sky'; "
            f"got matcher='{matcher}'. Use ray-union for skyerr/skyellipse."
        )
    if task_rows <= 0 or chunk_memory_gb <= 0:
        raise CrossMatchError("task_rows and chunk_memory_gb must be positive")
    if anchor_id_name.strip() == "":
        raise CrossMatchError("anchor_id_name must not be empty")

    used_tags: set[str] = set()
    anchor_plan = _build_catalogue_plan(
        anchor,
        tag="anchor",
        is_anchor=True,
        anchor_id_name=anchor_id_name,
        cache_root=cache_root,
        hats_threshold=hats_threshold,
    )
    survey_plans: list[CataloguePlan] = []
    tags: list[str] = []
    for source in surveys:
        tag = _safe_tag(source.name, used_tags)
        survey_plans.append(
            _build_catalogue_plan(
                source,
                tag=tag,
                is_anchor=False,
                anchor_id_name=anchor_id_name,
                cache_root=cache_root,
                hats_threshold=hats_threshold,
            )
        )
        tags.append(tag)

    delta = 0.0
    for plan in (anchor_plan, *survey_plans):
        for part in plan.partitions:
            delta = max(delta, _pixel_diagonal_deg(part.order, part.pix))

    out_columns = list(anchor_plan.final_cols)
    for survey, tag in zip(survey_plans, tags, strict=True):
        out_columns.extend(survey.final_cols)
        out_columns.extend(_survey_meta_columns(tag))

    storages = [open_storage(cat.root) for cat in (anchor_plan, *survey_plans)]
    candidates: list[dict[str, list[int]]] = []
    for part in anchor_plan.partitions:
        cand: dict[str, list[int]] = {}
        est = 0
        for survey, tag, storage in zip(survey_plans, tags, storages[1:], strict=True):
            idxs = _candidate_partitions(
                part, survey, sep_arcsec=float(sep_arcsec), delta_deg=delta
            )
            cand[tag] = idxs
            est += sum(_est_rows(survey.partitions[i], storage, hats_threshold) for i in idxs)
        est += _est_rows(part, storages[0], hats_threshold)
        if est > task_rows:
            raise CrossMatchError(
                f"anchor partition {part.rel} needs ~{est} rows of candidate data, above "
                f"--task-rows={task_rows}; raise --task-rows or lower --hats-threshold"
            )
        if est * _BYTES_PER_ROW / 1024**3 > chunk_memory_gb:
            raise CrossMatchError(
                f"anchor partition {part.rel} needs ~{est} candidate rows "
                f"(~{est * _BYTES_PER_ROW / 1024**3:,.2f} GiB), above "
                f"--chunk-memory-gb={chunk_memory_gb}; raise it or lower --hats-threshold"
            )
        candidates.append(cand)

    return AnchorPlan(
        anchor=anchor_plan,
        surveys=survey_plans,
        survey_tags=tags,
        sep_arcsec=float(sep_arcsec),
        delta_deg=delta,
        out_dir=str(out_dir),
        anchor_id=anchor_id_name,
        out_columns=out_columns,
        candidates=candidates,
        target_epoch=float(target_epoch) if target_epoch is not None else None,
        anchor_epoch=anchor.epoch,
        matcher=matcher,
        fallback_policy=fallback_policy,
        chunk_memory_gb=float(chunk_memory_gb),
        task_rows=int(task_rows),
    )


# --------------------------------------------------------------------------- #
# task internals
# --------------------------------------------------------------------------- #
def _read_frame(
    cat: CataloguePlan,
    idxs: Sequence[int],
    plan: AnchorPlan,
) -> pl.DataFrame:
    """Read selected columns from the given partitions, rename, and align epochs."""
    if not idxs:
        return pl.DataFrame(
            {name: pl.Series(name, [], dtype=dtype) for name, dtype in cat.dtypes.items()}
        )
    frames: list[pl.DataFrame] = []
    storage = open_storage(cat.root)
    for index in idxs:
        part = cat.partitions[index]
        if isinstance(storage, LocalStorage):
            frame = pl.read_parquet(Path(storage.root) / part.rel, columns=cat.cols)
        else:
            frame = storage.read_parquet(part.rel).select(cat.cols)
        frames.append(frame)
    frame = pl.concat(frames, how="diagonal_relaxed")
    frame = frame.rename(dict(zip(cat.cols, cat.final_cols, strict=True)))
    _check_coords(frame, cat.ra, cat.dec, f"catalogue '{cat.name}'")
    if plan.target_epoch is not None and frame.height:
        src_meta = _plan_source(cat)
        empty_src = CatalogueSource(
            name="_empty", is_local=True, ra_column="_ra", dec_column="_dec"
        )
        empty = pl.DataFrame(
            {"_ra": pl.Series([], dtype=pl.Float64), "_dec": pl.Series([], dtype=pl.Float64)}
        )
        frame, _ = matchers._apply_proper_motion(
            frame,
            empty,
            src_meta,
            empty_src,
            float(plan.target_epoch),
            propagate_covariance=False,
            fallback_policy=plan.fallback_policy,
        )
    return frame


def _null_frame(cat: CataloguePlan, height: int) -> pl.DataFrame:
    return pl.DataFrame(
        {name: pl.Series(name, [None] * height, dtype=dtype) for name, dtype in cat.dtypes.items()}
    )


def _pm_warn(anchor_frame: pl.DataFrame, plan: AnchorPlan, survey: CataloguePlan) -> np.ndarray:
    height = anchor_frame.height
    warn = np.zeros(height, dtype=bool)
    if not height:
        return warn
    anchor_epoch = plan.target_epoch if plan.target_epoch is not None else plan.anchor_epoch
    survey_epoch = survey.epoch
    if anchor_epoch is None or survey_epoch is None:
        return warn
    dt = abs(float(anchor_epoch) - float(survey_epoch))
    if dt <= 0.0:
        return warn
    pmra_col = plan.anchor.pm_ra_column
    pmde_col = plan.anchor.pm_dec_column
    if (
        pmra_col
        and pmde_col
        and pmra_col in anchor_frame.columns
        and pmde_col in anchor_frame.columns
    ):
        pmra = anchor_frame[pmra_col].cast(pl.Float64).fill_null(0.0).to_numpy()
        pmde = anchor_frame[pmde_col].cast(pl.Float64).fill_null(0.0).to_numpy()
        drift_arcsec = np.hypot(pmra, pmde) * dt / 1000.0
        return drift_arcsec > 0.5 * plan.sep_arcsec
    # No usable anchor PM: flag long baselines rather than pretending the
    # source is stationary.
    warn[:] = dt > 5.0
    return warn


def _quality(
    counts: np.ndarray,
    best_sep: np.ndarray,
    pm_warn: np.ndarray,
    radius_arcsec: float,
) -> pl.Series:
    matched = counts > 0
    conditions = [
        ~matched,
        matched & (counts > 1) & pm_warn,
        matched & (counts > 1),
        matched & pm_warn,
        matched & (best_sep > 0.8 * radius_arcsec),
    ]
    choices = ["no_match", "ambiguous+pm_warn", "ambiguous", "pm_warn", "poor"]
    return pl.Series(np.select(conditions, choices, default="good").tolist(), dtype=pl.Utf8)


def _match_survey(
    anchor_frame: pl.DataFrame,
    survey_frame: pl.DataFrame,
    plan: AnchorPlan,
    survey: CataloguePlan,
    tag: str,
) -> pl.DataFrame:
    """Best counterpart + ambiguity block for one survey, one row per anchor row."""
    from scipy.spatial import cKDTree  # noqa: PLC0415

    height = anchor_frame.height
    if height == 0:
        return pl.DataFrame(
            {name: pl.Series(name, [], dtype=dtype) for name, dtype in survey.dtypes.items()}
        ).hstack(
            pl.DataFrame(
                {
                    _survey_meta_columns(tag)[0]: pl.Series([], dtype=pl.Float64),
                    _survey_meta_columns(tag)[1]: pl.Series([], dtype=pl.Float64),
                    _survey_meta_columns(tag)[2]: pl.Series([], dtype=pl.Int64),
                    _survey_meta_columns(tag)[3]: pl.Series([], dtype=pl.Boolean),
                    _survey_meta_columns(tag)[4]: pl.Series([], dtype=pl.Boolean),
                    _survey_meta_columns(tag)[5]: pl.Series([], dtype=pl.Utf8),
                }
            )
        )

    anchor_xyz = matchers._radec_to_xyz(
        anchor_frame[plan.anchor.ra].cast(pl.Float64).to_numpy(),
        anchor_frame[plan.anchor.dec].cast(pl.Float64).to_numpy(),
    )
    pm_warn = _pm_warn(anchor_frame, plan, survey)
    radius = float(plan.sep_arcsec)
    chord = matchers._arcsec_to_chord(radius)

    if survey_frame.height == 0:
        best_rows = _null_frame(survey, height)
        counts = np.zeros(height, dtype=np.int64)
        best_sep = np.full(height, np.nan)
        second_sep = np.full(height, np.nan)
    else:
        survey_xyz = matchers._radec_to_xyz(
            survey_frame[survey.ra].cast(pl.Float64).to_numpy(),
            survey_frame[survey.dec].cast(pl.Float64).to_numpy(),
        )
        tree = cKDTree(survey_xyz)
        dist, idx = tree.query(anchor_xyz, k=2, distance_upper_bound=chord, workers=-1)
        counts = np.asarray(
            tree.query_ball_point(anchor_xyz, chord, workers=-1, return_length=True),
            dtype=np.int64,
        )
        best_ok = dist[:, 0] <= chord
        best_idx = np.where(best_ok, idx[:, 0], -1).astype(np.int64)
        best_sep = np.where(best_ok, matchers._chord_to_arcsec(dist[:, 0]), np.nan)
        second_ok = dist[:, 1] <= chord
        second_sep = np.where(second_ok, matchers._chord_to_arcsec(dist[:, 1]), np.nan)
        # query_ball_point may see a candidate that the two-neighbour query
        # omits at the exact boundary; a best index of -1 with count > 0 is
        # inconsistent, so fall back to the nearest candidate explicitly.
        missing = (counts > 0) & ~best_ok
        if missing.any():
            for row in np.flatnonzero(missing):
                hit = np.asarray(tree.query_ball_point(anchor_xyz[row], chord), dtype=np.int64)
                if hit.size:
                    d = matchers._chord_to_arcsec(
                        np.linalg.norm(survey_xyz[hit] - anchor_xyz[row], axis=1)
                    )
                    order = np.argsort(d, kind="stable")
                    best_idx[row] = int(hit[order[0]])
                    best_sep[row] = float(d[order[0]])
                    if order.size > 1:
                        second_sep[row] = float(d[order[1]])
        best_rows = _gather_rows(survey_frame, best_idx)

    meta = pl.DataFrame(
        {
            f"{tag}_sep_arcsec": pl.Series(best_sep, dtype=pl.Float64).fill_nan(None),
            f"{tag}_sep_second": pl.Series(second_sep, dtype=pl.Float64).fill_nan(None),
            f"{tag}_n_within": pl.Series(counts, dtype=pl.Int64),
            f"{tag}_crowded": pl.Series(counts > 1, dtype=pl.Boolean),
            f"{tag}_pm_warn": pl.Series(pm_warn, dtype=pl.Boolean),
            f"{tag}_quality": _quality(counts, best_sep, pm_warn, radius),
        }
    )
    return best_rows.hstack(meta)


def _output_rel(part: PartitionPlan) -> str:
    return f"Norder={part.order}/Dir={_hats_dir(part.pix)}/Npix={part.pix}.parquet"


def run_anchor_partition(plan: AnchorPlan, index: int) -> dict[str, Any]:
    """Process one anchor partition and write its output partition."""
    part = plan.anchor.partitions[index]
    anchor_frame = _read_frame(plan.anchor, [index], plan)
    if anchor_frame.height == 0:
        return {"index": index, "rows": 0, "rel": _output_rel(part)}
    output = anchor_frame.select(plan.anchor.final_cols)
    for survey, tag in zip(plan.surveys, plan.survey_tags, strict=True):
        survey_frame = _read_frame(survey, plan.candidates[index].get(tag, []), plan)
        output = output.hstack(_match_survey(output, survey_frame, plan, survey, tag))
    output = output.select(plan.out_columns)
    dest = Path(plan.out_dir) / "dataset" / _output_rel(part)
    dest.parent.mkdir(parents=True, exist_ok=True)
    _atomic_write_parquet(output, dest)
    return {"index": index, "rows": output.height, "rel": _output_rel(part)}


def _anchor_task_factory() -> Any:
    import ray  # noqa: PLC0415

    @ray.remote
    def anchor_task(plan: AnchorPlan, index: int) -> dict[str, Any]:
        return run_anchor_partition(plan, index)

    return anchor_task


# --------------------------------------------------------------------------- #
# output metadata
# --------------------------------------------------------------------------- #
def _write_metadata(plan: AnchorPlan) -> dict[str, Any]:
    out = Path(plan.out_dir)
    dataset = out / "dataset"
    dataset.mkdir(parents=True, exist_ok=True)
    info_rows: list[dict[str, Any]] = []
    for part in plan.anchor.partitions:
        dest = dataset / _output_rel(part)
        if not dest.exists():
            continue
        count = _footer_row_count(LocalStorage(str(out)), str(dest.relative_to(out)))
        if count is None:
            count = 0
        info_rows.append(
            {
                "Norder": int(part.order),
                "Dir": int(_hats_dir(part.pix)),
                "Npix": int(part.pix),
                "Nfiles": 1,
                "file_loc": _output_rel(part).removesuffix(".parquet"),
                "file_size": int(dest.stat().st_size),
                "count": int(count),
            }
        )
    max_order = max((p.order for p in plan.anchor.partitions), default=0)
    epoch = plan.target_epoch if plan.target_epoch is not None else plan.anchor_epoch
    props = {
        "obs_collection": out.name or "xmatcher-anchor",
        "dataproduct_type": "object",
        "hats_col_ra": plan.anchor.ra,
        "hats_col_dec": plan.anchor.dec,
        "hats_epoch": f"J{float(epoch):.3f}" if epoch is not None else "J2000",
        "hats_ordering": "NESTED",
        "hats_max_depth": str(max_order),
        "hats_cols_default": " ".join(plan.out_columns),
        "hats_npix_suffix": ".parquet",
    }
    return _write_hats_tree(out, info_rows, props)


# --------------------------------------------------------------------------- #
# driver
# --------------------------------------------------------------------------- #
def _fingerprint(plan: AnchorPlan) -> dict[str, Any]:
    def cat_fp(cat: CataloguePlan) -> dict[str, Any]:
        return {
            "name": cat.name,
            "root": cat.root,
            "rel": cat.rel,
            "ra": cat.ra,
            "dec": cat.dec,
            "cols": cat.cols,
            "final_cols": cat.final_cols,
            "dtypes": {k: str(v) for k, v in cat.dtypes.items()},
            "partitions": [asdict(p) for p in cat.partitions],
        }

    return {
        "version": 1,
        "anchor": cat_fp(plan.anchor),
        "surveys": [cat_fp(c) for c in plan.surveys],
        "tags": plan.survey_tags,
        "sep_arcsec": plan.sep_arcsec,
        "target_epoch": plan.target_epoch,
        "anchor_id": plan.anchor_id,
        "out_columns": plan.out_columns,
    }


def _clean_output(out: Path) -> None:
    for name in ("dataset", "chunks"):
        path = out / name
        if path.exists():
            shutil.rmtree(path)
    for name in ("properties", "partition_info.csv", "partition_info.parquet"):
        (out / name).unlink(missing_ok=True)


def ray_anchor_match(
    anchor: CatalogueSource,
    surveys: Sequence[CatalogueSource],
    *,
    sep_arcsec: float,
    output_file: str,
    hats_threshold: int = 100_000,
    task_rows: int | None = None,
    chunk_memory_gb: float | None = None,
    cache_root: str | None = None,
    progress_cb: Callable[[str], None] | None = None,
    target_epoch: float | None = None,
    anchor_id_name: str = "source_id",
    matcher: str = "sky",
    fallback_policy: str = "warn",
) -> None:
    """Build a Gaia-anchored master HATS catalogue at ``output_file``."""
    out = Path(str(output_file))
    out.mkdir(parents=True, exist_ok=True)
    for source in (anchor, *surveys):
        root, rel = _catalogue_root(source, cache_root)
        if not root.startswith("vos:"):
            input_path = (Path(root) / rel).resolve()
            output_path = out.resolve()
            if output_path.is_relative_to(input_path) or input_path.is_relative_to(output_path):
                raise CrossMatchError("ray-anchor output must not overlap an input catalogue")

    global _LAST_PLAN
    plan = build_anchor_plan(
        anchor,
        surveys,
        sep_arcsec=float(sep_arcsec),
        out_dir=str(out),
        cache_root=cache_root,
        hats_threshold=int(hats_threshold),
        task_rows=int(task_rows or DEFAULT_TASK_ROWS),
        chunk_memory_gb=float(chunk_memory_gb or DEFAULT_CHUNK_MEMORY_GB),
        target_epoch=target_epoch,
        anchor_id_name=anchor_id_name,
        matcher=matcher,
        fallback_policy=fallback_policy,
    )
    _LAST_PLAN = plan

    fingerprint = _fingerprint(plan)
    state_path = out / _STATE_NAME
    prior = None
    if state_path.exists():
        try:
            prior = json.loads(state_path.read_text()).get("fingerprint")
        except (OSError, ValueError):
            prior = None
    if prior is None and any((out / sub).exists() for sub in ("dataset", "chunks")):
        raise CrossMatchError(
            "cannot resume an existing anchor output without a valid fingerprint; "
            "use a new output directory"
        )
    if prior is not None and prior != fingerprint:
        _clean_output(out)
    _atomic_write_text(
        json.dumps({"status": "running", "fingerprint": fingerprint}, indent=2), state_path
    )
    if progress_cb:
        progress_cb(
            f"anchor plan: {len(plan.anchor.partitions)} anchor partitions, "
            f"{len(plan.surveys)} survey(s), radius={plan.sep_arcsec} arcsec"
        )

    todo = [
        index
        for index in range(len(plan.anchor.partitions))
        if not (out / "dataset" / _output_rel(plan.anchor.partitions[index])).exists()
    ]
    started = time.time()
    if not todo:
        assembled = _write_metadata(plan)
        if progress_cb:
            progress_cb(f"anchor: {assembled['rows']} rows, reused existing partitions")
        _atomic_write_text(
            json.dumps(
                {"status": "done", "rows": assembled["rows"], "fingerprint": fingerprint},
                indent=2,
            ),
            state_path,
        )
        return

    ray: Any = None
    try:
        ray = importlib.import_module("ray")
    except ImportError:
        ray = None

    if ray is None:
        for done, index in enumerate(todo, start=1):
            run_anchor_partition(plan, index)
            if progress_cb and (done == len(todo) or done % 10 == 0):
                progress_cb(f"anchor: {done}/{len(todo)} partitions")
    else:
        owns_ray = not ray.is_initialized()
        if owns_ray:
            address = os.environ.get("RAY_ADDRESS")
            try:
                ray.init(address=address or "auto", ignore_reinit_error=True)
            except ConnectionError as exc:
                if address and fallback_policy == "error":
                    raise CrossMatchError(
                        "engine='ray-anchor' could not connect to RAY_ADDRESS under "
                        "fallback_policy='error'"
                    ) from exc
                if address:
                    logger.warning("Cannot connect to RAY_ADDRESS; starting local Ray.")
                ray.init(address="local", ignore_reinit_error=True)
        try:
            plan_ref = ray.put(plan)
            task = _anchor_task_factory()
            refs = [task.remote(plan_ref, index) for index in todo]
            done = 0
            while refs:
                ready, refs = ray.wait(refs, num_returns=min(32, len(refs)), timeout=5.0)
                if not ready:
                    continue
                ray.get(ready)
                done += len(ready)
                if progress_cb:
                    elapsed = max(time.time() - started, 1e-6)
                    progress_cb(
                        f"anchor: {done}/{len(todo)} partitions "
                        f"({100.0 * done / len(todo):.0f}%, {done / elapsed:.1f}/s)"
                    )
        finally:
            if owns_ray:
                ray.shutdown()

    assembled = _write_metadata(plan)
    _atomic_write_text(
        json.dumps(
            {"status": "done", "rows": assembled["rows"], "fingerprint": fingerprint}, indent=2
        ),
        state_path,
    )
    if progress_cb:
        progress_cb(
            f"anchor: {assembled['rows']} rows, {assembled['partitions']} output partitions"
        )


__all__ = [
    "AnchorPlan",
    "build_anchor_plan",
    "last_plan",
    "ray_anchor_match",
    "run_anchor_partition",
]
