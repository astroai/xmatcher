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

``anchor_partitions`` (CLI ``--anchor-partitions ORDER/PIX,…``) restricts a run
to a subset of anchor partitions, e.g. for a pilot; the output then advertises
the subset in ``properties`` (``xmatcher_anchor_partitions``).  Each task
records its wall time, peak resident memory and, per survey, the candidate
partitions and rows it read (``task_stats`` in ``resume.state``).
"""

from __future__ import annotations

import importlib
import json
import logging
import math
import re
import resource
import shutil
import sys
import time
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl

from . import _torchsky, matchers
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
    _init_ray,
    _input_fingerprint,
    _list_partitions,
    _partition_index,
    _plan_source,
    _read_hats_properties,
    _write_hats_tree,
)
from .sources import CatalogueSource
from .storage import LocalStorage, open_storage

logger = logging.getLogger(__name__)

DEFAULT_TASK_ROWS = 2_000_000
DEFAULT_CHUNK_MEMORY_GB = 8.0
#: Candidate halo added to each anchor cone when coordinates are propagated
#: to a target epoch: the anchor and survey motions may each use half of it.
DEFAULT_MOTION_HALO_ARCSEC = 600.0
_BYTES_PER_ROW = 96.0
#: Partition files read concurrently within one task (I/O latency bound).
DEFAULT_READ_THREADS = 8
#: Task memory model (see :func:`_task_memory_bytes`): process baseline, the
#: per-row cost of a survey's sky index and match arrays, and the per-row cost
#: of propagating a survey that moves.  Calibrated on the Gaia-master pilots.
_TASK_BASE_BYTES = 2 * 1024**3
_INDEX_BYTES_PER_ROW = 40
_MOTION_BYTES_PER_ROW = 56
_META_BYTES_PER_ROW = 40
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
    #: Motion halo (degrees) beyond ``radius`` in every anchor cone; 0 when no
    #: target epoch is requested.
    delta_deg: float
    out_dir: str
    anchor_id: str
    out_columns: list[str]
    candidates: list[dict[str, list[int]]] = field(default_factory=list)
    target_epoch: float | None = None
    anchor_epoch: float | None = None
    matcher: str = "sky"
    fallback_policy: str = "warn"
    chunk_memory_gb: float = DEFAULT_CHUNK_MEMORY_GB
    task_rows: int = DEFAULT_TASK_ROWS
    #: ``(order, pix)`` of the anchor partitions this plan is restricted to
    #: (``None``: every anchor partition).
    anchor_subset: list[tuple[int, int]] | None = None
    #: Estimated peak resident bytes per anchor partition (same order as
    #: ``anchor.partitions``); used to reserve Ray memory per task.
    task_memory: list[int] = field(default_factory=list)
    read_threads: int = DEFAULT_READ_THREADS


_DTYPE_BYTES = {
    pl.Boolean: 1,
    pl.Int8: 1,
    pl.UInt8: 1,
    pl.Int16: 2,
    pl.UInt16: 2,
    pl.Int32: 4,
    pl.UInt32: 4,
    pl.Float32: 4,
    pl.Int64: 8,
    pl.UInt64: 8,
    pl.Float64: 8,
}


def _row_bytes(dtypes: dict[str, Any]) -> int:
    """In-memory bytes per row of a projection (strings as 16-byte views)."""
    total = 0
    for dtype in dtypes.values():
        base = dtype.base_type() if hasattr(dtype, "base_type") else dtype
        total += _DTYPE_BYTES.get(base, 16)
    return total


def _moves(cat: CataloguePlan, target_epoch: float | None) -> bool:
    """Whether propagation to ``target_epoch`` can move this catalogue's rows."""
    return bool(
        target_epoch is not None
        and cat.pm_ra_column
        and cat.pm_dec_column
        and (cat.epoch_column or cat.epoch is not None)
    )


def _task_memory_bytes(
    anchor: CataloguePlan,
    anchor_rows: int,
    surveys: Sequence[CataloguePlan],
    survey_rows: Sequence[int],
    target_epoch: float | None,
) -> int:
    """Estimated peak resident bytes of one anchor task.

    Surveys are read and matched one at a time, so the peak is the output
    frame (anchor rows x the full output width) plus the largest single
    survey: its projected rows counted twice (the per-file frames and their
    concatenation can coexist), its sky index and, when it moves, the
    propagation arrays.  On the Gaia-master pilots this was 1.17x the
    measured peak RSS of the largest task (59.5 GiB, 289M Euclid DR1
    candidate rows) and 1.5x that of a many-survey HSC partition (19.6 GiB):
    conservative, so reservations do not overcommit a node.
    """
    out_width = _row_bytes(anchor.dtypes) + sum(
        _row_bytes(s.dtypes) + _META_BYTES_PER_ROW for s in surveys
    )
    largest = 0
    for survey, rows in zip(surveys, survey_rows, strict=True):
        per_row = 2 * _row_bytes(survey.dtypes) + _INDEX_BYTES_PER_ROW
        if _moves(survey, target_epoch):
            per_row += _MOTION_BYTES_PER_ROW
        largest = max(largest, int(rows) * per_row)
    return int(_TASK_BASE_BYTES + anchor_rows * out_width + largest)


def parse_anchor_partitions(
    value: str | Sequence[str] | Sequence[tuple[int, int]] | None,
) -> list[tuple[int, int]] | None:
    """Parse ``ORDER/PIX`` tokens (comma or space separated) into pairs.

    ``"2/69,6/42837"`` gives ``[(2, 69), (6, 42837)]``; pairs pass through.
    ``None`` or an empty value means "every anchor partition".
    """
    if value is None:
        return None
    items: list[Any]
    if isinstance(value, str):
        items = [t for t in re.split(r"[,\s]+", value.strip()) if t]
    else:
        items = list(value)
    out: list[tuple[int, int]] = []
    for item in items:
        if isinstance(item, str):
            match = re.fullmatch(r"(?:Norder=)?(\d+)/(?:Npix=)?(\d+)", item.strip())
            if match is None:
                raise CrossMatchError(
                    f"anchor partition {item!r}: expected ORDER/PIX (e.g. 2/69 or 6/42837)"
                )
            pair = (int(match.group(1)), int(match.group(2)))
        else:
            order, pix = item
            pair = (int(order), int(pix))
        if pair[0] < 0 or pair[0] > 29 or pair[1] < 0 or pair[1] >= 12 * 4 ** pair[0]:
            raise CrossMatchError(f"anchor partition {pair[0]}/{pair[1]} is not a HEALPix cell")
        if pair not in out:
            out.append(pair)
    return out or None


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
    # Row estimates (one footer read each) are made lazily for the partitions a
    # plan actually selects, not for every partition of every input.

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


def _cell_radius_deg(order: int, pix: int) -> float:
    """Largest angular distance from a HEALPix cell's centre to its boundary.

    The boundary is sampled at the vertices of the cell's descendants three
    orders deeper (eight points per edge), which bounds the curved HEALPix
    edges far more tightly than the corner-to-corner diagonal.
    """
    from .ray_union import _cdshealpix, _pixel_center_deg

    cds = _cdshealpix()
    sub = min(int(order) + 3, 29)
    shift = 2 * (sub - int(order))
    children = np.arange(int(pix) << shift, (int(pix) + 1) << shift, dtype=np.uint64)
    lon, lat = cds.vertices(children, sub)
    lon_v = np.asarray(lon.value, dtype=float).ravel()
    lat_v = np.asarray(lat.value, dtype=float).ravel()
    c_lon, c_lat = (math.radians(v) for v in _pixel_center_deg(int(order), int(pix)))
    cos_d = np.sin(lat_v) * math.sin(c_lat) + np.cos(lat_v) * math.cos(c_lat) * np.cos(
        lon_v - c_lon
    )
    radius = float(np.degrees(np.max(np.arccos(np.clip(cos_d, -1.0, 1.0)))))
    # Sampling error between boundary points is far below 1e-3 of the radius.
    return radius * (1.0 + 1e-3)


def anchor_cone_radius_deg(part: PartitionPlan, sep_arcsec: float, halo_deg: float) -> float:
    """Search-cone radius for one anchor partition: its own centre-to-boundary
    radius plus the match radius and the motion halo (never a global maximum)."""
    return min(180.0, _cell_radius_deg(part.order, part.pix) + sep_arcsec / 3600.0 + halo_deg)


def _candidate_partitions(
    anchor_part: PartitionPlan,
    survey: CataloguePlan,
    *,
    cone_radius_deg: float,
) -> list[int]:
    """Survey partitions whose own cells overlap the anchor partition's cone."""
    if not survey.partitions:
        return []
    depth = max(p.order for p in survey.partitions)
    starts, ends, indices = _partition_index(survey, depth)
    seen: set[int] = set()
    for lo, hi in _cone_ranges(anchor_part.order, anchor_part.pix, cone_radius_deg, depth):
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
    motion_halo_arcsec: float | None = None,
    anchor_partitions: str | Sequence[str] | Sequence[tuple[int, int]] | None = None,
    read_threads: int = DEFAULT_READ_THREADS,
) -> AnchorPlan:
    """Build a picklable anchor plan; every source must point at a local HATS tree.

    Each anchor partition reads only the survey partitions whose cells overlap
    its own cone: the partition's centre-to-boundary radius plus
    ``sep_arcsec`` plus a motion halo (``motion_halo_arcsec``; default
    :data:`DEFAULT_MOTION_HALO_ARCSEC` with ``target_epoch``, else 0).  The
    plan therefore scales with the anchor partition, not with the coarsest
    partition of any input.

    ``anchor_partitions`` (``ORDER/PIX`` tokens or pairs) keeps only those
    anchor partitions; naming one the anchor tree does not have is an error.
    """
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
    if read_threads < 1:
        raise CrossMatchError(f"read_threads must be >= 1, got {read_threads}")
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
    subset = parse_anchor_partitions(anchor_partitions)
    if subset is not None:
        by_cell = {(p.order, p.pix): p for p in anchor_plan.partitions}
        missing = [f"{o}/{p}" for o, p in subset if (o, p) not in by_cell]
        if missing:
            orders = sorted({p.order for p in anchor_plan.partitions})
            raise CrossMatchError(
                f"anchor partition(s) {', '.join(missing)} are not in the anchor tree "
                f"'{anchor.name}' (its partitions have orders {orders})"
            )
        keep = set(subset)
        anchor_plan.partitions = [p for p in anchor_plan.partitions if (p.order, p.pix) in keep]
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

    if motion_halo_arcsec is None:
        motion_halo_arcsec = DEFAULT_MOTION_HALO_ARCSEC if target_epoch is not None else 0.0
    if not math.isfinite(motion_halo_arcsec) or motion_halo_arcsec < 0:
        raise CrossMatchError(f"motion_halo_arcsec must be >= 0, got {motion_halo_arcsec}")
    delta = float(motion_halo_arcsec) / 3600.0

    out_columns = list(anchor_plan.final_cols)
    for survey, tag in zip(survey_plans, tags, strict=True):
        out_columns.extend(survey.final_cols)
        out_columns.extend(_survey_meta_columns(tag))

    storages = [open_storage(cat.root) for cat in (anchor_plan, *survey_plans)]
    candidates: list[dict[str, list[int]]] = []
    task_memory: list[int] = []
    for part in anchor_plan.partitions:
        cand: dict[str, list[int]] = {}
        est = 0
        survey_rows: list[int] = []
        cone = anchor_cone_radius_deg(part, float(sep_arcsec), delta)
        for survey, tag, storage in zip(survey_plans, tags, storages[1:], strict=True):
            idxs = _candidate_partitions(part, survey, cone_radius_deg=cone)
            cand[tag] = idxs
            rows = sum(_est_rows(survey.partitions[i], storage, hats_threshold) for i in idxs)
            survey_rows.append(rows)
            est += rows
        anchor_rows = _est_rows(part, storages[0], hats_threshold)
        est += anchor_rows
        task_memory.append(
            _task_memory_bytes(anchor_plan, anchor_rows, survey_plans, survey_rows, target_epoch)
        )
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
        anchor_subset=subset,
        task_memory=task_memory,
        read_threads=int(read_threads),
    )


# --------------------------------------------------------------------------- #
# task internals
# --------------------------------------------------------------------------- #
def _valid_coords(frame: pl.DataFrame, ra: str, dec: str) -> np.ndarray:
    ra_v = frame[ra].cast(pl.Float64).fill_null(np.nan).to_numpy()
    dec_v = frame[dec].cast(pl.Float64).fill_null(np.nan).to_numpy()
    return np.isfinite(ra_v) & np.isfinite(dec_v) & (dec_v >= -90.0) & (dec_v <= 90.0)


def _read_frame(
    cat: CataloguePlan,
    idxs: Sequence[int],
    plan: AnchorPlan,
    *,
    invalid: dict[int, int] | None = None,
    stats: dict[str, Any] | None = None,
) -> pl.DataFrame:
    """Read selected columns from the given partitions, rename, and align epochs.

    With ``invalid`` (a survey read), rows with a non-finite coordinate or
    ``|dec| > 90`` are dropped and counted per partition index into it;
    without it (the anchor) such rows abort the task.  ``stats`` receives the
    partitions and rows read and the read seconds.
    """
    started = time.perf_counter()
    if stats is not None:
        stats.update(partitions=len(idxs), rows=0, read_s=0.0)
    if not idxs:
        return pl.DataFrame(
            {name: pl.Series(name, [], dtype=dtype) for name, dtype in cat.dtypes.items()}
        )
    storage = open_storage(cat.root)

    def read_one(index: int) -> tuple[int, pl.DataFrame, int]:
        part = cat.partitions[index]
        if isinstance(storage, LocalStorage):
            frame = pl.read_parquet(Path(storage.root) / part.rel, columns=cat.cols)
        else:
            frame = storage.read_parquet(part.rel).select(cat.cols)
        frame = frame.rename(dict(zip(cat.cols, cat.final_cols, strict=True)))
        bad = 0
        if invalid is not None and frame.height:
            ok = _valid_coords(frame, cat.ra, cat.dec)
            bad = int(frame.height - int(ok.sum()))
            if bad:
                frame = frame.filter(pl.Series(ok))
        return int(index), frame, bad

    # Files are read concurrently (I/O latency bound) and concatenated in
    # partition order, so the result and tie-breaking do not depend on timing.
    threads = max(1, min(int(plan.read_threads), len(idxs)))
    if threads == 1:
        results = [read_one(i) for i in idxs]
    else:
        with ThreadPoolExecutor(max_workers=threads) as pool:
            results = list(pool.map(read_one, idxs))
    frames: list[pl.DataFrame] = []
    for index, frame, bad in results:
        if bad and invalid is not None:
            invalid[index] = bad
        frames.append(frame)
    del results
    frame = pl.concat(frames, how="diagonal_relaxed")
    del frames
    if stats is not None:
        stats.update(rows=int(frame.height), read_s=round(time.perf_counter() - started, 3))
    if invalid is None:
        _check_coords(frame, cat.ra, cat.dec, f"catalogue '{cat.name}'")
    # A catalogue without motion metadata cannot move: skip the propagation and
    # its full-size coordinate copies.
    if _moves(cat, plan.target_epoch) and frame.height:
        src_meta = _plan_source(cat)
        empty_src = CatalogueSource(
            name="_empty", is_local=True, ra_column="_ra", dec_column="_dec"
        )
        empty = pl.DataFrame(
            {"_ra": pl.Series([], dtype=pl.Float64), "_dec": pl.Series([], dtype=pl.Float64)}
        )
        before = matchers._radec_to_xyz(
            frame[cat.ra].cast(pl.Float64).to_numpy(), frame[cat.dec].cast(pl.Float64).to_numpy()
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
        after = matchers._radec_to_xyz(
            frame[cat.ra].cast(pl.Float64).to_numpy(), frame[cat.dec].cast(pl.Float64).to_numpy()
        )
        moved = np.linalg.norm(after - before, axis=1)
        moved_deg = matchers._chord_to_arcsec(float(np.nanmax(moved))) / 3600.0
        # Survey candidates are chosen from the catalogue-epoch tiling with a
        # cone of (cell radius + radius + halo) around each anchor cell centre;
        # the halo is split between anchor and survey motion.  A larger move
        # could pair rows the cone never read, so it is an error rather than
        # a silent miss.
        limit_deg = 0.5 * plan.delta_deg
        if moved_deg > limit_deg:
            raise CrossMatchError(
                f"catalogue '{cat.name}': propagation to epoch {plan.target_epoch} moves a "
                f"source by {moved_deg * 3600.0:.1f} arcsec, beyond the "
                f"{limit_deg * 3600.0:.1f} arcsec candidate halo; check its proper-motion "
                "columns and units, raise motion_halo_arcsec, or match at the catalogue epoch"
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
        pmra = anchor_frame[pmra_col].cast(pl.Float64).fill_null(np.nan).to_numpy()
        pmde = anchor_frame[pmde_col].cast(pl.Float64).fill_null(np.nan).to_numpy()
        drift_arcsec = np.hypot(pmra, pmde) * dt / 1000.0
        usable = np.isfinite(drift_arcsec)
        # Rows without usable motion (Gaia two-parameter solutions) get the
        # same long-baseline flag as an anchor with no motion columns.
        return np.where(usable, drift_arcsec > 0.5 * plan.sep_arcsec, dt > 5.0)
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

    pm_warn = _pm_warn(anchor_frame, plan, survey)
    radius = float(plan.sep_arcsec)
    matchers._arcsec_to_chord(radius)  # rejects a non-finite or negative radius

    if survey_frame.height == 0:
        best_rows = _null_frame(survey, height)
        counts = np.zeros(height, dtype=np.int64)
        best_sep = np.full(height, np.nan)
        second_sep = np.full(height, np.nan)
    else:
        # torchsky gives the two nearest and the count under one inclusive
        # radius rule (sep <= radius), with ties to the lowest survey row.
        catalogs = _torchsky.require("catalogs", "ray-anchor")
        a_ra = matchers._ts_tensor(anchor_frame[plan.anchor.ra].cast(pl.Float64).to_numpy())
        a_dec = matchers._ts_tensor(anchor_frame[plan.anchor.dec].cast(pl.Float64).to_numpy())
        s_ra = matchers._ts_tensor(survey_frame[survey.ra].cast(pl.Float64).to_numpy())
        s_dec = matchers._ts_tensor(survey_frame[survey.dec].cast(pl.Float64).to_numpy())
        index = catalogs.SkyIndex.build(s_ra, s_dec)
        near = catalogs.match_nearest(
            a_ra, a_dec, s_ra, s_dec, k=2, max_radius_arcsec=radius, index=index
        )
        counts = catalogs.count_within(a_ra, a_dec, s_ra, s_dec, radius, index=index)
        counts = counts.numpy().astype(np.int64)
        idx = near.index.numpy().astype(np.int64)
        sep = near.separation_arcsec.numpy().astype(float)
        best_idx = idx[:, 0]
        best_sep = np.where(idx[:, 0] >= 0, sep[:, 0], np.nan)
        second_sep = np.where(idx[:, 1] >= 0, sep[:, 1], np.nan)
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


def _peak_rss_gib() -> float:
    """Peak resident set size of this process, GiB (``ru_maxrss``)."""
    peak = float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    # Linux reports KiB, macOS bytes.
    return round(peak / (1024**3 if sys.platform == "darwin" else 1024**2), 3)


def run_anchor_partition(plan: AnchorPlan, index: int) -> dict[str, Any]:
    """Process one anchor partition and write its output partition.

    The result carries ``stats``: wall seconds, the process's peak resident
    memory (per task under Ray, where each task gets a fresh worker; a running
    maximum on the serial path) and, per survey, the candidate partitions,
    rows read, invalid rows dropped, matched anchor rows and read/match
    seconds.
    """
    started = time.perf_counter()
    part = plan.anchor.partitions[index]
    anchor_stats: dict[str, Any] = {}
    anchor_frame = _read_frame(plan.anchor, [index], plan, stats=anchor_stats)
    stats: dict[str, Any] = {"anchor_rows": int(anchor_frame.height), "surveys": {}}

    def finish(result: dict[str, Any]) -> dict[str, Any]:
        stats["seconds"] = round(time.perf_counter() - started, 3)
        stats["peak_rss_gib"] = _peak_rss_gib()
        if plan.task_memory:
            stats["memory_estimate_gib"] = round(plan.task_memory[index] / 1024**3, 3)
        stats["anchor_read_s"] = anchor_stats.get("read_s", 0.0)
        result["stats"] = stats
        return result

    if anchor_frame.height == 0:
        return finish({"index": index, "rows": 0, "rel": _output_rel(part), "invalid": {}})
    output = anchor_frame.select(plan.anchor.final_cols)
    invalid: dict[str, dict[int, int]] = {}
    for survey, tag in zip(plan.surveys, plan.survey_tags, strict=True):
        dropped: dict[int, int] = {}
        survey_stats: dict[str, Any] = {}
        survey_frame = _read_frame(
            survey,
            plan.candidates[index].get(tag, []),
            plan,
            invalid=dropped,
            stats=survey_stats,
        )
        if dropped:
            invalid[tag] = dropped
        match_started = time.perf_counter()
        block = _match_survey(output, survey_frame, plan, survey, tag)
        del survey_frame
        survey_stats.update(
            invalid=int(sum(dropped.values())),
            matched=int((block[f"{tag}_n_within"] > 0).sum()),
            match_s=round(time.perf_counter() - match_started, 3),
        )
        stats["surveys"][tag] = survey_stats
        output = output.hstack(block)
    output = output.select(plan.out_columns)
    dest = Path(plan.out_dir) / "dataset" / _output_rel(part)
    dest.parent.mkdir(parents=True, exist_ok=True)
    _atomic_write_parquet(output, dest)
    return finish(
        {"index": index, "rows": output.height, "rel": _output_rel(part), "invalid": invalid}
    )


def _node_memory_cap(ray: Any) -> int:
    """90% of the largest alive node's schedulable Ray memory (0 if unknown)."""
    try:
        sizes = [
            float((node.get("Resources") or {}).get("memory", 0.0))
            for node in ray.nodes()
            if node.get("Alive", node.get("alive", True))
        ]
    except Exception:  # noqa: BLE001 - scheduling hint only
        return 0
    return int(0.9 * max(sizes, default=0.0))


def _task_reservation(plan: AnchorPlan, index: int, scale: float, cap: int) -> int:
    """Ray ``memory`` to reserve for one task (0: no reservation)."""
    if scale <= 0 or cap <= 0 or not plan.task_memory:
        return 0
    return int(min(plan.task_memory[index] * scale, cap))


def _anchor_task_factory() -> Any:
    import ray  # noqa: PLC0415

    # One task per worker process: tasks are large and memory-heavy, a fresh
    # process returns its memory, and ``ru_maxrss`` is then a per-task peak.
    @ray.remote(max_calls=1)
    def anchor_task(plan: AnchorPlan, index: int) -> dict[str, Any]:
        return run_anchor_partition(plan, index)

    return anchor_task


# --------------------------------------------------------------------------- #
# output metadata
# --------------------------------------------------------------------------- #
def _merge_invalid(
    acc: dict[str, dict[str, int]], result: dict[str, Any] | None
) -> dict[str, dict[str, int]]:
    """Fold one task's dropped-row counts into ``acc`` (keyed by survey
    partition, so a partition read by several anchor tasks counts once)."""
    for tag, counts in ((result or {}).get("invalid") or {}).items():
        slot = acc.setdefault(str(tag), {})
        for idx, n in counts.items():
            slot[str(idx)] = int(n)
    return acc


def _invalid_totals(acc: dict[str, dict[str, int]]) -> dict[str, int]:
    return {tag: sum(counts.values()) for tag, counts in sorted(acc.items()) if counts}


def _write_metadata(
    plan: AnchorPlan, invalid: dict[str, dict[str, int]] | None = None
) -> dict[str, Any]:
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
    totals = _invalid_totals(invalid or {})
    # Survey rows with non-finite coordinates or |dec| > 90 were dropped
    # before matching; record how many per survey (0 when none).
    props["xmatcher_invalid_survey_rows"] = (
        " ".join(f"{tag}:{totals.get(tag, 0)}" for tag in plan.survey_tags) or "none"
    )
    if plan.anchor_subset is not None:
        # A partial master (pilot): name the anchor partitions it covers.
        props["xmatcher_anchor_partitions"] = " ".join(f"{o}/{p}" for o, p in plan.anchor_subset)
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
            "partitions": [[p.order, p.pix, p.rel] for p in cat.partitions],
            # Epoch and motion metadata drive propagation and pm_warn.
            "epoch": cat.epoch,
            "epoch_column": cat.epoch_column,
            "pm_ra_column": cat.pm_ra_column,
            "pm_dec_column": cat.pm_dec_column,
            "parallax_column": cat.parallax_column,
            "radial_velocity_column": cat.radial_velocity_column,
            "frame": cat.frame,
        }

    return {
        # 3: n_within/sep_arcsec/sep_second share one inclusive radius rule.
        "version": 3,
        "anchor": cat_fp(plan.anchor),
        "surveys": [cat_fp(c) for c in plan.surveys],
        "tags": plan.survey_tags,
        "sep_arcsec": plan.sep_arcsec,
        "target_epoch": plan.target_epoch,
        "anchor_epoch": plan.anchor_epoch,
        "fallback_policy": plan.fallback_policy,
        "anchor_id": plan.anchor_id,
        "out_columns": plan.out_columns,
        "input_files": _input_fingerprint([plan.anchor, *plan.surveys]),
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
    motion_halo_arcsec: float | None = None,
    anchor_partitions: str | Sequence[str] | Sequence[tuple[int, int]] | None = None,
    read_threads: int | None = None,
    task_memory_scale: float | None = None,
) -> None:
    """Build a Gaia-anchored master HATS catalogue at ``output_file``.

    Survey rows with a non-finite coordinate or ``|dec| > 90`` are dropped
    (counted per survey in the output ``properties`` as
    ``xmatcher_invalid_survey_rows`` and in ``resume.state``); invalid anchor
    rows still abort, since every anchor row must appear in the master.
    ``anchor_partitions`` restricts the run to those anchor partitions.

    Under Ray each task reserves ``task_memory_scale`` x its estimated peak
    memory (:func:`_task_memory_bytes`; capped at 90% of the largest node's
    schedulable memory), so memory-heavy partitions are not packed onto one
    node; ``task_memory_scale=0`` schedules by CPU only.  ``read_threads``
    partition files are read concurrently within a task.
    """
    scale = 1.0 if task_memory_scale is None else float(task_memory_scale)
    if not math.isfinite(scale) or scale < 0:
        raise CrossMatchError(f"task_memory_scale must be >= 0, got {task_memory_scale}")
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
        motion_halo_arcsec=motion_halo_arcsec,
        anchor_partitions=anchor_partitions,
        read_threads=int(read_threads or DEFAULT_READ_THREADS),
    )
    _LAST_PLAN = plan

    fingerprint = _fingerprint(plan)
    state_path = out / _STATE_NAME
    prior = None
    invalid: dict[str, dict[str, int]] = {}
    task_stats: dict[str, Any] = {}
    if state_path.exists():
        try:
            prior_state = json.loads(state_path.read_text())
            prior = prior_state.get("fingerprint")
            if prior == fingerprint:
                invalid = {
                    str(k): {str(i): int(n) for i, n in v.items()}
                    for k, v in (prior_state.get("invalid_rows") or {}).items()
                }
                task_stats = dict(prior_state.get("task_stats") or {})
        except (OSError, ValueError, AttributeError, TypeError):
            prior = None
    if prior is None and any((out / sub).exists() for sub in ("dataset", "chunks")):
        raise CrossMatchError(
            "cannot resume an existing anchor output without a valid fingerprint; "
            "use a new output directory"
        )
    if prior is not None and prior != fingerprint:
        _clean_output(out)

    def write_state(status: str, **extra: Any) -> None:
        _atomic_write_text(
            json.dumps(
                {
                    "status": status,
                    **extra,
                    "fingerprint": fingerprint,
                    "invalid_rows": invalid,
                    "task_stats": task_stats,
                },
                indent=2,
            ),
            state_path,
        )

    def record(result: dict[str, Any] | None) -> None:
        _merge_invalid(invalid, result)
        stats = (result or {}).get("stats")
        if not stats:
            return
        task_stats[str(result["rel"])] = stats  # type: ignore[index]
        logger.info(
            "anchor task %s: %d rows, %.1f s, peak RSS %.2f GiB, %d survey rows read",
            result["rel"],  # type: ignore[index]
            int(stats.get("anchor_rows", 0)),
            float(stats.get("seconds", 0.0)),
            float(stats.get("peak_rss_gib", 0.0)),
            sum(int(s.get("rows", 0)) for s in (stats.get("surveys") or {}).values()),
        )

    write_state("running")
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
        assembled = _write_metadata(plan, invalid)
        if progress_cb:
            progress_cb(f"anchor: {assembled['rows']} rows, reused existing partitions")
        write_state("done", rows=assembled["rows"])
        return

    ray: Any = None
    try:
        ray = importlib.import_module("ray")
    except ImportError:
        ray = None

    if ray is None:
        for done, index in enumerate(todo, start=1):
            record(run_anchor_partition(plan, index))
            if done % 50 == 0:
                write_state("running")
            if progress_cb and (done == len(todo) or done % 10 == 0):
                progress_cb(f"anchor: {done}/{len(todo)} partitions")
    else:
        owns_ray = not ray.is_initialized()
        if owns_ray:
            _init_ray(ray, engine="ray-anchor", fallback_policy=fallback_policy)
        try:
            plan_ref = ray.put(plan)
            task = _anchor_task_factory()
            cap = _node_memory_cap(ray)
            refs = []
            for index in todo:
                memory = _task_reservation(plan, index, scale, cap)
                refs.append(
                    task.options(memory=memory).remote(plan_ref, index)
                    if memory
                    else task.remote(plan_ref, index)
                )
            if progress_cb and scale > 0 and cap:
                need = max(plan.task_memory[i] for i in todo) * scale
                progress_cb(
                    f"anchor: reserving task memory (largest {need / 1024**3:.1f} GiB, "
                    f"node cap {cap / 1024**3:.1f} GiB)"
                )
            done = 0
            while refs:
                ready, refs = ray.wait(refs, num_returns=min(32, len(refs)), timeout=5.0)
                if not ready:
                    continue
                for result in ray.get(ready):
                    record(result)
                write_state("running")
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

    assembled = _write_metadata(plan, invalid)
    write_state("done", rows=assembled["rows"])
    totals = _invalid_totals(invalid)
    if totals:
        logger.warning(
            "ray-anchor dropped survey rows with invalid coordinates: %s",
            ", ".join(f"{tag}={n}" for tag, n in totals.items()),
        )
    if progress_cb:
        progress_cb(
            f"anchor: {assembled['rows']} rows, {assembled['partitions']} output partitions"
        )


__all__ = [
    "DEFAULT_MOTION_HALO_ARCSEC",
    "AnchorPlan",
    "anchor_cone_radius_deg",
    "build_anchor_plan",
    "last_plan",
    "parse_anchor_partitions",
    "ray_anchor_match",
    "run_anchor_partition",
]
