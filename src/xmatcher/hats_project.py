"""Column projection for HATS trees.

A full HATS catalogue is often far wider than a crossmatch needs (Gaia DR3
has 225 columns, SDSS DR16 130).  Mirroring the whole tree and then selecting
columns at read time still pays for the bytes and the cache space.  This
module writes a narrow HATS tree with the same partition layout, so
``ray-anchor`` and ``ray-union`` can mirror only the columns they use.

The projection is deliberately structural: partition order/pixel, row counts,
and the ``properties``/``partition_info`` metadata are preserved.  It does not
re-tile or re-order the catalogue.
"""

from __future__ import annotations

import io
import json
import logging
import shutil
import tempfile
import urllib.parse
from collections.abc import Callable, Collection, Sequence
from pathlib import Path
from typing import Any, cast

import polars as pl

from .exceptions import CrossMatchError
from .ray_union import (
    _atomic_write_parquet,
    _atomic_write_text,
    _catalogue_root,
    _catalogue_schema,
    _hats_dir,
    _list_partitions,
    _parquet_row_count,
    _read_hats_properties,
    _write_hats_tree,
)
from .sources import CatalogueSource
from .storage import LocalStorage, open_storage

logger = logging.getLogger(__name__)


def _read_projected(storage: Any, part_rel: str, columns: Sequence[str]) -> pl.DataFrame:
    if isinstance(storage, LocalStorage):
        return pl.read_parquet(Path(storage.root) / part_rel, columns=list(columns))
    tmpdir = Path(tempfile.mkdtemp(prefix="xmatcher-project-"))
    try:
        local = tmpdir / "part.parquet"
        storage.stage_in(part_rel, local)
        return pl.read_parquet(local, columns=list(columns))
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def _validate_columns(schema: Collection[str], columns: Sequence[str], *, source: str) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for column in columns:
        if column in seen:
            continue
        if column not in schema:
            raise CrossMatchError(
                f"catalogue '{source}': column '{column}' is not in the HATS schema"
            )
        seen.add(column)
        ordered.append(column)
    if not ordered:
        raise CrossMatchError("project-hats needs at least one column")
    return ordered


_STATE_NAME = "project.state"


def _prepare_output(out: Path, fingerprint: dict[str, Any], *, resume: bool) -> None:
    """Validate/initialize a projection directory for a new or resumed run."""
    if not out.exists():
        out.mkdir(parents=True, exist_ok=True)
    elif not out.is_dir():
        raise CrossMatchError(f"project-hats output {out} is not a directory")
    elif any(out.iterdir()):
        if not resume:
            raise CrossMatchError(
                f"project-hats output {out} already exists and is not empty; write to a "
                "new directory, remove the old tree, or pass --resume"
            )
        state_path = out / _STATE_NAME
        try:
            prior = json.loads(state_path.read_text())
        except (OSError, ValueError) as exc:
            raise CrossMatchError(
                f"project-hats output {out} is non-empty but has no resume state; "
                "write to a new directory or remove the old tree"
            ) from exc
        if prior != fingerprint:
            raise CrossMatchError(
                f"project-hats resume state in {out} does not match this source/column "
                "projection; use a new output directory"
            )
    _atomic_write_text(json.dumps(fingerprint, indent=2, sort_keys=True), out / _STATE_NAME)


def _projected_props(
    props: dict[str, str], selected: Sequence[str], *, source: CatalogueSource
) -> dict[str, str]:
    """Source properties made true for a projection of ``selected`` columns.

    A copied ``properties`` file can name a coordinate column the projection
    dropped (``hats_col_ra``/``hats_col_dec``); keeping it would advertise a
    column the narrow tree does not have.  ``hats_nrows`` is recomputed by
    :func:`_write_hats_tree`.
    """
    out = dict(props)
    out.setdefault("obs_collection", source.name)
    out.setdefault("dataproduct_type", "object")
    for key in ("hats_col_ra", "hats_col_dec"):
        if key in out and out[key] not in selected:
            del out[key]
    out["hats_cols_default"] = " ".join(selected)
    if source.ra_column and source.ra_column in selected:
        out["hats_col_ra"] = source.ra_column
    if source.dec_column and source.dec_column in selected:
        out["hats_col_dec"] = source.dec_column
    return out


def _project_remote_http(
    source: CatalogueSource,
    output: Path,
    columns: Sequence[str],
    *,
    rate_limit_rps: float,
    resume: bool,
    progress_cb: Callable[[str], None] | None,
) -> dict[str, Any]:
    """Project an HTTPS HATS tree without mirroring every column first."""
    from .mirror import TokenBucket, _http_request, _remote_hats_listing

    root = str(source.access_identifier or "").rstrip("/")
    bucket = TokenBucket(rate_limit_rps or 0.0)
    files = _remote_hats_listing(source, bucket)
    pixel_files = [
        entry for entry in files if entry["rel"].endswith(".parquet") and "Norder=" in entry["rel"]
    ]
    if not pixel_files:
        raise CrossMatchError(f"catalogue '{source.name}' has no remote HATS partitions")

    import pyarrow.parquet as pq  # noqa: PLC0415

    schema = None
    meta_body, _ = _http_request(f"{root}/dataset/_common_metadata", bucket)
    if meta_body and meta_body.startswith(b"PAR1"):
        schema = pq.read_schema(io.BytesIO(meta_body))
    if schema is None:
        first_body, _ = _http_request(f"{root}/{pixel_files[0]['rel']}", bucket)
        schema = pq.read_schema(io.BytesIO(first_body))
    selected = _validate_columns(schema.names, columns, source=source.name)
    # CDS accepts up to eight comma-separated column names on a partition URL;
    # other servers ignore the parameter, in which case the first response is
    # the full row group and we select locally.
    chunks = [selected[i : i + 8] for i in range(0, len(selected), 8)]
    out = output
    info_rows: list[dict[str, Any]] = []
    for i, entry in enumerate(pixel_files):
        rel = entry["rel"]
        if not rel.startswith("dataset/"):
            rel = f"dataset/{rel}"
        dest = out / rel
        file_loc = rel.removeprefix("dataset/").removesuffix(".parquet")
        parts = file_loc.split("/")
        order = int(parts[0].split("=", 1)[1])
        directory = int(parts[1].split("=", 1)[1])
        pix = int(parts[2].split("=", 1)[1])
        if resume and dest.exists():
            count = _parquet_row_count(dest)
        else:
            frames: list[pl.DataFrame] = []
            for chunk in chunks:
                # CDS expects literal commas between column names;
                # percent-encoding them (`%2C`) is rejected with HTTP 400.
                # Encode individual names only (brackets and spaces are legal
                # column characters).
                query = "columns=" + ",".join(
                    urllib.parse.quote(column, safe="") for column in chunk
                )
                body, _ = _http_request(f"{root}/{entry['rel']}?{query}", bucket)
                body_schema = pq.read_schema(io.BytesIO(body))
                if set(body_schema.names) != set(chunk):
                    # The server ignored `columns`; read the whole row group
                    # once and project locally rather than repeating it.
                    table = pq.read_table(io.BytesIO(body), columns=selected)
                    frames = [cast(pl.DataFrame, pl.from_arrow(table))]
                    break
                frames.append(
                    cast(
                        pl.DataFrame,
                        pl.from_arrow(pq.read_table(io.BytesIO(body), columns=chunk)),
                    )
                )
            frame = frames[0]
            for extra in frames[1:]:
                frame = frame.hstack(extra.get_columns())
            dest.parent.mkdir(parents=True, exist_ok=True)
            _atomic_write_parquet(frame, dest)
            count = frame.height
        info_rows.append(
            {
                "Norder": order,
                "Dir": directory,
                "Npix": pix,
                "Nfiles": 1,
                "file_loc": file_loc,
                "file_size": int(dest.stat().st_size),
                "count": int(count),
            }
        )
        if progress_cb and (i + 1 == len(pixel_files) or (i + 1) % 100 == 0):
            progress_cb(f"project-hats: {i + 1}/{len(pixel_files)} remote partitions")

    props: dict[str, str] = {}
    for entry in files:
        if entry["rel"] in ("properties", "hats.properties"):
            body, _ = _http_request(f"{root}/{entry['rel']}", bucket)
            for line in body.decode("utf-8", "replace").splitlines():
                if "=" in line:
                    key, _, value = line.partition("=")
                    props[key.strip()] = value.strip()
            break
    stats = _write_hats_tree(out, info_rows, _projected_props(props, selected, source=source))
    return {"rows": stats["rows"], "columns": selected, "partitions": stats["partitions"]}


def project_hats(
    source: CatalogueSource,
    output: str | Path,
    columns: Sequence[str],
    *,
    cache_root: str | None = None,
    hats_threshold: int = 100_000,
    sync: bool = True,
    rate_limit_rps: float = 1.0,
    resume: bool = False,
    progress_cb: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    """Write a narrow HATS tree at ``output`` with only ``columns``.

    ``source`` may be a local HATS tree or a remote HATS/TAP catalogue.  An
    HTTPS HATS tree is projected partition-by-partition without mirroring the
    full tree first.  Other remote sources are mirrored unless ``sync=False``,
    in which case a cached copy must already exist.
    """
    from .mirror import _mirrored_source, ensure_mirrored, locate_mirrored

    out = Path(output).expanduser()
    if (
        sync
        and source.access_method == "hats"
        and isinstance(source.access_identifier, str)
        and source.access_identifier.startswith(("http://", "https://"))
    ):
        _prepare_output(
            out,
            {"source": source.access_identifier, "columns": list(columns)},
            resume=resume,
        )
        return _project_remote_http(
            source,
            out,
            columns,
            rate_limit_rps=rate_limit_rps,
            resume=resume,
            progress_cb=progress_cb,
        )

    if sync:
        mirrored = ensure_mirrored(
            source,
            cache_root=cache_root,
            hats_threshold=hats_threshold,
            progress_cb=progress_cb,
        )
    else:
        root, rel = locate_mirrored(source, cache_root=cache_root)
        mirrored = _mirrored_source(source, open_storage(root), rel, root)

    root, rel = _catalogue_root(mirrored, cache_root)
    storage = open_storage(root)
    parts = _list_partitions(rel, storage)
    if not parts:
        raise CrossMatchError(f"catalogue '{source.name}' has no HATS partitions")
    schema = _catalogue_schema(storage, parts[0].rel)
    if not schema:
        raise CrossMatchError(f"catalogue '{source.name}': cannot read its HATS schema")
    selected = _validate_columns(schema, columns, source=source.name)

    input_path = Path(storage.root) / rel if isinstance(storage, LocalStorage) else None
    if input_path is not None:
        in_res = input_path.resolve()
        out_res = out.resolve()
        if out_res.is_relative_to(in_res) or in_res.is_relative_to(out_res):
            raise CrossMatchError("project-hats output must not overlap its input")
    _prepare_output(
        out,
        {"source": str(input_path or root), "columns": selected},
        resume=resume,
    )
    dataset = out / "dataset"
    dataset.mkdir(parents=True, exist_ok=True)

    info_rows: list[dict[str, Any]] = []
    for i, part in enumerate(parts):
        rel_out = f"Norder={part.order}/Dir={_hats_dir(part.pix)}/Npix={part.pix}.parquet"
        dest = dataset / rel_out
        if resume and dest.exists():
            count = _parquet_row_count(dest)
        else:
            frame = _read_projected(storage, part.rel, selected)
            dest.parent.mkdir(parents=True, exist_ok=True)
            _atomic_write_parquet(frame, dest)
            count = frame.height
        info_rows.append(
            {
                "Norder": int(part.order),
                "Dir": int(_hats_dir(part.pix)),
                "Npix": int(part.pix),
                "Nfiles": 1,
                "file_loc": rel_out.removesuffix(".parquet"),
                "file_size": int(dest.stat().st_size),
                "count": int(count),
            }
        )
        if progress_cb and (i + 1 == len(parts) or (i + 1) % 100 == 0):
            progress_cb(f"project-hats: {i + 1}/{len(parts)} partitions")

    props = _read_hats_properties(storage, rel)
    stats = _write_hats_tree(out, info_rows, _projected_props(props, selected, source=source))
    logger.info("project-hats: wrote %d rows x %d columns to %s", stats["rows"], len(selected), out)
    return {"rows": stats["rows"], "columns": selected, "partitions": stats["partitions"]}


__all__ = ["project_hats"]
