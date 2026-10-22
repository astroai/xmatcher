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
import logging
import shutil
import tempfile
from collections.abc import Callable, Collection, Sequence
from pathlib import Path
from typing import Any

import polars as pl

from .exceptions import CrossMatchError
from .ray_union import (
    _atomic_write_csv,
    _atomic_write_parquet,
    _atomic_write_text,
    _catalogue_root,
    _catalogue_schema,
    _hats_dir,
    _list_partitions,
    _read_hats_properties,
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


def _write_partition_info(
    out: Path, info_rows: list[dict[str, Any]], total: int, props: dict[str, str]
) -> int:
    schema_info = {
        "Norder": pl.Int64,
        "Dir": pl.Int64,
        "Npix": pl.Int64,
        "Nfiles": pl.Int64,
        "file_loc": pl.Utf8,
        "file_size": pl.Int64,
        "count": pl.Int64,
    }
    info = pl.DataFrame(info_rows, schema=schema_info)
    dataset = out / "dataset"
    dataset.mkdir(parents=True, exist_ok=True)
    _atomic_write_csv(info, dataset / "partition_info.csv")
    _atomic_write_csv(info, out / "partition_info.csv")
    _atomic_write_parquet(info, dataset / "partition_info.parquet")
    props = dict(props)
    props.update(
        {
            "dataproduct_type": props.get("dataproduct_type", "object"),
            "hats_nrows": str(total),
        }
    )
    _atomic_write_text("".join(f"{k}={v}\n" for k, v in sorted(props.items())), out / "properties")
    _atomic_write_text(
        "".join(f"{k}={v}\n" for k, v in sorted(props.items())), dataset / "properties"
    )
    if info.height:
        try:
            import pyarrow.parquet as pq  # noqa: PLC0415

            first_rel = str(info["file_loc"][0])
            arrow_schema = pq.read_schema(dataset / f"{first_rel}.parquet")
            pq.write_metadata(arrow_schema, dataset / "_common_metadata")
            pq.write_metadata(arrow_schema, dataset / "_metadata")
        except Exception:  # noqa: BLE001
            logger.warning("skipping HATS metadata for %s", out)
    return info.height


def _project_remote_http(
    source: CatalogueSource,
    output: Path,
    columns: Sequence[str],
    *,
    rate_limit_rps: float,
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

    first_body, _ = _http_request(f"{root}/{pixel_files[0]['rel']}", bucket)
    import pyarrow.parquet as pq  # noqa: PLC0415

    first_schema = pq.read_schema(io.BytesIO(first_body))
    selected = _validate_columns(first_schema.names, columns, source=source.name)
    out = output
    info_rows: list[dict[str, Any]] = []
    total = 0
    for i, entry in enumerate(pixel_files):
        body, _ = _http_request(f"{root}/{entry['rel']}", bucket)
        table = pq.read_table(io.BytesIO(body), columns=selected)
        frame = pl.from_arrow(table)
        rel = entry["rel"]
        dest = out / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        _atomic_write_parquet(frame, dest)
        file_loc = rel.removeprefix("dataset/").removesuffix(".parquet")
        parts = file_loc.split("/")
        order = int(parts[0].split("=", 1)[1])
        directory = int(parts[1].split("=", 1)[1])
        pix = int(parts[2].split("=", 1)[1])
        info_rows.append(
            {
                "Norder": order,
                "Dir": directory,
                "Npix": pix,
                "Nfiles": 1,
                "file_loc": file_loc,
                "file_size": int(dest.stat().st_size),
                "count": int(frame.height),
            }
        )
        total += frame.height
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
    props["hats_cols_default"] = " ".join(selected)
    if source.ra_column and source.ra_column in selected:
        props["hats_col_ra"] = source.ra_column
    if source.dec_column and source.dec_column in selected:
        props["hats_col_dec"] = source.dec_column
    partitions = _write_partition_info(out, info_rows, total, props)
    return {"rows": total, "columns": selected, "partitions": partitions}


def project_hats(
    source: CatalogueSource,
    output: str | Path,
    columns: Sequence[str],
    *,
    cache_root: str | None = None,
    hats_threshold: int = 100_000,
    sync: bool = True,
    rate_limit_rps: float = 1.0,
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
        return _project_remote_http(
            source,
            out,
            columns,
            rate_limit_rps=rate_limit_rps,
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
        if source.access_method == "hats" and not source.hats_cache_rel:
            mirrored = source
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
    dataset = out / "dataset"
    dataset.mkdir(parents=True, exist_ok=True)

    info_rows: list[dict[str, Any]] = []
    total = 0
    for i, part in enumerate(parts):
        frame = _read_projected(storage, part.rel, selected)
        if frame.height == 0:
            continue
        rel_out = f"Norder={part.order}/Dir={_hats_dir(part.pix)}/Npix={part.pix}.parquet"
        dest = dataset / rel_out
        dest.parent.mkdir(parents=True, exist_ok=True)
        _atomic_write_parquet(frame, dest)
        info_rows.append(
            {
                "Norder": int(part.order),
                "Dir": int(_hats_dir(part.pix)),
                "Npix": int(part.pix),
                "Nfiles": 1,
                "file_loc": rel_out.removesuffix(".parquet"),
                "file_size": int(dest.stat().st_size),
                "count": int(frame.height),
            }
        )
        total += frame.height
        if progress_cb and (i + 1 == len(parts) or (i + 1) % 100 == 0):
            progress_cb(f"project-hats: {i + 1}/{len(parts)} partitions")

    schema_info = {
        "Norder": pl.Int64,
        "Dir": pl.Int64,
        "Npix": pl.Int64,
        "Nfiles": pl.Int64,
        "file_loc": pl.Utf8,
        "file_size": pl.Int64,
        "count": pl.Int64,
    }
    info = pl.DataFrame(info_rows, schema=schema_info)
    _atomic_write_csv(info, dataset / "partition_info.csv")
    _atomic_write_csv(info, out / "partition_info.csv")
    _atomic_write_parquet(info, dataset / "partition_info.parquet")

    props = _read_hats_properties(storage, rel)
    props.update(
        {
            "obs_collection": props.get("obs_collection", source.name),
            "dataproduct_type": props.get("dataproduct_type", "object"),
            "hats_nrows": str(total),
            "hats_cols_default": " ".join(selected),
        }
    )
    if source.ra_column and source.ra_column in selected:
        props["hats_col_ra"] = source.ra_column
    if source.dec_column and source.dec_column in selected:
        props["hats_col_dec"] = source.dec_column
    _atomic_write_text("".join(f"{k}={v}\n" for k, v in sorted(props.items())), out / "properties")
    _atomic_write_text(
        "".join(f"{k}={v}\n" for k, v in sorted(props.items())), dataset / "properties"
    )

    if info.height:
        try:
            import pyarrow.parquet as pq  # noqa: PLC0415

            first_rel = str(info["file_loc"][0])
            arrow_schema = pq.read_schema(dataset / f"{first_rel}.parquet")
            pq.write_metadata(arrow_schema, dataset / "_common_metadata")
            pq.write_metadata(arrow_schema, dataset / "_metadata")
        except Exception:  # noqa: BLE001
            logger.warning("skipping HATS metadata for %s", out)

    logger.info("project-hats: wrote %d rows x %d columns to %s", total, len(selected), out)
    return {"rows": total, "columns": selected, "partitions": info.height}


__all__ = ["project_hats"]
