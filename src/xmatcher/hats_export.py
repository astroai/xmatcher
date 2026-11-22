"""Export a HATS catalogue tree to a single flat file.

HATS stores one Parquet partition per HEALPix pixel. Matching and projection
keep that layout; this module flattens it to ``.parquet`` or ``.fits`` for
tools that only speak ordinary tables (STILTS, notebooks, small-catalog
pipelines).

Parquet export prefers a Polars ``scan_parquet`` over the Npix leaves and
``write_frame``'s streaming sink so medium trees need not be fully
materialized first. FITS is written by :func:`xmatcher.convert.convert` in one
bounded-memory pass (a standard binary table, ``TNULL`` for integer nulls).
The ``_healpix_29`` index column is dropped unless requested, as in
``convert``. A single flat file of a survey-scale tree is still large — keep it
partitioned (HATS) when a single file is not required.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import polars as pl

from .exceptions import CrossMatchError, InputError
from .hats_native import _hats_root, list_hats_pixels
from .io_utils import write_frame
from .sources import CatalogueSource

HEALPIX29 = "_healpix_29"

logger = logging.getLogger(__name__)

_EXPORT_SUFFIXES = {".parquet", ".fits", ".fit"}


def hats_partition_parquet_paths(root: Path) -> list[Path]:
    """Return every leaf Parquet file under a HATS catalogue root."""
    files: list[Path] = []
    for _, _, path in list_hats_pixels(root):
        if path.is_file() and path.suffix == ".parquet":
            files.append(path)
            continue
        if path.is_dir():
            files.extend(sorted(path.glob("*.parquet")))
    return files


def scan_hats(
    src: CatalogueSource | str | Path, *, columns: Sequence[str] | None = None
) -> pl.LazyFrame:
    """Lazily scan all Npix Parquet partitions in a local HATS tree.

    Parameters
    ----------
    src:
        A HATS ``CatalogueSource``, or a path to a local HATS directory.
    columns:
        Optional column projection pushed into the Parquet scan.

    Returns
    -------
    pl.LazyFrame
        A lazy scan over every partition. Collecting or sinking may still
        require memory proportional to the full catalogue for some formats.
    """
    source = _as_hats_source(src)
    root = _hats_root(source)
    files = hats_partition_parquet_paths(root)
    if not files:
        return _empty_hats_lazy(root, columns=columns)

    lf = pl.scan_parquet(files)
    if columns is not None:
        selected = _validate_columns(files[0], columns, source=source.name)
        lf = lf.select(selected)
    return lf


def export_hats(
    src: CatalogueSource | str | Path,
    output: str | Path,
    *,
    columns: Sequence[str] | None = None,
    keep_healpix29: bool = False,
) -> dict[str, Any]:
    """Export a local HATS catalogue to a single ``.parquet`` or ``.fits`` file.

    Parquet partitions are scanned lazily and written with
    :func:`xmatcher.io_utils.write_frame`'s streaming sink. FITS is streamed by
    :func:`xmatcher.convert.convert`. Without ``columns``, the ``_healpix_29``
    index column is dropped unless ``keep_healpix29`` is true. For
    survey-scale trees, keep the HATS layout instead of flattening when
    possible.

    Returns a dict with ``rows``, ``columns``, ``partitions``, and ``output``.
    """
    out = Path(output)
    suffix = out.suffix.lower()
    if suffix not in _EXPORT_SUFFIXES:
        raise InputError(f"export-hats output must end in .parquet or .fits (got '{out.name}')")

    source = _as_hats_source(src)
    root = _hats_root(source)
    files = hats_partition_parquet_paths(root)
    if not files:
        lf = _empty_hats_lazy(root, columns=columns)
        if columns is None and not keep_healpix29:
            lf = lf.select(pl.exclude(HEALPIX29))
        write_frame(lf, out)
        names = lf.collect_schema().names()
        logger.info("export-hats: wrote empty catalogue (%d columns) to %s", len(names), out)
        return {
            "rows": 0,
            "columns": names,
            "partitions": 0,
            "output": str(out),
        }

    if columns is not None:
        selected = _validate_columns(files[0], columns, source=source.name)
    else:
        schema_names = pl.scan_parquet(files[0]).collect_schema().names()
        selected = [n for n in schema_names if keep_healpix29 or n != HEALPIX29]

    if suffix == ".parquet":
        write_frame(pl.scan_parquet(files).select(selected), out)
        rows = int(pl.scan_parquet(out).select(pl.len()).collect().item())
    else:
        from .convert import convert  # noqa: PLC0415 - FITS path only

        report = convert(
            root, out, columns=selected, keep_healpix29=HEALPIX29 in selected, overwrite=True
        )
        rows = report.rows
    names = selected
    logger.info(
        "export-hats: wrote %d rows x %d columns from %d partition(s) to %s",
        rows,
        len(names),
        len(files),
        out,
    )
    return {
        "rows": rows,
        "columns": list(names),
        "partitions": len(files),
        "output": str(out),
    }


def _as_hats_source(src: CatalogueSource | str | Path) -> CatalogueSource:
    if isinstance(src, CatalogueSource):
        return src
    path = Path(src).expanduser()
    return CatalogueSource(
        name=path.name or "hats",
        is_local=True,
        path=path,
        access_method="hats",
        access_identifier=str(path),
    )


def _validate_columns(sample: Path, columns: Sequence[str], *, source: str) -> list[str]:
    schema = set(pl.scan_parquet(sample).collect_schema().names())
    ordered: list[str] = []
    seen: set[str] = set()
    for column in columns:
        name = column.strip()
        if not name or name in seen:
            continue
        if name not in schema:
            raise CrossMatchError(
                f"catalogue '{source}': column '{name}' is not in the HATS schema"
            )
        seen.add(name)
        ordered.append(name)
    if not ordered:
        raise CrossMatchError("export-hats needs at least one column")
    return ordered


def _empty_hats_lazy(root: Path, *, columns: Sequence[str] | None) -> pl.LazyFrame:
    """Schema-only LazyFrame when a tree has no Npix data partitions."""
    for base in (root / "dataset", root):
        metadata = base / "_common_metadata"
        if metadata.is_file():
            schema = pl.read_parquet_schema(metadata)
            names = list(schema.keys())
            if columns is not None:
                selected = []
                seen: set[str] = set()
                for column in columns:
                    name = column.strip()
                    if not name or name in seen:
                        continue
                    if name not in schema:
                        raise CrossMatchError(f"column '{name}' is not in the HATS schema")
                    seen.add(name)
                    selected.append(name)
                if not selected:
                    raise CrossMatchError("export-hats needs at least one column")
                names = selected
            return pl.DataFrame(schema={n: schema[n] for n in names}).lazy()
    if columns:
        raise CrossMatchError(
            "export-hats cannot project columns from an empty HATS tree with no schema metadata"
        )
    return pl.DataFrame().lazy()


__all__ = ["export_hats", "hats_partition_parquet_paths", "scan_hats"]
