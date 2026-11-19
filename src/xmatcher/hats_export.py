"""Export a HATS catalogue tree to a single flat file.

HATS stores one Parquet partition per HEALPix pixel. Matching and projection
keep that layout; this module flattens it to ``.parquet`` or ``.fits`` for
tools that only speak ordinary tables (STILTS, notebooks, small-catalog
pipelines).

Parquet export prefers a Polars ``scan_parquet`` over the Npix leaves and
``write_frame``'s streaming sink so medium trees need not be fully
materialized first. FITS always collects through Astropy. Huge surveys may
still OOM or thrash — keep them partitioned (HATS) when a single file is not
required.
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
) -> dict[str, Any]:
    """Export a local HATS catalogue to a single ``.parquet`` or ``.fits`` file.

    Prefer Parquet: partitions are scanned lazily and written with
    :func:`xmatcher.io_utils.write_frame`'s streaming sink. FITS collects the
    full table first. For survey-scale trees, keep the HATS layout instead of
    flattening when possible.

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
        write_frame(lf, out)
        names = lf.collect_schema().names()
        logger.info("export-hats: wrote empty catalogue (%d columns) to %s", len(names), out)
        return {
            "rows": 0,
            "columns": names,
            "partitions": 0,
            "output": str(out),
        }

    selected = None
    lf = pl.scan_parquet(files)
    if columns is not None:
        selected = _validate_columns(files[0], columns, source=source.name)
        lf = lf.select(selected)
    write_frame(lf, out)

    # Row count without a second full collect when possible: sum partition
    # metadata. Falls back to scanning the written Parquet / collecting FITS.
    rows = _count_exported_rows(out, files)
    names = selected if selected is not None else pl.scan_parquet(files[0]).collect_schema().names()
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


def _count_exported_rows(out: Path, source_files: Sequence[Path]) -> int:
    suffix = out.suffix.lower()
    if suffix == ".parquet" and out.is_file():
        return int(pl.scan_parquet(out).select(pl.len()).collect().item())
    if suffix in (".fits", ".fit") and out.is_file():
        from .io_utils import scan_frame

        return int(scan_frame(out).select(pl.len()).collect().item())
    total = 0
    for path in source_files:
        total += int(pl.scan_parquet(path).select(pl.len()).collect().item())
    return total


__all__ = ["export_hats", "hats_partition_parquet_paths", "scan_hats"]
