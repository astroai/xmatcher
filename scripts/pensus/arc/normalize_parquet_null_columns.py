#!/usr/bin/env python3
"""Rewrite Arrow ``null``-typed columns to a concrete type (default float64).

CasJobs/astropy → parquet often emits ``pa.null()`` for all-missing mag columns.
hats-import then fails in ``reduce_pixel_shards`` with::

    ArrowNotImplementedError: Unsupported cast from double to null

Use this before hats-import when shards disagree on null vs double (etc.).
"""

from __future__ import annotations

import argparse
import pathlib
import sys

import pyarrow as pa
import pyarrow.parquet as pq


def preferred_type(name: str) -> pa.DataType:
    # Hubble mag columns and generic all-null floats.
    return pa.float64()


def needs_rewrite(schema: pa.Schema) -> list[str]:
    return [f.name for f in schema if pa.types.is_null(f.type)]


def rewrite_table(table: pa.Table, null_cols: list[str]) -> pa.Table:
    for name in null_cols:
        idx = table.schema.get_field_index(name)
        n = table.num_rows
        table = table.set_column(idx, name, pa.nulls(n, type=preferred_type(name)))
    return table


def normalize_file(path: pathlib.Path, *, dry_run: bool) -> list[str]:
    schema = pq.read_schema(path)
    null_cols = needs_rewrite(schema)
    if not null_cols:
        return []
    if dry_run:
        return null_cols
    table = pq.read_table(path)
    table = rewrite_table(table, null_cols)
    # Atomic replace within the same directory (CephFS-safe enough for staging).
    tmp = path.with_suffix(path.suffix + ".nullfix.tmp")
    pq.write_table(table, tmp, compression="zstd")
    tmp.replace(path)
    return null_cols


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("inputs", nargs="+", help="parquet files or directories")
    ap.add_argument(
        "--exclude-name",
        action="append",
        default=[],
        help="basename to skip (repeatable), e.g. orphan overlap shards",
    )
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    exclude = set(args.exclude_name)

    files: list[pathlib.Path] = []
    for raw in args.inputs:
        p = pathlib.Path(raw)
        if p.is_dir():
            files.extend(sorted(p.glob("*.parquet")))
        else:
            files.append(p)
    files = [p for p in files if p.name not in exclude]
    if not files:
        print("no parquet inputs", file=sys.stderr)
        return 2

    n_touch = 0
    n_cols = 0
    for p in files:
        cols = normalize_file(p, dry_run=args.dry_run)
        if cols:
            n_touch += 1
            n_cols += len(cols)
            print(f"{p.name}: null→float64 {cols}", flush=True)
    print(
        f"{'dry_run ' if args.dry_run else ''}files={len(files)} "
        f"rewritten={n_touch} null_columns_total={n_cols}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
