#!/usr/bin/env python3
"""Convert UKIDSS WSA CSV.gz RA strips to parquet shards (skip # comments / empty strips)."""

from __future__ import annotations

import argparse
import gzip
import pathlib
import sys

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq


def convert_one(src: pathlib.Path, dest: pathlib.Path) -> int:
    # Skip comment lines by finding the last header-like line before data.
    with gzip.open(src, "rb") as fh:
        raw = fh.read()
    text = raw.decode("utf-8", "replace")
    lines = text.splitlines(keepends=True)
    header_idx = None
    for i, line in enumerate(lines):
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        if "sourceID" in s or "ra" in s.split(",")[0:3]:
            header_idx = i
            # keep scanning — last non-comment header wins if duplicated
            continue
        if header_idx is not None:
            break
    if header_idx is None:
        return 0
    cleaned = "".join(lines[header_idx:]).encode("utf-8")
    if not cleaned.strip():
        return 0
    table = pacsv.read_csv(
        pa.py_buffer(cleaned),
        convert_options=pacsv.ConvertOptions(strings_can_be_null=True, null_values=["", "NULL", "null"]),
    )
    if table.num_rows == 0:
        return 0
    # Normalize column names
    names = [c.lower() for c in table.column_names]
    table = table.rename_columns(names)
    if "ra" not in names or "dec" not in names:
        raise SystemExit(f"{src}: missing ra/dec columns {names}")
    dest.parent.mkdir(parents=True, exist_ok=True)
    partial = dest.with_suffix(".parquet.partial")
    pq.write_table(table, partial, compression="zstd", compression_level=3)
    partial.replace(dest)
    return table.num_rows


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--strips", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    strips = pathlib.Path(args.strips)
    out = pathlib.Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    files = sorted(strips.glob("*.csv.gz"))
    total = 0
    n_out = 0
    for i, src in enumerate(files):
        dest = out / (src.name.replace(".csv.gz", ".parquet"))
        if dest.exists():
            n = pq.read_metadata(dest).num_rows
            total += n
            n_out += 1
            print(f"skip {dest.name} rows={n}", flush=True)
            continue
        n = convert_one(src, dest)
        if n == 0:
            print(f"empty {src.name}", flush=True)
            continue
        total += n
        n_out += 1
        if (i + 1) % 10 == 0 or n > 0:
            print(f"wrote {dest.name} rows={n} cumulative={total}", flush=True)
    print(f"done parquet_files={n_out} rows={total} out={out}", flush=True)
    if total <= 0:
        return 2
    return 0


if __name__ == "__main__":
    # avoid unused import lint when resume path unused in some analyzers
    sys.exit(main())
