#!/usr/bin/env python3
"""Convert UKIDSS WSA CSV.gz RA strips to parquet shards.

WSA CSV.gz files put the column header only on `# sourceID,...` comment lines;
data rows follow without a non-comment header. Empty strips (comments only) → skip.
"""

from __future__ import annotations

import argparse
import gzip
import pathlib
import sys

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq


def convert_one(src: pathlib.Path, dest: pathlib.Path) -> int:
    header: str | None = None
    data_lines: list[str] = []
    with gzip.open(src, "rt", errors="replace") as fh:
        for line in fh:
            s = line.strip()
            if not s:
                continue
            if s.startswith("#"):
                body = s.lstrip("#").strip()
                if body.lower().startswith("sourceid"):
                    header = body
                continue
            data_lines.append(s)
    if not header or not data_lines:
        return 0
    buf = (header + "\n" + "\n".join(data_lines) + "\n").encode("utf-8")
    table = pacsv.read_csv(
        pa.py_buffer(buf),
        parse_options=pacsv.ParseOptions(newlines_in_values=False),
        convert_options=pacsv.ConvertOptions(
            strings_can_be_null=True,
            null_values=["", "NULL", "null"],
        ),
    )
    if table.num_rows == 0:
        return 0
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
    for src in files:
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
        print(f"wrote {dest.name} rows={n} cumulative={total}", flush=True)
    print(f"done parquet_files={n_out} rows={total} out={out}", flush=True)
    if total <= 0:
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
