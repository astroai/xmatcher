#!/usr/bin/env python3
"""Convert HSC PDR3 NB NAOJ csv.gz shards to parquet for hats-import.

NAOJ catalog_jobs csv.gz puts the column header on a ``# object_id,...`` comment
line (same class of quirk as WSA); data rows follow without a second header.
Empty / header-only shards (seen for nb1010 on some UD fields) are skipped.
"""

from __future__ import annotations

import argparse
import gzip
import pathlib
import sys

import pyarrow as pa
import pyarrow.compute as pc
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
                # Prefer a real column header on the comment line.
                if body.lower().startswith("object_id") or (
                    header is None and "," in body and "ra" in body.lower()
                ):
                    header = body
                continue
            if header is None and s.lower().startswith("object_id"):
                header = s
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
            null_values=["", "NULL", "null", "nan", "NaN"],
        ),
    )
    if table.num_rows == 0:
        return 0
    names = [c.lower() for c in table.column_names]
    table = table.rename_columns(names)
    if "ra" not in names or "dec" not in names:
        raise SystemExit(f"{src}: missing ra/dec columns {names}")
    ra_col = table.column("ra")
    wrapped = pc.if_else(
        pc.is_null(ra_col),
        ra_col,
        pc.subtract(ra_col, pc.multiply(pc.floor(pc.divide(ra_col, 360.0)), 360.0)),
    )
    table = table.set_column(table.schema.get_field_index("ra"), "ra", wrapped.cast(ra_col.type))
    dest.parent.mkdir(parents=True, exist_ok=True)
    partial = dest.with_suffix(".parquet.partial")
    pq.write_table(table, partial, compression="zstd", compression_level=3)
    partial.replace(dest)
    return int(table.num_rows)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--shards", required=True, help="dir with hsc_nb_<band>_*.csv.gz")
    ap.add_argument("--out", required=True, help="parquet output dir")
    ap.add_argument("--band", required=True, help="e.g. nb0387")
    args = ap.parse_args()
    shards = pathlib.Path(args.shards)
    out = pathlib.Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    files = sorted(shards.glob(f"hsc_nb_{args.band}_*.csv.gz"))
    if not files:
        print(f"FAIL no csv.gz for band={args.band} in {shards}", file=sys.stderr)
        return 2
    total = 0
    kept = 0
    for src in files:
        dest = out / (src.name.replace(".csv.gz", ".parquet"))
        if dest.is_file() and dest.stat().st_size > 0:
            n = pq.read_metadata(dest).num_rows
            print(f"skip existing {dest.name} rows={n}", flush=True)
            total += n
            kept += 1
            continue
        n = convert_one(src, dest)
        print(f"{src.name} -> rows={n}", flush=True)
        if n > 0:
            total += n
            kept += 1
        else:
            print(f"WARN empty shard skipped {src.name}", flush=True)
    print(f"band={args.band} files_in={len(files)} parquet_kept={kept} rows={total}", flush=True)
    if total <= 0:
        print(f"FAIL zero rows for band={args.band}", file=sys.stderr)
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
