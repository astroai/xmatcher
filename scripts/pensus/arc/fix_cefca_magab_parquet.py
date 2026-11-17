#!/usr/bin/env python3
"""Repair CEFCA MagAB* parquet shards written with #SELECT used as the header.

pyarrow.csv.read_csv treated the CEFCA `#SELECT ... FROM ... WHERE ...` comment
as column names (with leading spaces / FROM glued to MAG_ERR_AUTO). The real
CSV header became the first data row. Remap columns by position and drop the
bogus header row.
"""

from __future__ import annotations

import argparse
import pathlib

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

# Must match stage_jpas_e2dr_cefca.COLS / stage_jplus_dr3_cefca.COLS order.
JPAS_COLS = [
    "TILE_ID",
    "NUMBER",
    "ALPHA_J2000",
    "DELTA_J2000",
    "CLASS_STAR",
    "FLAGS",
    "MASK_FLAGS",
    "MAG_APER_3_0",
    "MAG_ERR_APER_3_0",
    "MAG_APER_COR_3_0",
    "MAG_ERR_APER_COR_3_0",
    "MAG_AUTO",
    "MAG_ERR_AUTO",
]
JPLUS_COLS = [
    "TILE_ID",
    "NUMBER",
    "ALPHA_J2000",
    "DELTA_J2000",
    "CLASS_STAR",
    "FLAGS",
    "MASK_FLAGS",
    "MAG_APER_3_0",
    "MAG_ERR_APER_3_0",
    "MAG_PSFCOR",
    "MAG_ERR_PSFCOR",
    "MAG_AUTO",
    "MAG_ERR_AUTO",
]


def is_broken(names: list[str]) -> bool:
    if "ALPHA_J2000" in names and "DELTA_J2000" in names:
        return False
    joined = " ".join(names)
    return "SELECT" in joined or any(n.strip().startswith("#") for n in names)


def fix_table(table: pa.Table, expected: list[str]) -> pa.Table:
    names = list(table.column_names)
    if not is_broken(names):
        # Still strip accidental whitespace names.
        if any(n != n.strip() for n in names):
            table = table.rename_columns([n.strip() for n in names])
        return table
    if table.num_columns != len(expected):
        raise RuntimeError(
            f"column count {table.num_columns} != expected {len(expected)}; names={names}"
        )
    table = table.rename_columns(expected)
    # Drop CSV header row if present as data.
    tid = table.column("TILE_ID")
    # TILE_ID may be string after broken parse.
    if pa.types.is_string(tid.type) or pa.types.is_large_string(tid.type):
        mask = pc.invert(
            pc.or_(
                pc.equal(pc.utf8_trim_whitespace(tid), "TILE_ID"),
                pc.equal(pc.utf8_trim_whitespace(tid), '"TILE_ID"'),
            )
        )
        table = table.filter(mask)
    # hats-import needs numeric ra/dec.
    for col in ("ALPHA_J2000", "DELTA_J2000"):
        c = table.column(col)
        if pa.types.is_floating(c.type):
            continue
        table = table.set_column(
            table.schema.get_field_index(col),
            col,
            pc.cast(c, pa.float64()),
        )
    return table


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--shards", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--survey", choices=("jpas", "jplus"), required=True)
    args = ap.parse_args()
    expected = JPAS_COLS if args.survey == "jpas" else JPLUS_COLS
    shards = pathlib.Path(args.shards)
    out = pathlib.Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    files = sorted(shards.glob("tile_*.parquet"))
    if not files:
        raise SystemExit(f"no tile_*.parquet under {shards}")
    total = 0
    fixed_n = 0
    for i, path in enumerate(files, 1):
        table = pq.read_table(path)
        broken = is_broken(list(table.column_names))
        table = fix_table(table, expected)
        if "ALPHA_J2000" not in table.column_names or "DELTA_J2000" not in table.column_names:
            raise SystemExit(f"still missing ra/dec after fix: {path} cols={table.column_names}")
        dest = out / path.name
        pq.write_table(table, dest, compression="zstd")
        total += table.num_rows
        fixed_n += int(broken)
        if i % 50 == 0 or i == len(files):
            print(
                f"progress {i}/{len(files)} rows={total} repaired_files={fixed_n}",
                flush=True,
            )
    print(f"done files={len(files)} rows={total} repaired_files={fixed_n} out={out}", flush=True)


if __name__ == "__main__":
    main()
