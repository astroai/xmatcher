#!/usr/bin/env python3
"""Convert staged ATLAS ER5 multi-band catalogue FITS tiles to Parquet shards.

Normalizes RA2000/DEC2000 → ra/dec and SOURCEID → id when present.
Keeps unique/primary sources when PRIMARY_SOURCE or PRIORSEC columns exist
(PRIMARY_SOURCE==1 preferred; else PRIORSEC==0).
"""

from __future__ import annotations

import argparse
import json
import pathlib
import re

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from astropy.table import Table

RA_CANDIDATES = ("RA2000", "RAJ2000", "ALPHA_J2000", "RA", "Ra", "ra")
DEC_CANDIDATES = ("DEC2000", "DECJ2000", "DELTA_J2000", "DEC", "Dec", "dec")
ID_CANDIDATES = ("SOURCEID", "sourceID", "ID", "id", "SeqNr", "NUMBER")


def pick(names: list[str], candidates: tuple[str, ...]) -> str | None:
    lower = {n.lower(): n for n in names}
    for c in candidates:
        if c in names:
            return c
        if c.lower() in lower:
            return lower[c.lower()]
    return None


def safe_stem(path: pathlib.Path) -> str:
    stem = path.name
    stem = re.sub(r"\.fits(\.gz)?$", "", stem, flags=re.IGNORECASE)
    stem = re.sub(r"[^A-Za-z0-9._\-]+", "_", stem)
    return stem[:180] or "tile"


def apply_primary_filter(tab: Table) -> Table:
    names = list(tab.colnames)
    lower = {n.lower(): n for n in names}
    if "primary_source" in lower:
        col = lower["primary_source"]
        mask = np.asarray(tab[col]) == 1
        return tab[mask]
    if "priorsec" in lower:
        col = lower["priorsec"]
        mask = np.asarray(tab[col]) == 0
        return tab[mask]
    return tab


def table_to_arrow(tab: Table) -> pa.Table:
    drop = []
    for name in tab.colnames:
        col = tab[name]
        dtype = getattr(col, "dtype", None)
        if col.ndim != 1 or (dtype is not None and getattr(dtype, "names", None)):
            drop.append(name)
    for name in drop:
        tab.remove_column(name)
    df = tab.to_pandas()
    return pa.Table.from_pandas(df, preserve_index=False)


def convert_one(src: pathlib.Path, dest: pathlib.Path) -> dict:
    tab = Table.read(str(src), format="fits")
    n_in = len(tab)
    tab = apply_primary_filter(tab)
    n_keep = len(tab)
    names = list(tab.colnames)
    ra = pick(names, RA_CANDIDATES)
    dec = pick(names, DEC_CANDIDATES)
    if not ra or not dec:
        raise RuntimeError(f"no RA/DEC in {src.name}: {names[:40]}")
    if ra != "ra":
        if "ra" in tab.colnames:
            tab.remove_column("ra")
        tab.rename_column(ra, "ra")
    if dec != "dec":
        if "dec" in tab.colnames:
            tab.remove_column("dec")
        tab.rename_column(dec, "dec")
    tab["ra"] = np.mod(np.asarray(tab["ra"], dtype=np.float64), 360.0)
    id_col = pick(list(tab.colnames), ID_CANDIDATES)
    if id_col and id_col != "id" and "id" not in tab.colnames:
        tab.rename_column(id_col, "id")
    arrow = table_to_arrow(tab)
    if "ra" not in arrow.column_names or "dec" not in arrow.column_names:
        raise RuntimeError(f"arrow missing ra/dec after convert {src.name}")
    dest.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(arrow, dest, compression="zstd")
    return {
        "src": src.name,
        "dest": dest.name,
        "rows_in": n_in,
        "rows_out": n_keep,
        "ncols": arrow.num_columns,
        "bytes": dest.stat().st_size,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tiles", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    tiles = pathlib.Path(args.tiles)
    out = pathlib.Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    fits = sorted({*tiles.glob("*.fits"), *tiles.glob("*.fits.gz"), *tiles.glob("*.FITS")})
    print(f"fits_tiles={len(fits)}", flush=True)
    summary: list[dict] = []
    for i, src in enumerate(fits, 1):
        dest = out / f"{safe_stem(src)}.parquet"
        if dest.is_file() and dest.stat().st_size > 0:
            meta = {
                "src": src.name,
                "dest": dest.name,
                "rows_out": int(pq.read_metadata(dest).num_rows),
                "skipped_existing": True,
            }
            summary.append(meta)
            continue
        meta = convert_one(src, dest)
        summary.append(meta)
        if i % 25 == 0 or i == len(fits):
            print(
                f"convert {i}/{len(fits)} last={src.name} rows_out={meta['rows_out']}",
                flush=True,
            )
    rows = sum(int(m.get("rows_out") or 0) for m in summary)
    (out / "CONVERT_SUMMARY.json").write_text(
        json.dumps({"files": summary, "rows_out": rows}, indent=2, sort_keys=True) + "\n"
    )
    print(f"done parquet_files={len(summary)} rows_out={rows}", flush=True)


if __name__ == "__main__":
    main()
