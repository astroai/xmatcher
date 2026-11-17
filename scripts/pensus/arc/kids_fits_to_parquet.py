#!/usr/bin/env python3
"""Convert staged KiDS DR5 multi-band catalogue FITS tiles to Parquet shards.

Keeps photometry columns as-is; normalizes a stable id column when present.
Ra/Dec are detected case-insensitively from common KiDS / SExtractor names.
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

RA_CANDIDATES = ("RAJ2000", "ALPHA_J2000", "RA", "Ra", "ra")
DEC_CANDIDATES = ("DECJ2000", "DELTA_J2000", "DEC", "Dec", "dec")
ID_CANDIDATES = ("ID", "KIDS_ID", "SeqNr", "NUMBER", "OBJECT_ID", "id")


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


def table_to_arrow(tab: Table) -> pa.Table:
    # Drop structured / multi-dimensional columns; keep scalar photometry + coords.
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
    names = list(tab.colnames)
    ra = pick(names, RA_CANDIDATES)
    dec = pick(names, DEC_CANDIDATES)
    if not ra or not dec:
        raise RuntimeError(f"no RA/DEC in {src.name}: {names[:40]}")
    # Normalize coordinate column names for hats-import
    if ra != "ra":
        if "ra" in tab.colnames:
            tab.remove_column("ra")
        tab.rename_column(ra, "ra")
    if dec != "dec":
        if "dec" in tab.colnames:
            tab.remove_column("dec")
        tab.rename_column(dec, "dec")
    # Wrap ra into [0, 360)
    tab["ra"] = np.mod(np.asarray(tab["ra"], dtype=np.float64), 360.0)
    id_col = pick(list(tab.colnames), ID_CANDIDATES)
    if id_col and id_col != "id":
        if "id" not in tab.colnames:
            tab.rename_column(id_col, "id")
    arrow = table_to_arrow(tab)
    if "ra" not in arrow.column_names or "dec" not in arrow.column_names:
        raise RuntimeError(f"arrow missing ra/dec after convert {src.name}")
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(".parquet.partial")
    pq.write_table(arrow, tmp, compression="zstd")
    tmp.replace(dest)
    return {
        "src": str(src),
        "dest": str(dest),
        "nrows": arrow.num_rows,
        "ncols": arrow.num_columns,
        "ra_src": ra,
        "dec_src": dec,
        "id_src": id_col,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tiles", required=True, help="directory of KiDS *.fits tiles")
    ap.add_argument("--out", required=True, help="output parquet directory")
    ap.add_argument("--manifest", default="", help="optional JSONL progress path")
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()
    tiles_dir = pathlib.Path(args.tiles)
    out_dir = pathlib.Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    files = sorted(tiles_dir.glob("*.fits")) + sorted(tiles_dir.glob("*.fits.gz"))
    # Prefer catalogue tiles; skip accidental non-cat if mixed
    cat = [p for p in files if "_cat" in p.name.lower() or "cat.fits" in p.name.lower()]
    if cat:
        files = cat
    if args.limit > 0:
        files = files[: args.limit]
    if not files:
        raise SystemExit(f"no FITS tiles under {tiles_dir}")
    man = pathlib.Path(args.manifest) if args.manifest else out_dir / "CONVERT.jsonl"
    done = {json.loads(ln)["src"] for ln in man.read_text().splitlines() if ln.strip()} if man.is_file() else set()
    total = 0
    n = 0
    with man.open("a") as mf:
        for i, src in enumerate(files, 1):
            dest = out_dir / f"{safe_stem(src)}.parquet"
            if str(src) in done and dest.is_file():
                meta = pq.read_metadata(dest)
                total += meta.num_rows
                n += 1
                continue
            info = convert_one(src, dest)
            mf.write(json.dumps(info, sort_keys=True) + "\n")
            mf.flush()
            total += info["nrows"]
            n += 1
            if i == 1 or i % 25 == 0 or i == len(files):
                print(
                    f"progress {i}/{len(files)} parquet_files={n} rows={total} last={src.name}",
                    flush=True,
                )
    summary = {
        "tiles": len(files),
        "parquet_files": n,
        "nrows": total,
        "out": str(out_dir),
    }
    (out_dir / "CONVERT_SUMMARY.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    print(json.dumps(summary, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
