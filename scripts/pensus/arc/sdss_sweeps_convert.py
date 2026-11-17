#!/usr/bin/env python3
"""Convert SDSS PHOTO_SWEEP calibObj-*-stargal-primary.fits.gz to Parquet.

Expands 5-band vector columns into u/g/r/i/z scalars for hats-import.
Keeps PhotoPrimary-style stargal-primary rows as published (already primary).
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

BANDS = ("u", "g", "r", "i", "z")
# Photometry vectors worth expanding; everything else multi-dim is dropped.
EXPAND = {
    "PSFFLUX",
    "PSFFLUX_IVAR",
    "FIBERFLUX",
    "FIBERFLUX_IVAR",
    "FIBER2FLUX",
    "FIBER2FLUX_IVAR",
    "MODELFLUX",
    "MODELFLUX_IVAR",
    "MODELMAG",
    "PETROFLUX",
    "PETROFLUX_IVAR",
    "DEVFLUX",
    "DEVFLUX_IVAR",
    "EXPFLUX",
    "EXPFLUX_IVAR",
    "EXTINCTION",
    "PSF_FWHM",
    "NMGYPERCOUNT",
    "CALIB_STATUS",
    "FLAGS",
    "FLAGS2",
}


def _stem(path: pathlib.Path) -> str:
    name = re.sub(r"\.fits(\.gz)?$", "", path.name, flags=re.IGNORECASE)
    return re.sub(r"[^A-Za-z0-9._\-]+", "_", name)[:180]


def _native(arr: np.ndarray) -> np.ndarray:
    """FITS often yields big-endian arrays; Arrow needs native endian."""
    data = np.asarray(arr)
    if data.dtype.byteorder not in ("=", "|") and data.dtype.kind in "iuf":
        data = data.astype(data.dtype.newbyteorder("="))
    return np.ascontiguousarray(data)


def _scalarize(tab: Table) -> dict[str, np.ndarray]:
    out: dict[str, np.ndarray] = {}
    for name in tab.colnames:
        col = tab[name]
        data = np.asarray(col)
        key = name.strip()
        if data.ndim == 1 and not getattr(data.dtype, "names", None):
            out[key] = _native(data)
            continue
        if data.ndim == 2 and data.shape[1] == 5 and key in EXPAND:
            for i, b in enumerate(BANDS):
                out[f"{key}_{b}"] = _native(data[:, i])
            continue
        # drop other multi-dim / structured columns
    if "THING_ID" in out and "id" not in out:
        out["id"] = out["THING_ID"].astype(np.int64, copy=False)
    if "RA" in out:
        out["RA"] = np.mod(out["RA"].astype(np.float64, copy=False), 360.0)
    if "DEC" in out:
        out["DEC"] = out["DEC"].astype(np.float64, copy=False)
    return out


def convert_one(src: pathlib.Path, dest: pathlib.Path) -> dict:
    tab = Table.read(str(src), format="fits")
    cols = _scalarize(tab)
    if "RA" not in cols or "DEC" not in cols:
        raise RuntimeError(f"missing RA/DEC in {src.name}: {sorted(cols)[:40]}")
    arrays = []
    names = []
    for k, v in cols.items():
        try:
            arrays.append(pa.array(v))
            names.append(k)
        except (pa.ArrowInvalid, pa.ArrowTypeError, TypeError, ValueError):
            # skip unconvertible leftovers
            continue
    table = pa.Table.from_arrays(arrays, names=names)
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(".parquet.partial")
    pq.write_table(table, tmp, compression="zstd", compression_level=3)
    tmp.replace(dest)
    return {
        "src": str(src),
        "dest": str(dest),
        "nrows": table.num_rows,
        "ncols": table.num_columns,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--manifest", default="")
    ap.add_argument("--delete-raw", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()
    raw = pathlib.Path(args.raw)
    out = pathlib.Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    files = sorted(raw.glob("calibObj-*-stargal-primary.fits.gz"))
    files = [p for p in files if "wise-stargal-primary" not in p.name]
    if args.limit > 0:
        files = files[: args.limit]
    man = pathlib.Path(args.manifest) if args.manifest else None
    if man:
        man.parent.mkdir(parents=True, exist_ok=True)
    done = 0
    rows = 0
    for src in files:
        dest = out / f"{_stem(src)}.parquet"
        if dest.exists() and dest.stat().st_size > 0:
            meta = {"src": str(src), "dest": str(dest), "nrows": -1, "skipped": True}
        else:
            meta = convert_one(src, dest)
            n_rows = meta["nrows"]
            if not isinstance(n_rows, int):
                raise TypeError(f"nrows must be int, got {type(n_rows)!r}")
            rows += n_rows
            if args.delete_raw:
                src.unlink(missing_ok=True)
        done += 1
        if man:
            with man.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(meta) + "\n")
        if done % 50 == 0:
            print(f"converted {done}/{len(files)} rows_so_far={rows}", flush=True)
    print(json.dumps({"files": len(files), "converted": done, "nrows_new": rows}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
