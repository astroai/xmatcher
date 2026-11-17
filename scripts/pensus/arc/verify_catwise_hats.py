"""Exact row-conservation check between CatWISE2020 stage-1 Parquet tiles and the HATS build.

`source_id` is `<rrrr><p|m><ddd>_b0-<nnnnnn>` (tile name plus a 6-digit index), so it packs losslessly
into an int64 key. Input and output must agree on row count, wrapped key sum, key xor, and the integer
sums of RA and Dec at their published 1e-7 deg precision; output keys must all be distinct.
Usage: verify_catwise_hats.py <hats_root> <expected_rows> <input_glob>
"""

from __future__ import annotations

import glob
import json
import sys
from pathlib import Path

import numpy as np
import polars as pl
import pyarrow.parquet as pq

COLS = ["source_id", "ra", "dec"]
ID_RE = r"^(\d{4})([pm])(\d{3})_b0-(\d{6})$"
U64 = (1 << 64) - 1


def keys(source_id: pl.Series) -> np.ndarray:
    parts = source_id.str.extract_groups(ID_RE)
    if parts.struct.field("1").is_null().any():
        bad = source_id.filter(parts.struct.field("1").is_null()).head(3).to_list()
        raise ValueError(f"source_id outside {ID_RE}: {bad}")
    ra4 = parts.struct.field("1").cast(pl.Int64)
    neg = (parts.struct.field("2") == "m").cast(pl.Int64)
    dec3 = parts.struct.field("3").cast(pl.Int64)
    idx = parts.struct.field("4").cast(pl.Int64)
    return (((ra4 * 2 + neg) * 1000 + dec3) * 1_000_000 + idx).to_numpy()


class Tally:
    def __init__(self, keep_keys: int = 0) -> None:
        self.rows = self.ksum = self.kxor = self.ra = self.dec = 0
        self.keys = np.empty(keep_keys, dtype=np.int64) if keep_keys else None

    def add(self, batch) -> None:
        k = keys(pl.from_arrow(batch.column("source_id")))
        ra = batch.column("ra").to_numpy(zero_copy_only=False).astype(np.float64)
        dec = batch.column("dec").to_numpy(zero_copy_only=False).astype(np.float64)
        if not (np.isfinite(ra).all() and np.isfinite(dec).all()):
            raise ValueError("non-finite coordinates")
        if self.keys is not None:
            if self.rows + len(k) > len(self.keys):
                raise ValueError(f"more rows than expected ({len(self.keys)})")
            self.keys[self.rows : self.rows + len(k)] = k
        self.rows += len(k)
        self.ksum = (self.ksum + int(k.view(np.uint64).sum(dtype=np.uint64))) & U64
        self.kxor ^= int(np.bitwise_xor.reduce(k.view(np.uint64))) if len(k) else 0
        self.ra += int(np.rint(ra * 1e7).astype(np.int64).sum())
        self.dec += int(np.rint((dec + 90.0) * 1e7).astype(np.int64).sum())

    def summary(self) -> dict:
        return {
            "rows": self.rows,
            "key_sum": self.ksum,
            "key_xor": self.kxor,
            "ra_e7": self.ra,
            "dec_e7": self.dec,
        }


def scan(files: list[Path], tally: Tally) -> None:
    for i, f in enumerate(files):
        for batch in pq.ParquetFile(f).iter_batches(batch_size=1_048_576, columns=COLS):
            tally.add(batch)
        if i % 1000 == 0:
            print(f"  {i}/{len(files)} rows={tally.rows}", file=sys.stderr, flush=True)


def main(root: str, expected_rows: int, input_glob: str) -> int:
    inputs = sorted(Path(p) for p in glob.glob(input_glob))
    outputs = sorted(Path(root, "dataset").rglob("*.parquet"))
    if not inputs or not outputs:
        print(f"FAIL: inputs={len(inputs)} outputs={len(outputs)}")
        return 1
    print(f"inputs {len(inputs)} output partitions {len(outputs)}", file=sys.stderr, flush=True)
    t_in, t_out = Tally(), Tally(keep_keys=expected_rows)
    scan(inputs, t_in)
    scan(outputs, t_out)
    k = t_out.keys[: t_out.rows]
    k.sort()
    distinct = int(len(k) - np.count_nonzero(k[1:] == k[:-1])) if len(k) else 0
    a, b = t_in.summary(), t_out.summary()
    checks = {name: a[name] == b[name] for name in a}
    checks["rows_expected"] = b["rows"] == expected_rows
    checks["output_ids_distinct"] = distinct == b["rows"]
    ok = all(checks.values())
    print(
        json.dumps(
            {"ok": ok, "checks": checks, "input": a, "output": b, "distinct": distinct}, indent=2
        )
    )
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1], int(sys.argv[2]), sys.argv[3]))
