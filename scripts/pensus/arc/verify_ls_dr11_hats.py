"""Exact row-conservation check between LS DR11 stage-1 Parquet and the HATS build.

Tractor sweeps have no `LS_ID` column; the published unique key is
`RELEASE * 10**12 + BRICKID * 10**6 + OBJID`. North and South share brick numbering
but carry distinct `PHOTSYS` (`N`/`S`), so the integrity key is that LS id shifted
left by one bit with PHOTSYS in the low bit. Input and output must agree on row
count, wrapped key sum, key xor, and integer RA/Dec sums at 1e-7 deg; output keys
must all be distinct.
Usage: verify_ls_dr11_hats.py <hats_root> <expected_rows> <input_glob>
"""

from __future__ import annotations

import glob
import json
import sys
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq

COLS = ["RELEASE", "BRICKID", "OBJID", "PHOTSYS", "RA", "DEC"]
U64 = (1 << 64) - 1


def keys(batch) -> np.ndarray:
    release = batch.column("RELEASE").to_numpy(zero_copy_only=False).astype(np.int64)
    brick = batch.column("BRICKID").to_numpy(zero_copy_only=False).astype(np.int64)
    obj = batch.column("OBJID").to_numpy(zero_copy_only=False).astype(np.int64)
    ps = batch.column("PHOTSYS").to_pylist()
    if any(p not in ("N", "S") for p in ps):
        bad = sorted({p for p in ps if p not in ("N", "S")})[:3]
        raise ValueError(f"PHOTSYS outside {{N,S}}: {bad}")
    if (obj < 0).any() or (obj >= 1_000_000).any():
        raise ValueError("OBJID outside [0, 1e6) — LS_ID packing would collide")
    if (brick < 0).any() or (brick >= 1_000_000).any():
        raise ValueError("BRICKID outside [0, 1e6) — LS_ID packing would collide")
    ls_id = release * 1_000_000_000_000 + brick * 1_000_000 + obj
    bit = np.fromiter((0 if p == "N" else 1 for p in ps), dtype=np.int64, count=len(ps))
    return ls_id * 2 + bit


class Tally:
    def __init__(self, keep_keys: int = 0) -> None:
        self.rows = self.ksum = self.kxor = self.ra = self.dec = 0
        self.keys = np.empty(keep_keys, dtype=np.int64) if keep_keys else None

    def add(self, batch) -> None:
        k = keys(batch)
        ra = batch.column("RA").to_numpy(zero_copy_only=False).astype(np.float64)
        dec = batch.column("DEC").to_numpy(zero_copy_only=False).astype(np.float64)
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
        if i % 200 == 0:
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
