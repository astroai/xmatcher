#!/usr/bin/env python3
"""Check a synced HATS tree against its partition_info.csv and hats.properties."""

from __future__ import annotations

import csv
import json
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pyarrow.parquet as pq


def read_props(root: Path) -> dict[str, str]:
    for name in ("hats.properties", "properties"):
        p = root / name
        if p.exists():
            out = {}
            for line in p.read_text().splitlines():
                if "=" in line and not line.startswith("#"):
                    k, v = line.split("=", 1)
                    out[k.strip()] = v.strip()
            return out
    raise FileNotFoundError(f"no properties under {root}")


def main(root_arg: str) -> int:
    root = Path(root_arg)
    props = read_props(root)
    with open(root / "partition_info.csv") as fh:
        parts = [(int(r["Norder"]), int(r["Npix"])) for r in csv.DictReader(fh)]

    def check(part: tuple[int, int]) -> tuple[tuple[int, int], int | None]:
        order, pix = part
        stem = root / "dataset" / f"Norder={order}" / f"Dir={pix // 10000 * 10000}" / f"Npix={pix}"
        path = stem.with_suffix(".parquet")
        if path.is_file() and path.stat().st_size > 0:
            return part, pq.ParquetFile(path).metadata.num_rows
        if stem.is_dir():
            files = sorted(stem.glob("*.parquet"))
            if files:
                return part, sum(pq.ParquetFile(f).metadata.num_rows for f in files)
        return part, None

    with ThreadPoolExecutor(32) as ex:
        results = list(ex.map(check, parts))
    missing = [p for p, n in results if n is None]
    nrows = sum(n for _, n in results if n is not None)
    expected = int(props.get("hats_nrows", "-1"))
    report = {
        "root": str(root),
        "partitions": len(parts),
        "missing": len(missing),
        "missing_sample": missing[:20],
        "rows": nrows,
        "hats_nrows": expected,
        "ok": not missing and nrows == expected,
    }
    print(json.dumps(report, indent=2))
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1]))
