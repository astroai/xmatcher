#!/usr/bin/env python3
"""Deep QA of a promoted HATS release: structure, every partition, pixel membership, cone read, pointer."""

from __future__ import annotations

import csv
import json
import math
import os
import sys
from pathlib import Path

import astropy.units as u
import numpy as np
import pyarrow.parquet as pq
from astropy.coordinates import Latitude, Longitude
from cdshealpix.nested import lonlat_to_healpix

sys.path.insert(0, "/arc/projects/k-pop/pensus/code")
sys.path.insert(0, "/arc/projects/k-pop/pensus/code/pensus")
import canfar_pinned_match as pm  # noqa: E402

SKY_DEG2 = 4 * math.pi * (180 / math.pi) ** 2


def read_props(root: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    for name in ("hats.properties", "properties"):
        p = root / name
        if p.is_file():
            for line in p.read_text().splitlines():
                if "=" in line and not line.lstrip().startswith("#"):
                    k, v = line.split("=", 1)
                    out.setdefault(k.strip(), v.strip())
    return out


def main(root_arg: str, alias: str, expect_rows: int, expect_parts: int) -> int:
    root = Path(root_arg)
    checks: dict[str, bool] = {}
    notes: dict[str, object] = {}
    props = read_props(root)
    checks["properties_present"] = bool(props)
    checks["partition_info_present"] = (root / "partition_info.csv").is_file()
    ra_col, dec_col = props.get("hats_col_ra", "ra"), props.get("hats_col_dec", "dec")
    with open(root / "partition_info.csv") as fh:
        parts = sorted((int(r["Norder"]), int(r["Npix"])) for r in csv.DictReader(fh))
    checks["partition_count"] = len(parts) == expect_parts
    checks["hats_nrows_matches_expected"] = int(props.get("hats_nrows", -1)) == expect_rows

    total = 0
    bad: list[str] = []
    # Cone centred on a real source; the brute-force count accumulates during the full scan.
    o0, p0 = parts[len(parts) // 3]
    stem0 = root / "dataset" / f"Norder={o0}" / f"Dir={p0 // 10000 * 10000}" / f"Npix={p0}"
    f0 = (
        stem0.with_suffix(".parquet")
        if stem0.with_suffix(".parquet").is_file()
        else sorted(stem0.glob("*.parquet"))[0]
    )
    t0 = pq.read_table(f0, columns=[ra_col, dec_col])
    cra, cdec, rad = float(t0.column(ra_col)[0].as_py()), float(t0.column(dec_col)[0].as_py()), 0.5
    d2r = math.pi / 180
    brute = 0
    cells7: set[int] = set()
    hp_checked = 0
    for i, (order, pix) in enumerate(parts):
        if i % 100 == 0:
            print(f"scan {i}/{len(parts)} rows={total}", file=sys.stderr, flush=True)
        stem = root / "dataset" / f"Norder={order}" / f"Dir={pix // 10000 * 10000}" / f"Npix={pix}"
        files = (
            [stem.with_suffix(".parquet")]
            if stem.with_suffix(".parquet").is_file()
            else sorted(stem.glob("*.parquet"))
        )
        if not files:
            bad.append(f"missing {order}/{pix}")
            continue
        n_part = 0
        for f in files:
            try:
                # Every column is decoded, batch by batch, so wide partitions stay in bounded memory.
                for batch in pq.ParquetFile(f).iter_batches(batch_size=262_144):
                    n_part += batch.num_rows
                    ra = batch.column(ra_col).to_numpy(zero_copy_only=False).astype(np.float64)
                    dec = batch.column(dec_col).to_numpy(zero_copy_only=False).astype(np.float64)
                    if not (np.isfinite(ra).all() and np.isfinite(dec).all()):
                        bad.append(f"non-finite coords {order}/{pix}")
                    if ((ra < 0) | (ra >= 360) | (dec < -90) | (dec > 90)).any():
                        bad.append(f"coords out of range {order}/{pix}")
                    if "_healpix_29" in batch.schema.names:
                        hp = (
                            batch.column("_healpix_29")
                            .to_numpy(zero_copy_only=False)
                            .astype(np.int64)
                        )
                    else:
                        hp = lonlat_to_healpix(
                            Longitude(ra * u.deg), Latitude(dec * u.deg), 29
                        ).astype(np.int64)
                    if not (hp >> (2 * (29 - order)) == pix).all():
                        bad.append(f"rows outside pixel {order}/{pix}")
                    cells7.update(np.unique(hp >> (2 * (29 - 7))).tolist())
                    hp_checked += len(hp)
                    cosd = np.sin(cdec * d2r) * np.sin(dec * d2r) + np.cos(cdec * d2r) * np.cos(
                        dec * d2r
                    ) * np.cos((ra - cra) * d2r)
                    brute += int((np.degrees(np.arccos(np.clip(cosd, -1, 1))) <= rad).sum())
            except Exception as exc:
                bad.append(f"unreadable {f}: {exc}")
        if n_part == 0:
            bad.append(f"empty partition {order}/{pix}")
        total += n_part
    checks["all_partitions_readable_nonempty_in_range"] = not bad
    checks["rows_sum_equals_expected"] = total == expect_rows
    checks["healpix_membership_checked_all_rows"] = hp_checked == total
    notes["bad_sample"] = bad[:10]
    notes["rows"] = total
    notes["footprint_deg2_order7"] = round(len(cells7) * SKY_DEG2 / (12 * 4**7), 1)

    print(f"scan done rows={total}; cone extract", file=sys.stderr, flush=True)
    out = Path(os.environ.get("QA_SCRATCH", "/scratch")) / "qa_cone.parquet"
    ext = pm.extract_cone(root, out, cra, cdec, rad)
    cone_rows = pq.read_metadata(out).num_rows
    checks["cone_extract_equals_bruteforce"] = cone_rows == brute and brute > 0
    notes["cone"] = {
        "ra": round(cra, 5),
        "dec": round(cdec, 5),
        "radius_deg": rad,
        "extract_rows": cone_rows,
        "bruteforce_rows": brute,
        "pixels_used": len(ext["pixels_used"]),
    }

    resolved = pm._resolve_local_tree(alias)
    checks["pointer_resolves_to_hats"] = resolved.resolve() == root.resolve()
    notes["pointer_resolves_to"] = str(resolved)

    ok = all(checks.values())
    print(json.dumps({"root": str(root), "ok": ok, "checks": checks, **notes}, indent=2))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])))
