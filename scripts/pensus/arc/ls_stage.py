"""Stage 1 of the Legacy Surveys DR11 import: NERSC sweeps -> Parquet in k-pop staging.

Resumable: a sweep is done when stage1/_done/<photsys>_<sweep>.json exists. Each download must match the
manifest byte size and published sha256, and the Parquet row count must equal the FITS NAXIS2 recorded in
the manifest. The FITS file is deleted once its Parquet is written. New downloads stop while k-pop has
less than FLOOR_BYTES free (CephFS quota xattrs).
Usage: ls_stage.py <shard> <nshards> <workers>
"""

from __future__ import annotations

import concurrent.futures as cf
import hashlib
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent))
import ls_convert  # noqa: E402

URL = "https://portal.nersc.gov/cfs/cosmo/data/legacysurvey/dr11/"
KPOP = Path("/arc/projects/k-pop")
ROOT = KPOP / "pensus/staging/legacysurvey_dr11"
STAGE = ROOT / "stage1"
DONE = STAGE / "_done"
SCRATCH = Path("/scratch/legacysurvey_dr11")
FLOOR_BYTES = 300 * 2**30


def kpop_free() -> int:
    quota = int(os.getxattr(KPOP, "ceph.quota.max_bytes"))
    used = int(os.getxattr(KPOP, "ceph.dir.rbytes"))
    return quota - used


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        while chunk := fh.read(64 * 2**20):
            h.update(chunk)
    return h.hexdigest()


def key(m: dict) -> str:
    return f"{m['photsys']}_{Path(m['file']).stem}"


def one(m: dict) -> dict:
    marker = DONE / f"{key(m)}.json"
    if marker.exists():
        return json.loads(marker.read_text())
    fits = SCRATCH / f"{key(m)}.fits"
    t0 = time.time()
    for attempt in range(6):
        rc = subprocess.run(
            ["curl", "-sfL", "--retry", "5", "--retry-all-errors", "-o", str(fits), URL + m["file"]]
        ).returncode
        if rc == 0 and fits.stat().st_size == m["bytes"]:
            break
        fits.unlink(missing_ok=True)
        time.sleep(10 * (attempt + 1))
    else:
        raise RuntimeError(f"download failed for {m['file']}")
    digest = sha256(fits)
    if digest != m["sha256"]:
        fits.unlink()
        raise RuntimeError(f"{m['file']}: sha256 {digest} != published {m['sha256']}")
    t1 = time.time()
    schema = pq.read_schema(ROOT / "mirror_schema.parquet")
    out = STAGE / m["photsys"] / f"{Path(m['file']).stem}.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_suffix(".parquet.partial")
    rows = ls_convert.convert(fits, tmp, m["photsys"], schema)
    if not (rows == pq.read_metadata(tmp).num_rows == m["nrows"]):
        tmp.unlink()
        raise RuntimeError(f"{m['file']}: rows {rows} manifest {m['nrows']}")
    tmp.rename(out)
    fits.unlink()
    rec = {
        "file": m["file"],
        "in_bytes": m["bytes"],
        "rows": rows,
        "out_bytes": out.stat().st_size,
        "download_s": round(t1 - t0, 1),
        "convert_s": round(time.time() - t1, 1),
    }
    marker.write_text(json.dumps(rec) + "\n")
    return rec


def main(shard: int, nshards: int, workers: int) -> int:
    DONE.mkdir(parents=True, exist_ok=True)
    SCRATCH.mkdir(parents=True, exist_ok=True)
    manifest = sorted(json.loads((ROOT / "manifest.json").read_text()), key=lambda m: m["file"])
    mine = [m for i, m in enumerate(manifest) if i % nshards == shard]
    todo = [m for m in mine if not (DONE / f"{key(m)}.json").exists()]
    print(f"shard {shard}/{nshards}: {len(mine)} sweeps, {len(todo)} to do", flush=True)
    failed = 0
    with cf.ProcessPoolExecutor(workers) as ex:
        pending: set[cf.Future[dict]] = set()
        it = iter(todo)
        while True:
            while len(pending) < workers:
                if kpop_free() < FLOOR_BYTES:
                    print(f"STOP: k-pop free {kpop_free() / 2**30:.0f} GiB below floor", flush=True)
                    break
                m = next(it, None)
                if m is None:
                    break
                pending.add(ex.submit(one, m))
            if not pending:
                break
            done, pending = cf.wait(pending, return_when=cf.FIRST_COMPLETED)
            for f in done:
                try:
                    print(json.dumps(f.result()), flush=True)
                except Exception as exc:  # noqa: BLE001
                    failed += 1
                    print(f"FAIL {exc!r}", flush=True)
    n_done = sum(1 for m in mine if (DONE / f"{key(m)}.json").exists())
    print(f"shard {shard}: done {n_done}/{len(mine)} failed {failed}", flush=True)
    return 0 if n_done == len(mine) else 1


if __name__ == "__main__":
    raise SystemExit(main(int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3])))
