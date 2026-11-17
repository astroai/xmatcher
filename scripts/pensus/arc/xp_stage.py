"""Stage 1 of the Gaia DR3 XP continuous import: ESA CDN CSV.gz -> MD5-checked Parquet in k-pop staging.

Resumable: a file is done when stage1/_done/<name>.json exists. Each worker downloads one file to
/scratch, verifies it against _MD5SUM.txt, converts it with xp_convert, then deletes the download.
Usage: xp_stage.py <shard> <nshards> <workers>
"""

from __future__ import annotations

import concurrent.futures as cf
import hashlib
import json
import os
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import xp_convert  # noqa: E402

URL = "https://cdn.gea.esac.esa.int/Gaia/gdr3/Spectroscopy/xp_continuous_mean_spectrum/"
STAGE = Path("/arc/projects/k-pop/pensus/staging/gaia_dr3_xp_continuous/stage1")
DONE = STAGE / "_done"
SCRATCH = Path("/scratch/xp")
CAP_BYTES = 2750 * 2**30


def staged_bytes() -> int:
    return sum(json.loads(p.read_text())["out_bytes"] for p in DONE.glob("*.json"))


def md5(path: Path) -> str:
    h = hashlib.md5()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 24), b""):
            h.update(chunk)
    return h.hexdigest()


def one(name: str, want_md5: str) -> dict:
    marker = DONE / f"{name}.json"
    if marker.exists():
        return json.loads(marker.read_text())
    gz = SCRATCH / name
    t0 = time.time()
    for attempt in range(6):
        rc = subprocess.run(
            [
                "curl",
                "-sfL",
                "--retry",
                "5",
                "--retry-all-errors",
                "-C",
                "-",
                "-o",
                str(gz),
                URL + name,
            ]
        ).returncode
        if rc == 0 and md5(gz) == want_md5:
            break
        gz.unlink(missing_ok=True)
        time.sleep(10 * (attempt + 1))
    else:
        raise RuntimeError(f"download/md5 failed for {name}")
    t1 = time.time()
    table = xp_convert.convert(gz)
    out = STAGE / name.replace(".csv.gz", ".parquet")
    tmp = out.with_suffix(".parquet.partial")
    xp_convert.write(table, tmp)
    tmp.rename(out)
    rec = {
        "file": name,
        "md5": want_md5,
        "in_bytes": gz.stat().st_size,
        "rows": table.num_rows,
        "out_bytes": out.stat().st_size,
        "download_s": round(t1 - t0, 1),
        "convert_s": round(time.time() - t1, 1),
    }
    gz.unlink()
    marker.write_text(json.dumps(rec) + "\n")
    return rec


def main(shard: int, nshards: int, workers: int) -> int:
    DONE.mkdir(parents=True, exist_ok=True)
    SCRATCH.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(URL + "_MD5SUM.txt", timeout=120) as r:
        sums = [ln.split() for ln in r.read().decode().splitlines() if ln.strip()]
    (STAGE.parent / "_MD5SUM.txt").write_text("".join(f"{m}  {n}\n" for m, n in sums))
    mine = [(n, m) for i, (m, n) in enumerate(sums) if i % nshards == shard]
    todo = [(n, m) for n, m in mine if not (DONE / f"{n}.json").exists()]
    print(f"shard {shard}/{nshards}: {len(mine)} files, {len(todo)} to do", flush=True)
    failed = 0
    with cf.ProcessPoolExecutor(workers) as ex:
        pending: set[cf.Future[dict]] = set()
        it = iter(todo)
        while True:
            while len(pending) < workers:
                if staged_bytes() > CAP_BYTES:
                    print("STOP: staging cap reached", flush=True)
                    break
                nxt = next(it, None)
                if nxt is None:
                    break
                pending.add(ex.submit(one, *nxt))
            if not pending:
                break
            done, pending = cf.wait(pending, return_when=cf.FIRST_COMPLETED)
            for f in done:
                try:
                    rec = f.result()
                    print(json.dumps(rec), flush=True)
                except Exception as exc:  # noqa: BLE001
                    failed += 1
                    print(f"FAIL {exc!r}", flush=True)
    n_done = sum(1 for n, _ in mine if (DONE / f"{n}.json").exists())
    print(
        f"shard {shard}: done {n_done}/{len(mine)} failed {failed} staged_total_gib {staged_bytes() / 2**30:.1f}",
        flush=True,
    )
    return 0 if n_done == len(mine) else 1


if __name__ == "__main__":
    os.environ.setdefault("OMP_NUM_THREADS", "1")
    raise SystemExit(main(int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3])))
