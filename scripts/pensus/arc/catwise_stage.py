"""Stage 1 of the CatWISE2020 import: NERSC IPAC tiles -> Parquet in k-pop staging.

Resumable: a tile is done when stage1/_done/<tile>.json exists. Each tile's download must match the
manifest byte size and its row count must equal the tile's \\Nsrc header (catwise_convert checks).
A second session may work the same shard with `reverse`: scratch and temp names are per host, and a
tile both sessions convert ends up written twice with identical content.
Usage: catwise_stage.py <shard> <nshards> <workers> [reverse]
"""

from __future__ import annotations

import concurrent.futures as cf
import json
import socket
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import catwise_convert  # noqa: E402

URL = "https://portal.nersc.gov/project/cosmo/data/CatWISE/2020/"
ROOT = Path("/arc/projects/k-pop/pensus/staging/catwise_2020")
STAGE = ROOT / "stage1"
DONE = STAGE / "_done"
HOST = socket.gethostname()
SCRATCH = Path("/scratch") / f"catwise_{HOST}"
CAP_BYTES = 1000 * 2**30


def staged_bytes() -> int:
    return sum(json.loads(p.read_text())["out_bytes"] for p in DONE.glob("*.json"))


def one(rel: str, nbytes: int, nsrc: int) -> dict:
    tile = Path(rel).name.removesuffix(".tbl.gz")
    marker = DONE / f"{tile}.json"
    if marker.exists():
        return json.loads(marker.read_text())
    gz = SCRATCH / Path(rel).name
    t0 = time.time()
    for attempt in range(6):
        rc = subprocess.run(
            ["curl", "-sfL", "--retry", "5", "--retry-all-errors", "-o", str(gz), URL + rel]
        ).returncode
        if rc == 0 and gz.stat().st_size == nbytes:
            break
        gz.unlink(missing_ok=True)
        time.sleep(10 * (attempt + 1))
    else:
        raise RuntimeError(f"download failed for {rel}")
    t1 = time.time()
    table, header_nsrc = catwise_convert.read_ipac(gz)
    if not (table.num_rows == header_nsrc == nsrc):
        raise RuntimeError(f"{rel}: rows {table.num_rows} header {header_nsrc} manifest {nsrc}")
    out = STAGE / Path(rel).parent / f"{tile}.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_suffix(f".{HOST}.partial")
    catwise_convert.write(table, tmp)
    tmp.rename(out)
    rec = {
        "file": rel,
        "in_bytes": nbytes,
        "rows": table.num_rows,
        "out_bytes": out.stat().st_size,
        "download_s": round(t1 - t0, 1),
        "convert_s": round(time.time() - t1, 1),
    }
    gz.unlink()
    marker.write_text(json.dumps(rec) + "\n")
    return rec


def main(shard: int, nshards: int, workers: int, reverse: bool = False) -> int:
    DONE.mkdir(parents=True, exist_ok=True)
    SCRATCH.mkdir(parents=True, exist_ok=True)
    manifest = json.loads((ROOT / "manifest.json").read_text())
    mine = [
        m for i, m in enumerate(sorted(manifest, key=lambda m: m["file"])) if i % nshards == shard
    ]
    todo = [
        m
        for m in mine
        if not (DONE / f"{Path(m['file']).name.removesuffix('.tbl.gz')}.json").exists()
    ]
    if reverse:
        todo.reverse()
    print(f"shard {shard}/{nshards}: {len(mine)} tiles, {len(todo)} to do", flush=True)
    failed = 0
    with cf.ProcessPoolExecutor(workers) as ex:
        pending: set[cf.Future[dict]] = set()
        it = iter(todo)
        staged, submitted = staged_bytes(), 0
        while True:
            while len(pending) < workers:
                if submitted % 200 == 199:
                    staged = staged_bytes()
                if staged > CAP_BYTES:
                    print("STOP: staging cap reached", flush=True)
                    break
                m = next(it, None)
                if m is None:
                    break
                pending.add(ex.submit(one, m["file"], m["bytes"], m["nsrc"]))
                submitted += 1
            if not pending:
                break
            done, pending = cf.wait(pending, return_when=cf.FIRST_COMPLETED)
            for f in done:
                try:
                    rec = f.result()
                    staged += rec["out_bytes"]
                    print(json.dumps(rec), flush=True)
                except Exception as exc:  # noqa: BLE001
                    failed += 1
                    print(f"FAIL {exc!r}", flush=True)
    n_done = sum(
        1 for m in mine if (DONE / f"{Path(m['file']).name.removesuffix('.tbl.gz')}.json").exists()
    )
    print(
        f"shard {shard}: done {n_done}/{len(mine)} failed {failed} staged_total_gib {staged_bytes() / 2**30:.1f}",
        flush=True,
    )
    return 0 if n_done == len(mine) else 1


if __name__ == "__main__":
    raise SystemExit(
        main(int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]), sys.argv[4:] == ["reverse"])
    )
