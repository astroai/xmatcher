#!/usr/bin/env python3
"""CANFAR entry: pin LIneA GALEX MCAT under alias ``galex_linea``.

Raw ``xmatcher sync <url>`` stores under a URL-hash cache name (``galex/``).
This launcher keeps the SOURCE pointer at ``inputs/galex_linea/SOURCE.json``
and re-runs sync on failure so IncompleteRead / truncated GETs can resume
from the incremental manifest.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

WORK = Path("/arc/projects/k-pop/pensus")
CODE = WORK / "code"
CACHE = WORK / "inputs" / "_cache"
URL = "https://linea.data.lsdb.io/hats/galex/galex"
ALIAS = "galex_linea"
MAX_ATTEMPTS = int(os.environ.get("PENSUS_SYNC_RETRY_ATTEMPTS", "4"))


def _bootstrap_path() -> None:
    deps = Path(os.environ.get("PENSUS_PYDEPS", "/scratch/pensus-pydeps"))
    deps.mkdir(parents=True, exist_ok=True)
    vendor = CODE / "vendor" / "xmatcher" / "src"
    for path in (deps, vendor, CODE):
        if path.exists() and str(path) not in sys.path:
            sys.path.insert(0, str(path))
    os.environ["PYTHONPATH"] = os.pathsep.join(str(p) for p in (deps, vendor, CODE) if p.exists())
    os.environ["PENSUS_PYDEPS"] = str(deps)
    os.environ["XMATCHER_CACHE_ROOT"] = str(CACHE)


def main() -> int:
    _bootstrap_path()
    import pensus.canfar_sync_job as job

    CACHE.mkdir(parents=True, exist_ok=True)
    run_id = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    os.environ["PENSUS_RUN_ID"] = run_id
    boot = job.bootstrap_runtime()
    results: dict = {
        "run_id": run_id,
        "bootstrap": boot,
        "aliases": [ALIAS],
        "cache_root": str(CACHE),
        "url": URL,
        "steps": [],
    }
    if not boot.get("ok"):
        results["ok"] = False
        job._write_json(WORK / "logs" / run_id / "sync-job-report.json", results)
        print(json.dumps(results, indent=2, sort_keys=True))
        return 2

    log_dir = WORK / "logs" / run_id
    log_dir.mkdir(parents=True, exist_ok=True)
    cmd = [
        sys.executable,
        "-m",
        "xmatcher",
        "sync",
        URL,
        "--cache-root",
        str(CACHE),
        "--threads",
        os.environ.get("PENSUS_SYNC_THREADS", "16"),
        "--min-free-gb",
        os.environ.get("PENSUS_SYNC_MIN_FREE_GB", "50"),
        "--rate-limit",
        "0",
        "-v",
    ]
    ok = False
    rc = 1
    elapsed = 0.0
    for attempt in range(1, MAX_ATTEMPTS + 1):
        stdout_path = log_dir / f"sync-{ALIAS}.attempt{attempt}.stdout.log"
        stderr_path = log_dir / f"sync-{ALIAS}.attempt{attempt}.stderr.log"
        print(f"+ attempt {attempt}/{MAX_ATTEMPTS}: {' '.join(cmd)}", flush=True)
        t0 = time.perf_counter()
        with stdout_path.open("w") as out, stderr_path.open("w") as err:
            proc = subprocess.run(cmd, stdout=out, stderr=err, text=True, check=False)
        elapsed = round(time.perf_counter() - t0, 3)
        rc = proc.returncode
        ok = rc == 0
        results["steps"].append(
            {
                "alias": ALIAS,
                "attempt": attempt,
                "url": URL,
                "ok": ok,
                "returncode": rc,
                "elapsed_s": elapsed,
                "stdout_log": str(stdout_path),
                "stderr_log": str(stderr_path),
            }
        )
        if ok:
            break
        # Incremental sync: pause briefly then resume from sync.json.
        time.sleep(min(30 * attempt, 120))

    job._write_source_pointer(
        ALIAS,
        CACHE,
        ok,
        {
            "cmd": cmd,
            "returncode": rc,
            "elapsed_s": elapsed,
            "url": URL,
            "attempts": len(results["steps"]),
            "cache_alias_hint": "galex",
        },
    )
    results["ok"] = ok
    results["finished_at_utc"] = datetime.now(UTC).isoformat()
    job._write_json(WORK / "logs" / run_id / "sync-job-report.json", results)
    print(json.dumps(results, indent=2, sort_keys=True))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
