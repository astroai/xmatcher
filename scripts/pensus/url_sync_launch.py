#!/usr/bin/env python3
"""CANFAR entry: pin an arbitrary remote HATS URL under a pensus alias.

Env:
  PENSUS_SYNC_URL          remote HATS root (http(s) / s3:// / ...)
  PENSUS_SYNC_ALIAS        SOURCE.json alias under inputs/<alias>/
  PENSUS_SYNC_CACHE_HINT   optional _cache folder name for URL syncs
  PENSUS_SYNC_EST_GIB      estimated pin size (GiB); required with a budget gate
  PENSUS_SYNC_FREE_GIB     project free space (GiB) from a VOS/quota probe
  PENSUS_SYNC_RESERVE_GIB  free-space floor (default 2048)
  PENSUS_SYNC_MAX_GIB      hard cap on est_gib (optional)

Do **not** trust ``shutil.disk_usage("/arc")`` for project budget: on Skaha that
reports the shared Ceph pool (~10^5 GiB free), not the k-pop/hats quota.
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


def _budget_ok(est_gib: float | None) -> tuple[bool, dict]:
    """Refuse starts that would eat the reserved free-space floor.

    Project free space must be supplied explicitly via ``PENSUS_SYNC_FREE_GIB``
    (from a VOS child-sum / quota probe). Shared-filesystem ``df`` on ``/arc`` is
    intentionally ignored.
    """
    reserve = float(os.environ.get("PENSUS_SYNC_RESERVE_GIB", "2048"))
    free_raw = os.environ.get("PENSUS_SYNC_FREE_GIB", "").strip()
    free_gib = float(free_raw) if free_raw else None
    max_raw = os.environ.get("PENSUS_SYNC_MAX_GIB", "").strip()
    max_gib = float(max_raw) if max_raw else None
    info: dict[str, object] = {
        "est_gib": est_gib,
        "reserve_gib": reserve,
        "free_gib": None if free_gib is None else round(free_gib, 1),
        "max_gib": max_gib,
        "free_source": "PENSUS_SYNC_FREE_GIB" if free_gib is not None else None,
    }
    if est_gib is None:
        return True, info
    if free_gib is None and max_gib is None:
        info["reason"] = "set PENSUS_SYNC_FREE_GIB and/or PENSUS_SYNC_MAX_GIB (do not use /arc df)"
        return False, info
    if free_gib is not None and est_gib > max(0.0, free_gib - reserve):
        info["reason"] = "est_gib exceeds free_gib - reserve"
        return False, info
    if max_gib is not None and est_gib > max_gib:
        info["reason"] = "est_gib exceeds PENSUS_SYNC_MAX_GIB"
        return False, info
    return True, info


def main() -> int:
    _bootstrap_path()
    import pensus.canfar_sync_job as job

    url = os.environ.get("PENSUS_SYNC_URL", "").strip()
    alias = os.environ.get("PENSUS_SYNC_ALIAS", "").strip()
    hint = os.environ.get("PENSUS_SYNC_CACHE_HINT", "").strip() or None
    est_raw = os.environ.get("PENSUS_SYNC_EST_GIB", "").strip()
    est_gib = float(est_raw) if est_raw else None
    if not url or not alias:
        print("PENSUS_SYNC_URL and PENSUS_SYNC_ALIAS are required", file=sys.stderr)
        return 2

    ok_budget, budget = _budget_ok(est_gib)
    if not ok_budget:
        payload = {
            "ok": False,
            "error": "size_budget_rejected",
            "budget": budget,
            "url": url,
            "alias": alias,
        }
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 3

    CACHE.mkdir(parents=True, exist_ok=True)
    run_id = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    os.environ["PENSUS_RUN_ID"] = run_id
    boot = job.bootstrap_runtime()
    results: dict = {
        "run_id": run_id,
        "bootstrap": boot,
        "aliases": [alias],
        "cache_root": str(CACHE),
        "url": url,
        "budget": budget,
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
        url,
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
        stdout_path = log_dir / f"sync-{alias}.attempt{attempt}.stdout.log"
        stderr_path = log_dir / f"sync-{alias}.attempt{attempt}.stderr.log"
        print(f"+ attempt {attempt}/{MAX_ATTEMPTS}: {' '.join(cmd)}", flush=True)
        t0 = time.perf_counter()
        with stdout_path.open("w") as out, stderr_path.open("w") as err:
            proc = subprocess.run(cmd, stdout=out, stderr=err, text=True, check=False)
        elapsed = round(time.perf_counter() - t0, 3)
        rc = proc.returncode
        ok = rc == 0
        results["steps"].append(
            {
                "alias": alias,
                "attempt": attempt,
                "url": url,
                "ok": ok,
                "returncode": rc,
                "elapsed_s": elapsed,
                "stdout_log": str(stdout_path),
                "stderr_log": str(stderr_path),
            }
        )
        if ok:
            break
        time.sleep(min(30 * attempt, 120))

    extra = {
        "cmd": cmd,
        "returncode": rc,
        "elapsed_s": elapsed,
        "url": url,
        "attempts": len(results["steps"]),
        "budget": budget,
    }
    if hint:
        extra["cache_alias_hint"] = hint
    job._write_source_pointer(alias, CACHE, ok, extra)
    results["ok"] = ok
    results["finished_at_utc"] = datetime.now(UTC).isoformat()
    job._write_json(WORK / "logs" / run_id / "sync-job-report.json", results)
    print(json.dumps(results, indent=2, sort_keys=True))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
