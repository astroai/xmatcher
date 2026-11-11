#!/usr/bin/env python3
"""CANFAR job: pin selected LSDB HATS trees into k-pop/pensus/inputs.

Uses ``xmatcher sync`` when the package is available (pixi or pip); otherwise
records a skip with install instructions. Prefer small/medium trees first.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

WORK = Path(os.environ.get("PENSUS_WORK_ROOT", "/arc/projects/k-pop/pensus"))
# Start with 2MASS (smaller than Gaia); Gaia is a follow-on campaign.
DEFAULT_ALIASES = os.environ.get(
    "PENSUS_SYNC_ALIASES", "twomass_psc_hats,skymapper_dr4_hats"
).split(",")


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def find_xmatcher() -> list[str] | None:
    if shutil.which("pixi") and (Path.cwd() / "pixi.toml").is_file():
        return ["pixi", "run", "xmatcher"]
    if shutil.which("xmatcher"):
        return ["xmatcher"]
    try:
        import xmatcher  # noqa: F401

        return [sys.executable, "-m", "xmatcher"]
    except ImportError:
        return None


def main(argv: list[str] | None = None) -> int:
    aliases = [a.strip() for a in (argv or DEFAULT_ALIASES) if a.strip()]
    run_id = os.environ.get("PENSUS_RUN_ID") or time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    cache = WORK / "inputs" / "_cache"
    cache.mkdir(parents=True, exist_ok=True)
    results: dict = {
        "run_id": run_id,
        "started_at_utc": datetime.now(UTC).isoformat(),
        "aliases": aliases,
        "cache_root": str(cache),
        "steps": [],
    }
    cmd_base = find_xmatcher()
    if cmd_base is None:
        results["ok"] = False
        results["error"] = "xmatcher not installed in session; bootstrap pixi/xmatcher first"
        _write_json(WORK / "logs" / run_id / "sync-job-report.json", results)
        print(json.dumps(results, indent=2, sort_keys=True))
        return 2

    ok = True
    for alias in aliases:
        dest = WORK / "inputs" / alias
        dest.mkdir(parents=True, exist_ok=True)
        cmd = [*cmd_base, "sync", alias, "--cache-root", str(cache), "--output", str(dest)]
        # Some CLIs use different flags; fall back to cache-only sync.
        print("+", " ".join(cmd), flush=True)
        t0 = time.perf_counter()
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
        step = {
            "alias": alias,
            "cmd": cmd,
            "returncode": proc.returncode,
            "elapsed_s": round(time.perf_counter() - t0, 3),
            "stdout_tail": (proc.stdout or "")[-2000:],
            "stderr_tail": (proc.stderr or "")[-2000:],
            "dest": str(dest),
        }
        if proc.returncode != 0:
            ok = False
            # Retry without --output if unsupported
            cmd2 = [*cmd_base, "sync", alias, "--cache-root", str(cache)]
            print("+ retry", " ".join(cmd2), flush=True)
            proc2 = subprocess.run(cmd2, capture_output=True, text=True, check=False)
            step["retry"] = {
                "cmd": cmd2,
                "returncode": proc2.returncode,
                "stdout_tail": (proc2.stdout or "")[-2000:],
                "stderr_tail": (proc2.stderr or "")[-2000:],
            }
            if proc2.returncode == 0:
                ok = True if ok else True
                # keep overall ok if retry worked for this alias
            else:
                ok = False
        results["steps"].append(step)

    results["ok"] = ok
    results["finished_at_utc"] = datetime.now(UTC).isoformat()
    out = WORK / "logs" / run_id / "sync-job-report.json"
    _write_json(out, results)
    print(json.dumps(results, indent=2, sort_keys=True))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
