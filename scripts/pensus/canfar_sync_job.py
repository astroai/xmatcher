#!/usr/bin/env python3
"""CANFAR job: pin selected LSDB HATS trees into k-pop/pensus/inputs.

Bootstraps runtime deps into ``/scratch/pensus-pydeps`` (Skaha sets
``PYTHONNOUSERSITE=1``, so ``pip install --user`` is invisible). Prefers an
ARC-vendored ``xmatcher`` source tree so the session does not need PyPI
publication. Mirrors into ``inputs/_cache`` (k-pop quota) and writes
``SOURCE.json`` pointers under ``inputs/<alias>/``.
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
CODE = Path(os.environ.get("PENSUS_CODE_ROOT", str(WORK / "code")))
VENDOR_SRC = Path(
    os.environ.get(
        "PENSUS_XMATCHER_SRC",
        str(CODE / "vendor" / "xmatcher" / "src"),
    )
)
# Start with 2MASS then SkyMapper; Gaia is a follow-on campaign (much larger).
DEFAULT_ALIASES = os.environ.get(
    "PENSUS_SYNC_ALIASES", "twomass_psc_hats,skymapper_dr4_hats"
).split(",")


def _log(msg: str) -> None:
    print(msg, flush=True)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def _deps_target() -> Path:
    explicit = os.environ.get("PENSUS_PYDEPS")
    if explicit:
        path = Path(explicit)
    elif Path("/scratch").exists():
        path = Path("/scratch/pensus-pydeps")
    else:
        path = WORK / "code" / "pydeps"
    path.mkdir(parents=True, exist_ok=True)
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))
    os.environ["PYTHONPATH"] = str(path) + (
        os.pathsep + os.environ["PYTHONPATH"] if os.environ.get("PYTHONPATH") else ""
    )
    return path


def bootstrap_runtime() -> dict:
    """Install sync runtime deps into a scratch/ARC target (not --user)."""
    target = _deps_target()
    if VENDOR_SRC.is_dir() and str(VENDOR_SRC) not in sys.path:
        sys.path.insert(0, str(VENDOR_SRC))
        os.environ["PYTHONPATH"] = str(VENDOR_SRC) + os.pathsep + os.environ.get("PYTHONPATH", "")

    wanted = ["polars", "pyarrow", "numpy", "yaml", "astropy", "scipy", "pyvo"]
    pip_names = {
        "yaml": "pyyaml",
        "polars": "polars",
        "pyarrow": "pyarrow",
        "numpy": "numpy",
        "astropy": "astropy",
        "scipy": "scipy",
        "pyvo": "pyvo",
    }
    missing: list[str] = []
    for mod in wanted:
        try:
            __import__(mod)
        except ImportError:
            missing.append(pip_names[mod])

    pip_rc = 0
    pip_err = ""
    if missing:
        cmd = [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--target",
            str(target),
            "--quiet",
            *missing,
        ]
        _log("+ " + " ".join(cmd))
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
        pip_rc = proc.returncode
        pip_err = (proc.stderr or "")[-2000:]
        if str(target) not in sys.path:
            sys.path.insert(0, str(target))

    still: list[str] = []
    for mod in wanted:
        try:
            __import__(mod)
        except ImportError:
            still.append(mod)

    xmatcher_ok = False
    xmatcher_err = ""
    try:
        import xmatcher  # noqa: F401

        xmatcher_ok = True
    except ImportError as exc:
        xmatcher_err = f"{type(exc).__name__}: {exc}"

    return {
        "target": str(target),
        "vendor_src": str(VENDOR_SRC),
        "vendor_exists": VENDOR_SRC.is_dir(),
        "requested": missing,
        "still_missing": still,
        "xmatcher_import_ok": xmatcher_ok,
        "xmatcher_error": xmatcher_err,
        "returncode": pip_rc,
        "stderr_tail": pip_err,
        "ok": not still and xmatcher_ok,
        "python": sys.executable,
        "sys_path_head": sys.path[:6],
    }


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


def _write_source_pointer(alias: str, cache: Path, ok: bool, extra: dict) -> Path:
    dest = WORK / "inputs" / alias
    dest.mkdir(parents=True, exist_ok=True)
    payload = {
        "alias": alias,
        "cache_root": str(cache),
        "ok": ok,
        "probed_at_utc": datetime.now(UTC).isoformat(),
        **extra,
    }
    path = dest / "SOURCE.json"
    _write_json(path, payload)
    return path


def main(argv: list[str] | None = None) -> int:
    raw = argv if argv is not None else sys.argv[1:]
    aliases = [a.strip() for a in (raw or DEFAULT_ALIASES) if a.strip()]
    run_id = os.environ.get("PENSUS_RUN_ID") or time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    os.environ.setdefault("PENSUS_RUN_ID", run_id)
    cache = Path(os.environ.get("XMATCHER_CACHE_ROOT", str(WORK / "inputs" / "_cache")))
    cache.mkdir(parents=True, exist_ok=True)
    os.environ["XMATCHER_CACHE_ROOT"] = str(cache)

    results: dict = {
        "run_id": run_id,
        "started_at_utc": datetime.now(UTC).isoformat(),
        "aliases": aliases,
        "cache_root": str(cache),
        "host": os.uname().nodename,
        "steps": [],
    }

    _log("== bootstrap ==")
    results["bootstrap"] = bootstrap_runtime()
    if not results["bootstrap"].get("ok"):
        results["ok"] = False
        results["error"] = (
            "bootstrap failed; still_missing="
            + ",".join(results["bootstrap"].get("still_missing") or [])
            + f"; xmatcher={results['bootstrap'].get('xmatcher_error')}"
        )
        out = WORK / "logs" / run_id / "sync-job-report.json"
        results["finished_at_utc"] = datetime.now(UTC).isoformat()
        _write_json(out, results)
        print(json.dumps(results, indent=2, sort_keys=True))
        return 2

    cmd_base = find_xmatcher()
    if cmd_base is None:
        results["ok"] = False
        results["error"] = "xmatcher not importable after bootstrap"
        out = WORK / "logs" / run_id / "sync-job-report.json"
        results["finished_at_utc"] = datetime.now(UTC).isoformat()
        _write_json(out, results)
        print(json.dumps(results, indent=2, sort_keys=True))
        return 2

    workers = os.environ.get("PENSUS_SYNC_THREADS", "8")
    min_free = os.environ.get("PENSUS_SYNC_MIN_FREE_GB", "50")
    overall_ok = True

    for alias in aliases:
        log_dir = WORK / "logs" / run_id
        log_dir.mkdir(parents=True, exist_ok=True)
        stdout_path = log_dir / f"sync-{alias}.stdout.log"
        stderr_path = log_dir / f"sync-{alias}.stderr.log"
        cmd = [
            *cmd_base,
            "sync",
            alias,
            "--cache-root",
            str(cache),
            "--threads",
            str(workers),
            "--min-free-gb",
            str(min_free),
            "-v",
        ]
        _log("+ " + " ".join(cmd))
        t0 = time.perf_counter()
        with stdout_path.open("w") as out_f, stderr_path.open("w") as err_f:
            proc = subprocess.run(cmd, stdout=out_f, stderr=err_f, text=True, check=False)
        elapsed = round(time.perf_counter() - t0, 3)
        stdout_tail = stdout_path.read_text()[-2000:] if stdout_path.exists() else ""
        stderr_tail = stderr_path.read_text()[-2000:] if stderr_path.exists() else ""
        step_ok = proc.returncode == 0
        if not step_ok:
            overall_ok = False
        pointer = _write_source_pointer(
            alias,
            cache,
            step_ok,
            {
                "cmd": cmd,
                "returncode": proc.returncode,
                "elapsed_s": elapsed,
                "stdout_log": str(stdout_path),
                "stderr_log": str(stderr_path),
            },
        )
        results["steps"].append(
            {
                "alias": alias,
                "cmd": cmd,
                "returncode": proc.returncode,
                "elapsed_s": elapsed,
                "ok": step_ok,
                "stdout_tail": stdout_tail,
                "stderr_tail": stderr_tail,
                "source_pointer": str(pointer),
            }
        )
        _log(json.dumps({"alias": alias, "ok": step_ok, "elapsed_s": elapsed}))

    results["ok"] = overall_ok
    results["finished_at_utc"] = datetime.now(UTC).isoformat()
    out = WORK / "logs" / run_id / "sync-job-report.json"
    _write_json(out, results)
    _log(json.dumps({"report": str(out), "ok": overall_ok}, sort_keys=True))
    print(json.dumps(results, indent=2, sort_keys=True))
    return 0 if overall_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
