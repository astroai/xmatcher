#!/usr/bin/env python3
"""CANFAR job: spine×survey match on pinned k-pop HATS trees (--no-sync).

Uses local mirrors under ``inputs/_cache`` for Gaia + 2MASS (+ optional
SkyMapper). Intended after LSDB/LIneA pin syncs succeed.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

WORK = Path(os.environ.get("PENSUS_WORK_ROOT", "/arc/projects/k-pop/pensus"))
CODE = Path(os.environ.get("PENSUS_CODE_ROOT", str(WORK / "code")))
VENDOR_SRC = Path(os.environ.get("PENSUS_XMATCHER_SRC", str(CODE / "vendor" / "xmatcher" / "src")))
CACHE = Path(os.environ.get("XMATCHER_CACHE_ROOT", str(WORK / "inputs" / "_cache")))
# Small extragalactic-ish cone for a go/no-go match on real pins.
DEFAULT_RA = float(os.environ.get("PENSUS_MATCH_RA", "150.1"))
DEFAULT_DEC = float(os.environ.get("PENSUS_MATCH_DEC", "2.2"))
DEFAULT_RADIUS_DEG = float(os.environ.get("PENSUS_MATCH_RADIUS_DEG", "0.25"))
DEFAULT_RADIUS_ARCSEC = float(os.environ.get("PENSUS_MATCH_RADIUS_ARCSEC", "1.5"))
SURVEYS = [
    a.strip()
    for a in os.environ.get("PENSUS_MATCH_SURVEYS", "twomass_psc_hats,skymapper_dr4_hats").split(
        ","
    )
    if a.strip()
]
SPINE = os.environ.get("PENSUS_MATCH_SPINE", "gaia_lsdb_hats")


def _log(msg: str) -> None:
    print(msg, flush=True)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def bootstrap() -> dict:
    target = Path(os.environ.get("PENSUS_PYDEPS", "/scratch/pensus-pydeps"))
    target.mkdir(parents=True, exist_ok=True)
    for path in (target, VENDOR_SRC, CODE):
        if path.exists() and str(path) not in sys.path:
            sys.path.insert(0, str(path))
    os.environ["PYTHONPATH"] = os.pathsep.join(
        [str(p) for p in (target, VENDOR_SRC, CODE) if p.exists()]
        + ([os.environ["PYTHONPATH"]] if os.environ.get("PYTHONPATH") else [])
    )
    wanted = [
        ("polars", "polars"),
        ("pyarrow", "pyarrow"),
        ("numpy", "numpy"),
        ("yaml", "pyyaml"),
        ("astropy", "astropy"),
        ("scipy", "scipy"),
        ("pyvo", "pyvo"),
    ]
    missing = []
    for mod, pip_name in wanted:
        try:
            __import__(mod)
        except ImportError:
            missing.append(pip_name)
    pip_rc = 0
    pip_err = ""
    if missing:
        cmd = [sys.executable, "-m", "pip", "install", "--target", str(target), "--quiet", *missing]
        _log("+ " + " ".join(cmd))
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
        pip_rc = proc.returncode
        pip_err = (proc.stderr or "")[-1500:]
        if str(target) not in sys.path:
            sys.path.insert(0, str(target))
    still = []
    for mod, _pip_name in wanted:
        try:
            __import__(mod)
        except ImportError:
            still.append(mod)
    try:
        import xmatcher  # noqa: F401

        xm_ok = True
        xm_err = ""
    except ImportError as exc:
        xm_ok = False
        xm_err = f"{type(exc).__name__}: {exc}"
    return {
        "ok": xm_ok and not still,
        "target": str(target),
        "requested": missing,
        "still_missing": still,
        "xmatcher_ok": xm_ok,
        "xmatcher_error": xm_err,
        "returncode": pip_rc,
        "stderr_tail": pip_err,
    }


def main(argv: list[str] | None = None) -> int:
    _ = argv
    run_id = os.environ.get("PENSUS_RUN_ID") or time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    os.environ.setdefault("PENSUS_RUN_ID", run_id)
    os.environ["XMATCHER_CACHE_ROOT"] = str(CACHE)
    out_dir = WORK / "tiles" / f"pinned-match-{run_id}"
    out_dir.mkdir(parents=True, exist_ok=True)
    results: dict = {
        "run_id": run_id,
        "started_at_utc": datetime.now(UTC).isoformat(),
        "host": os.uname().nodename,
        "spine": SPINE,
        "surveys": SURVEYS,
        "cone": {
            "ra": DEFAULT_RA,
            "dec": DEFAULT_DEC,
            "radius_deg": DEFAULT_RADIUS_DEG,
            "match_radius_arcsec": DEFAULT_RADIUS_ARCSEC,
        },
        "cache_root": str(CACHE),
        "steps": [],
    }
    _log("== bootstrap ==")
    results["bootstrap"] = bootstrap()
    if not results["bootstrap"].get("ok"):
        results["ok"] = False
        results["error"] = "bootstrap failed"
        results["finished_at_utc"] = datetime.now(UTC).isoformat()
        _write_json(WORK / "logs" / run_id / "pinned-match-report.json", results)
        print(json.dumps(results, indent=2, sort_keys=True))
        return 2

    def _resolve_local_tree(alias: str) -> Path:
        """Map a registry alias to its mirrored HATS directory under CACHE."""
        # Preferred: xmatcher locate_mirrored (same version hash as sync).
        try:
            from xmatcher.crossmatch import CrossMatch
            from xmatcher.mirror import locate_mirrored

            cm = CrossMatch()
            src = cm.resolve_source(alias)
            root, rel = locate_mirrored(src, cache_root=str(CACHE))
            path = Path(root) / rel if rel else Path(root)
            if (path / "properties").is_file() or (path / "dataset").is_dir():
                return path
        except Exception as exc:  # noqa: BLE001
            _log(f"locate_mirrored({alias}) failed: {type(exc).__name__}: {exc}")
        # Fallback: first child of CACHE/alias with properties.
        base = CACHE / alias
        if (base / "properties").is_file():
            return base
        if base.is_dir():
            for child in sorted(base.iterdir()):
                if child.is_dir() and (
                    (child / "properties").is_file() or (child / "dataset").is_dir()
                ):
                    return child
        raise FileNotFoundError(f"no local HATS tree for {alias} under {CACHE}")

    try:
        spine_path = _resolve_local_tree(SPINE)
    except Exception as exc:  # noqa: BLE001
        results["ok"] = False
        results["error"] = f"spine resolve failed: {type(exc).__name__}: {exc}"
        results["finished_at_utc"] = datetime.now(UTC).isoformat()
        _write_json(WORK / "logs" / run_id / "pinned-match-report.json", results)
        print(json.dumps(results, indent=2, sort_keys=True))
        return 2
    results["spine_path"] = str(spine_path)

    overall_ok = True
    for survey in SURVEYS:
        out = out_dir / f"{SPINE}__{survey}.parquet"
        log_dir = WORK / "logs" / run_id
        log_dir.mkdir(parents=True, exist_ok=True)
        stdout_path = log_dir / f"match-{survey}.stdout.log"
        stderr_path = log_dir / f"match-{survey}.stderr.log"
        try:
            survey_path = _resolve_local_tree(survey)
        except Exception as exc:  # noqa: BLE001
            overall_ok = False
            results["steps"].append(
                {
                    "survey": survey,
                    "ok": False,
                    "error": f"resolve failed: {type(exc).__name__}: {exc}",
                }
            )
            continue
        cmd = [
            sys.executable,
            "-m",
            "xmatcher",
            "match",
            str(spine_path),
            str(survey_path),
            "--engine",
            "fast",
            "--fallback-policy",
            "error",
            "-r",
            str(DEFAULT_RADIUS_ARCSEC),
            "--ra",
            str(DEFAULT_RA),
            "--dec",
            str(DEFAULT_DEC),
            "--radius-deg",
            str(DEFAULT_RADIUS_DEG),
            "-o",
            str(out),
            "-v",
        ]
        _log("+ " + " ".join(cmd))
        t0 = time.perf_counter()
        with stdout_path.open("w") as out_f, stderr_path.open("w") as err_f:
            proc = subprocess.run(cmd, stdout=out_f, stderr=err_f, text=True, check=False)
        elapsed = round(time.perf_counter() - t0, 3)
        step_ok = proc.returncode == 0 and out.is_file()
        if not step_ok:
            overall_ok = False
        rows = None
        if out.is_file():
            try:
                import polars as pl

                rows = pl.scan_parquet(out).select(pl.len()).collect().item()
            except Exception as exc:  # noqa: BLE001
                rows = f"count_failed:{type(exc).__name__}"
        results["steps"].append(
            {
                "survey": survey,
                "survey_path": str(survey_path),
                "cmd": cmd,
                "returncode": proc.returncode,
                "elapsed_s": elapsed,
                "ok": step_ok,
                "output": str(out),
                "rows": rows,
                "stdout_tail": stdout_path.read_text()[-2000:] if stdout_path.exists() else "",
                "stderr_tail": stderr_path.read_text()[-2000:] if stderr_path.exists() else "",
            }
        )
        _log(json.dumps({"survey": survey, "ok": step_ok, "elapsed_s": elapsed, "rows": rows}))

    results["ok"] = overall_ok
    results["finished_at_utc"] = datetime.now(UTC).isoformat()
    report = WORK / "logs" / run_id / "pinned-match-report.json"
    _write_json(report, results)
    _log(json.dumps({"report": str(report), "ok": overall_ok}, sort_keys=True))
    print(json.dumps(results, indent=2, sort_keys=True))
    return 0 if overall_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
