#!/usr/bin/env python3
"""Pensus CANFAR headless job entrypoint (Day-0 link + spike + probes).

Designed to run inside a Skaha headless session with /arc and /scratch mounted::

    python3 /arc/projects/k-pop/pensus/code/canfar_job.py day0
    python3 /arc/projects/k-pop/pensus/code/canfar_job.py spike
    python3 /arc/projects/k-pop/pensus/code/canfar_job.py all

This agent VM uploads a self-contained copy under
``/arc/projects/k-pop/pensus/code/`` so the session does not need git.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
import traceback
import urllib.request
from datetime import UTC, datetime
from pathlib import Path

WORK = Path(os.environ.get("PENSUS_WORK_ROOT", "/arc/projects/k-pop/pensus"))
PUBLIC = Path(os.environ.get("PENSUS_PUBLIC_ROOT", "/arc/projects/hats/pensus"))
SCRATCH = Path(os.environ.get("PENSUS_SCRATCH_ROOT", f"/scratch/pensus-{os.environ.get('PENSUS_RUN_ID', 'job')}"))
PRISTINE_EXISTING = Path(
    os.environ.get(
        "PENSUS_PRISTINE_HATS",
        "/arc/projects/k-pop/catalogues/lsdb/pristine_internal",
    )
)
CODE = Path(__file__).resolve().parent


def _log(msg: str) -> None:
    print(msg, flush=True)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def probe_http(url: str, timeout: float = 30.0) -> dict:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            body = resp.read(400).decode("utf-8", "replace")
            return {"url": url, "ok": True, "status": getattr(resp, "status", 200), "head": body[:200]}
    except Exception as exc:  # noqa: BLE001 - probe must never crash the job
        return {"url": url, "ok": False, "error": f"{type(exc).__name__}: {exc}"}


def step_day0() -> dict:
    """Link Pristine HATS into pensus inputs + ensure layout."""
    for sub in ("staging", "inputs", "hats_build", "tiles", "drafts/views", "logs", "docs", "code"):
        (WORK / sub).mkdir(parents=True, exist_ok=True)
    for sub in ("views", "sidecars", "docs"):
        (PUBLIC / sub).mkdir(parents=True, exist_ok=True)

    pointer = WORK / "inputs" / "pristine_dr2" / "SOURCE.json"
    pointer.parent.mkdir(parents=True, exist_ok=True)
    pristine_ok = PRISTINE_EXISTING.is_dir() and (PRISTINE_EXISTING / "properties").is_file()
    payload = {
        "survey": "pristine_dr2",
        "join": "id_join_on_gaia_source_id",
        "positional_match": False,
        "hats_path": str(PRISTINE_EXISTING),
        "exists": pristine_ok,
        "env": "XMATCHER_PRISTINE_DR2_HATS",
        "probed_at_utc": datetime.now(UTC).isoformat(),
    }
    if pristine_ok:
        props = (PRISTINE_EXISTING / "properties").read_text(errors="replace")
        payload["properties_head"] = props.splitlines()[:20]
    _write_json(pointer, payload)
    # Convenience env file for later sessions.
    (WORK / "docs" / "env.pristine.sh").write_text(
        f'export XMATCHER_PRISTINE_DR2_HATS="{PRISTINE_EXISTING}"\n'
    )
    return payload


def step_probes() -> dict:
    urls = {
        "gaia_lsdb": "https://data.lsdb.io/hats/gaia_dr3/gaia/properties",
        "two_mass": "https://data.lsdb.io/hats/two_mass/two_mass/properties",
        "skymapper_legacy_path": "https://data.lsdb.io/hats/skymapper_dr4/catalog/sky_mapper_dr4/properties",
        "catwise_cds": "https://vizcat.cds.unistra.fr/hats/catwise2020/main/properties",
        "galex_cds": "https://vizcat.cds.unistra.fr/hats/galex_gr67/main/properties",
        "ps1_mast": "https://stpubdata.s3.us-east-1.amazonaws.com/panstarrs/ps1/public/hats/otmo/properties",
    }
    results = {name: probe_http(url) for name, url in urls.items()}
    cds_up = bool(results["catwise_cds"]["ok"] or results["galex_cds"]["ok"])
    return {
        "probes": results,
        "cds_up": cds_up,
        "cds_fallback_recommended": not cds_up,
        "scratch_exists": Path("/scratch").exists(),
        "arc_work_exists": WORK.exists(),
        "df_scratch": _df("/scratch"),
        "df_arc": _df("/arc"),
    }


def _df(path: str) -> dict:
    p = Path(path)
    if not p.exists():
        return {"path": path, "mounted": False}
    usage = shutil.disk_usage(p)
    return {
        "path": path,
        "mounted": True,
        "total_gib": round(usage.total / (1024**3), 3),
        "free_gib": round(usage.free / (1024**3), 3),
    }


def step_spike_demo() -> dict:
    """Offline tile spike (always runnable) to validate scripts on the session image."""
    sys.path.insert(0, str(CODE.parent))
    from pensus.spike import main as spike_main

    campaign = f"canfar-spike-{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}"
    rc = spike_main(
        [
            "--work-root",
            str(WORK),
            "--campaign",
            campaign,
            "--order",
            "4",
            "--pixel",
            "0",
            "--demo",
        ]
    )
    report_path = WORK / "logs" / campaign / "spike-report.json"
    report = json.loads(report_path.read_text()) if report_path.is_file() else {}
    return {"returncode": rc, "campaign": campaign, "report": report}


def step_project_pixel() -> dict:
    """Optional: project one Gaia HEALPix pixel into pensus inputs for real-tree spike.

    Uses hats + lsdb if available; otherwise records skip reason.
    """
    out = WORK / "inputs" / "gaia_dr3" / "pixel_spike"
    out.mkdir(parents=True, exist_ok=True)
    SCRATCH.mkdir(parents=True, exist_ok=True)
    try:
        import hats  # noqa: F401
    except ImportError as exc:
        return {"skipped": True, "reason": f"hats not importable: {exc}"}

    # Prefer a tiny HTTP property check before heavy I/O.
    gaia = probe_http("https://data.lsdb.io/hats/gaia_dr3/gaia/properties")
    if not gaia["ok"]:
        return {"skipped": True, "reason": "gaia properties unreachable", "probe": gaia}

    cmd = [
        sys.executable,
        "-c",
        (
            "import json,hats;"
            "cat=hats.read_hats('https://data.lsdb.io/hats/gaia_dr3/gaia');"
            "print(json.dumps({'npix': len(cat.get_healpix_pixels()), "
            "'name': getattr(cat,'collection_or_catalog_name',None)}))"
        ),
    ]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=300, check=False)
    except Exception as exc:  # noqa: BLE001
        return {"skipped": True, "reason": f"hats read failed: {exc}"}
    return {
        "skipped": proc.returncode != 0,
        "returncode": proc.returncode,
        "stdout": proc.stdout[-2000:],
        "stderr": proc.stderr[-2000:],
        "out": str(out),
    }


def main(argv: list[str] | None = None) -> int:
    args = list(argv if argv is not None else sys.argv[1:])
    mode = args[0] if args else "all"
    run_id = os.environ.get("PENSUS_RUN_ID") or time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    os.environ.setdefault("PENSUS_RUN_ID", run_id)
    started = datetime.now(UTC).isoformat()
    results: dict = {"run_id": run_id, "mode": mode, "started_at_utc": started, "host": os.uname().nodename}
    try:
        if mode in {"day0", "all"}:
            _log("== day0 ==")
            results["day0"] = step_day0()
        if mode in {"probes", "all"}:
            _log("== probes ==")
            results["probes"] = step_probes()
        if mode in {"spike", "all"}:
            _log("== spike demo ==")
            results["spike"] = step_spike_demo()
        if mode in {"project", "all"}:
            _log("== project pixel ==")
            results["project"] = step_project_pixel()
        results["ok"] = True
    except Exception as exc:  # noqa: BLE001
        results["ok"] = False
        results["error"] = f"{type(exc).__name__}: {exc}"
        results["traceback"] = traceback.format_exc()
        _log(results["traceback"])
    results["finished_at_utc"] = datetime.now(UTC).isoformat()
    out = WORK / "logs" / run_id / "canfar-job-report.json"
    _write_json(out, results)
    _log(json.dumps({"report": str(out), "ok": results.get("ok")}, sort_keys=True))
    print(json.dumps(results, indent=2, sort_keys=True))
    return 0 if results.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
