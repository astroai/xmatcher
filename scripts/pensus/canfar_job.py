#!/usr/bin/env python3
"""Pensus CANFAR headless job entrypoint (Day-0 link + spike + probes).

Designed to run inside a Skaha headless session with /arc and /scratch mounted::

    python3 /arc/projects/k-pop/pensus/code/launch.py

Uploads live under ``/arc/projects/k-pop/pensus/code/``.
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
SCRATCH = Path(
    os.environ.get(
        "PENSUS_SCRATCH_ROOT", f"/scratch/pensus-{os.environ.get('PENSUS_RUN_ID', 'job')}"
    )
)
PRISTINE_EXISTING = Path(
    os.environ.get(
        "PENSUS_PRISTINE_HATS",
        "/arc/projects/k-pop/catalogues/lsdb/pristine_internal",
    )
)
CODE = Path(__file__).resolve().parent
GAIA_HATS = "https://data.lsdb.io/hats/gaia_dr3/gaia"
TWOMASS_HATS = "https://data.lsdb.io/hats/two_mass/two_mass"
SKYMAPPER_HATS = "https://data.lsdb.io/hats/skymapper_dr4/skymapper_dr4_lightcurves"


def _log(msg: str) -> None:
    print(msg, flush=True)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def probe_http(url: str, timeout: float = 20.0) -> dict:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            body = resp.read(400).decode("utf-8", "replace")
            return {
                "url": url,
                "ok": True,
                "status": getattr(resp, "status", 200),
                "head": body[:200],
            }
    except Exception as exc:  # noqa: BLE001
        return {"url": url, "ok": False, "error": f"{type(exc).__name__}: {exc}"}


def bootstrap_deps() -> dict:
    """Install minimal runtime deps into the user site (base image lacks polars)."""
    wanted = ["polars", "pyarrow", "hats", "cdshealpix", "numpy"]
    missing: list[str] = []
    for name in wanted:
        try:
            __import__(name if name != "cdshealpix" else "cdshealpix")
        except ImportError:
            missing.append(name)
    if not missing:
        return {"installed": [], "already": wanted, "ok": True}
    cmd = [sys.executable, "-m", "pip", "install", "--user", "--quiet", *missing]
    _log("+ " + " ".join(cmd))
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    # Ensure user site is importable even if PYTHONNOUSERSITE was set.
    import site

    user_site = site.getusersitepackages()
    if user_site not in sys.path:
        sys.path.insert(0, user_site)
    still: list[str] = []
    for name in missing:
        try:
            __import__(name if name != "cdshealpix" else "cdshealpix")
        except ImportError:
            still.append(name)
    return {
        "requested": missing,
        "still_missing": still,
        "returncode": proc.returncode,
        "stderr_tail": (proc.stderr or "")[-1500:],
        "ok": not still,
    }


def step_day0() -> dict:
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
    (WORK / "docs" / "env.pristine.sh").write_text(
        f'export XMATCHER_PRISTINE_DR2_HATS="{PRISTINE_EXISTING}"\n'
    )
    return payload


def step_probes() -> dict:
    urls = {
        "gaia_lsdb": f"{GAIA_HATS}/properties",
        "two_mass": f"{TWOMASS_HATS}/properties",
        "skymapper_lsdb": f"{SKYMAPPER_HATS}/properties",
        "catwise_cds": "https://vizcat.cds.unistra.fr/hats/catwise2020/main/properties",
        "galex_cds": "https://vizcat.cds.unistra.fr/hats/galex_gr67/main/properties",
    }
    results = {name: probe_http(url) for name, url in urls.items()}
    cds_up = bool(results["catwise_cds"]["ok"] or results["galex_cds"]["ok"])
    fallback = {
        "cds_up": cds_up,
        "fire": not cds_up,
        "surveys": ["catwise2020", "galex_guvcat", "vhs_dr5"] if not cds_up else [],
        "action": (
            "start chunked builds into /arc/projects/k-pop/pensus/hats_build/{survey}"
            if not cds_up
            else "no action"
        ),
        "probed_at_utc": datetime.now(UTC).isoformat(),
    }
    _write_json(WORK / "docs" / "cds-fallback-status.json", fallback)
    return {
        "probes": results,
        "cds_up": cds_up,
        "cds_fallback_recommended": not cds_up,
        "cds_fallback": fallback,
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
    return {"returncode": rc, "campaign": campaign, "report": report, "ok": rc == 0}


def step_real_tile_spike() -> dict:
    """Exercise tile markers on real Pristine ARC partitions; probe remote HATS metadata."""
    try:
        import numpy as np
        import polars as pl
    except ImportError as exc:
        return {"ok": False, "skipped": True, "reason": f"import failed: {exc}"}

    sys.path.insert(0, str(CODE.parent))
    from pensus.mutual_best import mutual_best_edges
    from pensus.tile_geometry import margin_arcsec
    from pensus.tile_markers import TileMarker, file_sha256, write_marker

    order, pixel = 5, 0
    margin = margin_arcsec(r_max_arcsec=3.0, cascade_levels=3, pm_halo_arcsec=17.0)
    SCRATCH.mkdir(parents=True, exist_ok=True)
    campaign = f"canfar-real-{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}"
    tile_dir = WORK / "tiles" / campaign / f"o{order}" / f"p{pixel}"
    tile_dir.mkdir(parents=True, exist_ok=True)

    t0 = time.perf_counter()
    try:
        remote = {
            "gaia": probe_http(f"{GAIA_HATS}/properties"),
            "two_mass": probe_http(f"{TWOMASS_HATS}/properties"),
            "skymapper": probe_http(f"{SKYMAPPER_HATS}/properties"),
        }
        gaia_npix = twomass_npix = None
        try:
            import hats

            gaia_npix = len(list(hats.read_hats(GAIA_HATS).get_healpix_pixels()))
            twomass_npix = len(list(hats.read_hats(TWOMASS_HATS).get_healpix_pixels()))
        except Exception as exc:  # noqa: BLE001
            remote["hats_pixel_lists"] = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}

        files = sorted((PRISTINE_EXISTING / "dataset").rglob("*.parquet"))
        if not files:
            return {"ok": False, "reason": "no pristine parquet partitions found"}
        sample_files = files[:3]
        left = pl.concat([pl.read_parquet(path) for path in sample_files])
        if "ra" not in left.columns or "dec" not in left.columns:
            ra_col = next(c for c in left.columns if c.lower() in {"ra", "raj2000"})
            dec_col = next(c for c in left.columns if c.lower() in {"dec", "dej2000"})
            left = left.rename({ra_col: "ra", dec_col: "dec"})
        id_col = "source_id" if "source_id" in left.columns else left.columns[0]
        n = min(left.height, 5000)
        left = left.head(n)
        right = left.select(
            [
                pl.col(id_col).alias("survey_id"),
                (pl.col("ra") + 0.00001).alias("ra"),
                pl.col("dec"),
            ]
        )
        lra = left["ra"].to_numpy()
        ldec = left["dec"].to_numpy()
        rra = right["ra"].to_numpy()
        seps = np.abs(lra - rra) * 3600.0 * np.cos(np.deg2rad(ldec))
        edges = pl.DataFrame(
            {
                "spine_id": left[id_col].cast(pl.Utf8),
                "survey_id": right["survey_id"].cast(pl.Utf8),
                "sep_arcsec": seps,
            }
        )
        mb = mutual_best_edges(edges)
        out_parquet = tile_dir / "edges_mutual_best.parquet"
        mb.write_parquet(out_parquet)
        digest = file_sha256(out_parquet)
        marker = TileMarker(
            campaign=campaign,
            order=order,
            pixel=pixel,
            rows=mb.height,
            bytes=out_parquet.stat().st_size,
            sha256=digest,
        )
        write_marker(WORK / "tiles" / campaign / "markers", marker)
        report = {
            "ok": True,
            "campaign": campaign,
            "margin_arcsec": margin,
            "pristine_sample_files": [str(path) for path in sample_files],
            "rows_in": n,
            "rows_out": mb.height,
            "elapsed_s": round(time.perf_counter() - t0, 3),
            "remote_probes": remote,
            "gaia_npix": gaia_npix,
            "twomass_npix": twomass_npix,
            "note": (
                "Real Pristine partitions on ARC + remote HATS metadata probes; "
                "full Gaia/2MASS pixel materialization is the next sync job."
            ),
            "output": str(out_parquet),
        }
        _write_json(WORK / "logs" / campaign / "real-spike-report.json", report)
        return report
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
            "traceback": traceback.format_exc()[-3000:],
        }


def step_arm_cds_fallback() -> dict:
    """If CDS is down, create hats_build stubs and a start ledger for CatWISE/GALEX."""
    status_path = WORK / "docs" / "cds-fallback-status.json"
    status = json.loads(status_path.read_text()) if status_path.is_file() else {"fire": True}
    if not status.get("fire"):
        return {"armed": False, "reason": "cds_up"}
    created = []
    for survey in status.get("surveys", []):
        path = WORK / "hats_build" / survey
        path.mkdir(parents=True, exist_ok=True)
        marker = {
            "survey": survey,
            "status": "awaiting_chunked_build",
            "reason": "CDS HATS unreachable from CANFAR session",
            "scratch_budget_gib": 250,
            "created_at_utc": datetime.now(UTC).isoformat(),
        }
        _write_json(path / "PENSUS_BUILD.json", marker)
        created.append(str(path))
    return {"armed": True, "created": created}


def main(argv: list[str] | None = None) -> int:
    args = list(argv if argv is not None else sys.argv[1:])
    mode = args[0] if args else "all"
    run_id = os.environ.get("PENSUS_RUN_ID") or time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    os.environ.setdefault("PENSUS_RUN_ID", run_id)
    started = datetime.now(UTC).isoformat()
    results: dict = {
        "run_id": run_id,
        "mode": mode,
        "started_at_utc": started,
        "host": os.uname().nodename,
    }
    failures: list[str] = []
    try:
        _log("== bootstrap ==")
        results["bootstrap"] = bootstrap_deps()
        if not results["bootstrap"].get("ok"):
            failures.append("bootstrap")

        if mode in {"day0", "all"}:
            _log("== day0 ==")
            results["day0"] = step_day0()
        if mode in {"probes", "all"}:
            _log("== probes ==")
            results["probes"] = step_probes()
            results["cds_fallback"] = step_arm_cds_fallback()
        if mode in {"spike", "all"}:
            _log("== spike demo ==")
            results["spike"] = step_spike_demo()
            if not results["spike"].get("ok"):
                failures.append("spike")
        if mode in {"real", "all"}:
            _log("== real tile spike ==")
            results["real_spike"] = step_real_tile_spike()
            if not results["real_spike"].get("ok"):
                failures.append("real_spike")
        results["ok"] = not failures
        results["failures"] = failures
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
