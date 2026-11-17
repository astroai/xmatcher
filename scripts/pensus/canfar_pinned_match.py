#!/usr/bin/env python3
"""CANFAR job: cone extract + match on pinned k-pop HATS trees.

HATS native ``fast`` path materializes whole catalogues when left/right
trees have different HEALPix orders (Gaia vs 2MASS). For go/no-go we
extract only cone pixels to parquet, then match those local frames.
"""

from __future__ import annotations

import importlib
import json
import math
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
        ("cdshealpix", "cdshealpix"),
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
        pip_err = (proc.stderr or "")[-1500:]
        if str(target) not in sys.path:
            sys.path.insert(0, str(target))
        # The target dir may already be on sys.path with a stale finder cache.
        importlib.invalidate_caches()
    still = []
    import_errors: dict[str, str] = {}
    for mod, _pip in wanted:
        try:
            __import__(mod)
        except ImportError as exc:
            still.append(mod)
            import_errors[mod] = f"{type(exc).__name__}: {exc}"
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
        "import_errors": import_errors,
        "xmatcher_ok": xm_ok,
        "xmatcher_error": xm_err,
        "returncode": pip_rc,
        "stderr_tail": pip_err,
    }


def _resolve_local_tree(alias: str) -> Path:
    # URL syncs (e.g. LIneA GALEX) land under a host-derived folder name while
    # SOURCE.json is written under the pensus alias (galex_linea → galex/).
    candidates = [alias]
    official: list[Path] = []
    source = WORK / "inputs" / alias / "SOURCE.json"
    if source.is_file():
        try:
            meta = json.loads(source.read_text())
        except (OSError, json.JSONDecodeError):
            meta = {}
        hint = meta.get("cache_alias_hint")
        if isinstance(hint, str) and hint.strip():
            candidates.append(hint.strip())
        for key in ("promoted_hats_path", "hats_path"):
            value = meta.get(key)
            if isinstance(value, str) and value.strip():
                official.append(Path(value.strip()))
    if alias.endswith("_linea"):
        candidates.append(alias[: -len("_linea")])
    seen: set[str] = set()
    for name in candidates:
        if name in seen:
            continue
        seen.add(name)
        base = CACHE / name
        if (base / "properties").is_file():
            return base
        if base.is_dir():
            for child in sorted(base.iterdir()):
                if child.is_dir() and (
                    (child / "properties").is_file() or (child / "dataset").is_dir()
                ):
                    return child
    # Official /arc/projects/hats/<survey>/<release> copy, used once the k-pop cache is gone.
    for root in official:
        if (root / "properties").is_file() or (root / "hats.properties").is_file():
            return root
    raise FileNotFoundError(f"no local HATS tree for {alias} under {CACHE}")


def _props(root: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    for name in ("properties", "hats.properties"):
        path = root / name
        if not path.is_file():
            continue
        for line in path.read_text().splitlines():
            if "=" in line and not line.strip().startswith("#"):
                key, val = line.split("=", 1)
                out[key.strip()] = val.strip()
        break
    return out


def _cone_pixels(order: int, ra: float, dec: float, radius_deg: float) -> set[int]:
    import astropy.units as u
    import cdshealpix as chp
    import numpy as np
    from astropy.coordinates import Latitude, Longitude

    depth = int(order)
    nside = 2**depth
    pix_scale_deg = math.degrees(math.sqrt(4.0 * math.pi / (12.0 * nside * nside)))
    radius = min(180.0, float(radius_deg) + 1.5 * pix_scale_deg) * u.deg
    lon = Longitude(ra * u.deg)
    lat = Latitude(dec * u.deg)
    ipix, _depths, _fully = chp.cone_search(lon, lat, radius, depth=depth, flat=True)
    return {int(p) for p in np.atleast_1d(ipix).astype(int).ravel() if int(p) >= 0}


def _list_pixel_files(root: Path) -> list[tuple[int, int, Path]]:
    base = root / "dataset"
    if not base.is_dir():
        base = root
    found: list[tuple[int, int, Path]] = []
    # A partition is either Npix=N.parquet or an Npix=N/ directory of parquet files (IRSA).
    for entry in sorted(base.glob("Norder=*/Dir=*/Npix=*")):
        try:
            order = int(entry.parent.parent.name.split("=")[1])
            pix = int(entry.name.removesuffix(".parquet").split("=")[1])
        except Exception:
            continue
        if entry.is_dir():
            found.extend((order, pix, f) for f in sorted(entry.glob("*.parquet")))
        elif entry.suffix == ".parquet":
            found.append((order, pix, entry))
    return found


def extract_cone(root: Path, out_parquet: Path, ra: float, dec: float, radius_deg: float) -> dict:
    import polars as pl

    props = _props(root)
    ra_col = props.get("hats_col_ra") or "ra"
    dec_col = props.get("hats_col_dec") or "dec"
    pixels = _list_pixel_files(root)
    if not pixels:
        raise FileNotFoundError(f"no parquet pixels under {root}")
    # Group by order; include any order whose pixel intersects the cone.
    wanted: set[tuple[int, int]] = set()
    for order in sorted({o for o, _, _ in pixels}):
        for pix in _cone_pixels(order, ra, dec, radius_deg):
            wanted.add((order, pix))
    import numpy as np

    cra, cdec = math.radians(ra), math.radians(dec)
    frames = []
    used = []
    for order, pix, path in pixels:
        if (order, pix) not in wanted:
            continue
        df = pl.read_parquet(path)
        if ra_col not in df.columns or dec_col not in df.columns:
            # SkyMapper uses raj2000/dej2000 already in props.
            raise KeyError(f"{path}: missing {ra_col}/{dec_col}; cols={df.columns[:12]}")
        used.append({"order": order, "pixel": pix, "path": str(path), "rows": df.height})
        # Cut each file before concatenating so wide catalogues hold one file in memory, not the cone's pixels.
        ra_v = np.deg2rad(df[ra_col].to_numpy())
        dec_v = np.deg2rad(df[dec_col].to_numpy())
        cos_d = np.sin(cdec) * np.sin(dec_v) + np.cos(cdec) * np.cos(dec_v) * np.cos(ra_v - cra)
        sep_deg = np.degrees(np.arccos(np.clip(cos_d, -1.0, 1.0)))
        frames.append(df.filter(pl.Series(sep_deg <= radius_deg)))
        del df
    if not frames:
        raise RuntimeError(f"cone selected 0 pixels under {root}")
    out = pl.concat(frames, how="diagonal_relaxed")
    out_parquet.parent.mkdir(parents=True, exist_ok=True)
    out.write_parquet(out_parquet)
    return {
        "rows": out.height,
        "pixels_used": used,
        "ra_col": ra_col,
        "dec_col": dec_col,
        "path": str(out_parquet),
    }


def main(argv: list[str] | None = None) -> int:
    _ = argv
    run_id = os.environ.get("PENSUS_RUN_ID") or time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    os.environ.setdefault("PENSUS_RUN_ID", run_id)
    os.environ["XMATCHER_CACHE_ROOT"] = str(CACHE)
    scratch = Path(os.environ.get("PENSUS_SCRATCH_ROOT", f"/scratch/pensus-match-{run_id}"))
    scratch.mkdir(parents=True, exist_ok=True)
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

    spine_path = _resolve_local_tree(SPINE)
    results["spine_path"] = str(spine_path)
    spine_extract = scratch / f"{SPINE}_cone.parquet"
    _log(f"== extract spine {spine_path} ==")
    results["spine_extract"] = extract_cone(
        spine_path, spine_extract, DEFAULT_RA, DEFAULT_DEC, DEFAULT_RADIUS_DEG
    )
    _log(json.dumps({"spine_rows": results["spine_extract"]["rows"]}))

    overall_ok = True
    for survey in SURVEYS:
        log_dir = WORK / "logs" / run_id
        log_dir.mkdir(parents=True, exist_ok=True)
        try:
            survey_path = _resolve_local_tree(survey)
            survey_extract = scratch / f"{survey}_cone.parquet"
            _log(f"== extract {survey} ==")
            extract_info = extract_cone(
                survey_path, survey_extract, DEFAULT_RA, DEFAULT_DEC, DEFAULT_RADIUS_DEG
            )
            out = out_dir / f"{SPINE}__{survey}.parquet"
            stdout_path = log_dir / f"match-{survey}.stdout.log"
            stderr_path = log_dir / f"match-{survey}.stderr.log"
            cmd = [
                sys.executable,
                "-m",
                "xmatcher",
                "match",
                str(spine_extract),
                str(survey_extract),
                "--engine",
                "fast",
                "--fallback-policy",
                "error",
                "-r",
                str(DEFAULT_RADIUS_ARCSEC),
                "--ra1",
                results["spine_extract"]["ra_col"],
                "--dec1",
                results["spine_extract"]["dec_col"],
                "--ra2",
                extract_info["ra_col"],
                "--dec2",
                extract_info["dec_col"],
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
                import polars as pl

                rows = pl.scan_parquet(out).select(pl.len()).collect().item()
            results["steps"].append(
                {
                    "survey": survey,
                    "survey_path": str(survey_path),
                    "extract": extract_info,
                    "cmd": cmd,
                    "returncode": proc.returncode,
                    "elapsed_s": elapsed,
                    "ok": step_ok,
                    "output": str(out),
                    "rows": rows,
                    "stdout_tail": stdout_path.read_text()[-1500:] if stdout_path.exists() else "",
                    "stderr_tail": stderr_path.read_text()[-1500:] if stderr_path.exists() else "",
                }
            )
            _log(json.dumps({"survey": survey, "ok": step_ok, "rows": rows, "elapsed_s": elapsed}))
        except Exception as exc:  # noqa: BLE001
            overall_ok = False
            results["steps"].append(
                {
                    "survey": survey,
                    "ok": False,
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )
            _log(f"FAIL {survey}: {type(exc).__name__}: {exc}")

    results["ok"] = overall_ok
    results["finished_at_utc"] = datetime.now(UTC).isoformat()
    report = WORK / "logs" / run_id / "pinned-match-report.json"
    _write_json(report, results)
    _log(json.dumps({"report": str(report), "ok": overall_ok}, sort_keys=True))
    print(json.dumps(results, indent=2, sort_keys=True))
    return 0 if overall_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
