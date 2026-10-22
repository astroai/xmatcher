#!/usr/bin/env python3
"""Run one xmatcher test scenario inside a CANFAR headless session.

CANFAR splits ``create ... -- CMD ARGS`` on whitespace and does not honour
shell quoting, so the session command has to be a single token.  The embeddable
bootstrap in ``scripts/canfar_bootstrap.py`` satisfies that: it copies a
source tarball from VOSpace (the repository is private) into ``/scratch`` and
hands off to this file:

    python3 -c exec(__import__('base64').b64decode('<bootstrap>').decode()) <scenario>

Scenarios use real TAP services with small explicit cones, so they stay
bounded while exercising alias resolution, explicit identifiers, matchers,
union/fof, HATS inputs, discovery, and the registry contract.

Results are written to ``/arc/projects/hats/tests/xmatcher-matrix/<run>/``
and a ``SUMMARY {...}`` line is printed for ``canfar logs`` collection.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

REF = os.environ.get("XMATCHER_REF", "main")
RUN_ID = os.environ.get("XMATCHER_RUN_ID") or time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
WORK = Path(os.environ.get("XMATCHER_WORK", f"/scratch/xmatcher-{RUN_ID}"))
REPO = Path(os.environ.get("XMATCHER_REPO", str(WORK / "xmatcher")))

# The base image ships a pixi config pointing at the read-only
# /usr/local/share/pixi; sessions run as an unprivileged user.
os.environ["PIXI_HOME"] = str(WORK / "pixi-home")
os.environ["PIXI_CACHE_DIR"] = str(WORK / "pixi-cache")
os.environ["UV_CACHE_DIR"] = str(WORK / "uv-cache")
REPO_URL = "https://github.com/sfabbro/xmatcher.git"
ARC_ROOT = Path("/arc/projects/hats/tests/xmatcher-matrix") / RUN_ID

COSMOS = (150.10, 2.20, 0.05)
GALEX = (150.10, 2.20, 0.20)
DES = (36.10, -5.00, 0.05)
NSC = (10.00, -30.00, 0.05)
FIELDS = {"cosmos": COSMOS, "galex": GALEX, "des": DES, "nsc": NSC}

RESULTS: list[dict] = []
SCENARIO = sys.argv[1] if len(sys.argv) > 1 else "help"


# --------------------------------------------------------------------------- #
# process helpers
# --------------------------------------------------------------------------- #
def sh(
    cmd: list[str],
    *,
    cwd: Path | None = None,
    timeout: int = 900,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    full_env = dict(os.environ)
    full_env.setdefault("NO_COLOR", "1")
    if env:
        full_env.update(env)
    return subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        capture_output=True,
        text=True,
        timeout=timeout,
        env=full_env,
    )


TRANSIENT_ERRORS = (
    "connection reset",
    "connection aborted",
    "timed out",
    "temporarily unavailable",
    "http 502",
    "http 503",
    "http 504",
)


def xmatcher(
    *args: str,
    timeout: int = 900,
    env: dict[str, str] | None = None,
    retries: int = 2,
):
    """Run the CLI, retrying only transient TAP/network failures."""
    for attempt in range(retries + 1):
        proc = sh(["pixi", "run", "xmatcher", *args], cwd=REPO, timeout=timeout, env=env)
        if proc.returncode == 0:
            return proc
        text = ((proc.stdout or "") + (proc.stderr or "")).lower()
        if attempt >= retries or not any(marker in text for marker in TRANSIENT_ERRORS):
            return proc
        print(f"retry {attempt + 1}/{retries} after transient error", flush=True)
        time.sleep(5 * (attempt + 1))
    return proc


def pixi_python(code: str, *args: str, timeout: int = 600) -> subprocess.CompletedProcess[str]:
    return sh(["pixi", "run", "python", "-c", code, *args], cwd=REPO, timeout=timeout)


def stream(cmd: list[str], *, cwd: Path | None = None, timeout: int = 900) -> int:
    """Run a command with its output left on the session log (no capture)."""
    print("+", " ".join(cmd), flush=True)
    return subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        env=dict(os.environ),
        timeout=timeout,
        check=False,
    ).returncode


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def require_ok(proc: subprocess.CompletedProcess[str], label: str) -> None:
    text = (proc.stdout or "") + (proc.stderr or "")
    require(proc.returncode == 0, f"{label} exited {proc.returncode}: {text[-1200:]}")
    require("Traceback" not in text, f"{label} raised a traceback: {text[-1200:]}")


def check(name: str, fn) -> None:
    started = time.time()
    try:
        detail = fn() or ""
        ok, error = True, ""
    except Exception as exc:  # noqa: BLE001 - report every scenario failure
        ok, detail, error = False, "", f"{type(exc).__name__}: {exc}"
    elapsed = round(time.time() - started, 1)
    RESULTS.append(
        {"name": name, "ok": ok, "seconds": elapsed, "detail": str(detail), "error": error}
    )
    print(
        f"{'PASS' if ok else 'FAIL'} {name} ({elapsed}s){' ' + error if error else ''}", flush=True
    )


def out(name: str) -> Path:
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", name)
    path = WORK / "out" / SCENARIO / safe
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


# --------------------------------------------------------------------------- #
# data helpers
# --------------------------------------------------------------------------- #
def parquet_rows(path: Path) -> int:
    code = "import sys,polars as pl; print(pl.read_parquet(sys.argv[1]).height)"
    proc = pixi_python(code, str(path))
    require_ok(proc, f"read {path.name}")
    return int(proc.stdout.strip().splitlines()[-1])


def parquet_max_sep(path: Path) -> float:
    code = (
        "import sys,polars as pl\n"
        "d=pl.read_parquet(sys.argv[1])\n"
        "cols=[c for c in d.columns if 'sep' in c.lower()]\n"
        "assert cols, list(d.columns)\n"
        "print(float(d[cols[0]].max()))\n"
    )
    proc = pixi_python(code, str(path))
    require_ok(proc, f"separation in {path.name}")
    return float(proc.stdout.strip().splitlines()[-1])


def write_targets(path: Path, ra0: float, dec0: float, n: int = 40, step: float = 0.0005) -> None:
    code = (
        "import sys,polars as pl\n"
        "ra0,dec0,n,step,path=float(sys.argv[1]),float(sys.argv[2]),int(sys.argv[3]),float(sys.argv[4]),sys.argv[5]\n"
        "pl.DataFrame({'ra':[ra0+(i%8)*step for i in range(n)],"
        "'dec':[dec0+(i//8)*step for i in range(n)],"
        "'ra_err':[0.3]*n,'dec_err':[0.3]*n,'id':list(range(n))}).write_parquet(path)\n"
    )
    proc = pixi_python(code, str(ra0), str(dec0), str(n), str(step), str(path))
    require_ok(proc, "write targets")


def cone_args(field: str, radius_arcsec: float, *, find: str = "best") -> list[str]:
    ra, dec, radius_deg = FIELDS[field]
    return [
        "--ra",
        str(ra),
        "--dec",
        str(dec),
        "--radius-deg",
        str(radius_deg),
        "-r",
        str(radius_arcsec),
        "--find",
        find,
    ]


def match_check(
    name: str,
    left: str,
    right: str,
    field: str,
    *,
    radius_arcsec: float = 1.0,
    extra: list[str] | None = None,
    min_rows: int = 1,
) -> dict:
    path = out(f"{name}.parquet")
    args = ["match", left, right, *cone_args(field, radius_arcsec), "-o", str(path), *(extra or [])]
    proc = xmatcher(*args)
    require_ok(proc, name)
    rows = parquet_rows(path)
    require(rows >= min_rows, f"{name}: expected >= {min_rows} rows, got {rows}")
    max_sep = parquet_max_sep(path)
    require(max_sep <= radius_arcsec + 0.05, f"{name}: max separation {max_sep} > {radius_arcsec}")
    return {"rows": rows, "max_sep_arcsec": max_sep}


def alias_equivalence(alias: str, canonical: str, field: str, radius_arcsec: float = 1.0) -> dict:
    """Alias and canonical name must produce the same self-match result."""
    first = out(f"{alias}__{canonical}.parquet")
    second = out(f"{canonical}__{canonical}.parquet")
    p1 = xmatcher("match", alias, canonical, *cone_args(field, radius_arcsec), "-o", str(first))
    require_ok(p1, f"{alias} vs {canonical}")
    p2 = xmatcher(
        "match", canonical, canonical, *cone_args(field, radius_arcsec), "-o", str(second)
    )
    require_ok(p2, f"{canonical} self-match")
    n1, n2 = parquet_rows(first), parquet_rows(second)
    require(n1 > 0, f"{alias}: self-match returned no rows")
    require(n1 == n2, f"{alias}: {n1} rows vs canonical {n2}")
    require(parquet_max_sep(first) <= radius_arcsec + 0.05, f"{alias}: separation exceeds radius")
    return {"rows": n1}


# --------------------------------------------------------------------------- #
# scenarios
# --------------------------------------------------------------------------- #
def scenario_registry() -> None:
    code = r"""
import json
from xmatcher import CrossMatch

cm = CrossMatch()
cats = cm.catalogues_config
aliases = dict(cm.aliases_config)
assert len(cats) >= 40, len(cats)
assert len(aliases) >= 60, len(aliases)

for name, cfg in cats.items():
    for key in ("archive", "service_id", "access_identifier", "release", "ra_column", "dec_column"):
        assert cfg.get(key), f"{name} missing {key}"
    assert any(k for k in cfg if "err" in k) or cfg.get("default_pos_error_arcsec"), name

for alias in aliases:
    resolved = cm.resolve_name(alias)
    assert resolved in cats, f"{alias} -> {resolved}"

former = {
    "gaia_cds": "gaia_dr3_cds",
    "galex": "galex_ais",
    "vhs": "vhs_dr5",
    "ps1": "ps1_dr1",
    "twomass": "twomass_psc",
    "catwise": "catwise_2020",
    "des": "des_dr2",
    "nsc": "nsc_dr2",
    "smash": "smash_dr2",
    "unwise": "unwise_dr1",
    "desils": "legacy_dr10",
    "gaia_noao": "gaia_noirlab",
    "ukidss": "ukidss_las",
    "erosita": "erosita_erass1",
    "xmm": "xmm_dr13",
}
for old, new in former.items():
    assert cm.resolve_name(old) == new, (old, cm.resolve_name(old))

legacy = cm.resolve_source("decals_objects", {})
assert legacy.access_identifier == "ls_dr10.object", legacy.access_identifier
assert cm.resolve_source("decals", {}).access_identifier == "ls_dr10.tractor"
print(json.dumps({"catalogues": len(cats), "aliases": len(aliases)}))
"""
    proc = pixi_python(code, timeout=300)
    require_ok(proc, "registry contract")
    json.loads(proc.stdout.strip().splitlines()[-1])

    for key in ("gaia_cds", "desils", "gaia_noao", "decals_objects", "twomass", "ps1"):
        proc = xmatcher("describe", key)
        require_ok(proc, f"describe {key}")
        require("Catalogue:" in proc.stdout, f"describe {key} printed no catalogue")

    proc = xmatcher("list", "--aliases")
    require_ok(proc, "list --aliases")
    require("gaia_cds" in proc.stdout and "legacy_dr10" in proc.stdout, "alias table incomplete")

    for shell in ("bash", "zsh", "fish"):
        proc = xmatcher("completion", shell)
        require_ok(proc, f"completion {shell}")
        require("xmatcher" in proc.stdout, f"completion {shell} empty")

    proc = xmatcher("doctor")
    require_ok(proc, "doctor")


def scenario_aliases_cds() -> None:
    for alias, canonical, field, radius in (
        ("gaia", "gaia_dr3_cds", "cosmos", 1.0),
        ("ps1", "ps1_dr1", "cosmos", 1.0),
        ("twomass", "twomass_psc", "cosmos", 2.0),
        ("catwise", "catwise_2020", "cosmos", 2.0),
        ("galex", "galex_ais", "galex", 5.0),
    ):
        check(
            f"alias-{alias}",
            lambda a=alias, c=canonical, f=field, r=radius: alias_equivalence(a, c, f, r),
        )


def scenario_aliases_noirlab() -> None:
    for alias, canonical, field, radius in (
        ("des", "des_dr2", "des", 2.0),
        ("desils", "legacy_dr10", "des", 2.0),
        ("nsc", "nsc_dr2", "nsc", 2.0),
        ("unwise", "unwise_dr1", "cosmos", 2.0),
        ("allwise_dl", "allwise", "cosmos", 2.0),
    ):
        check(
            f"alias-{alias}",
            lambda a=alias, c=canonical, f=field, r=radius: alias_equivalence(a, c, f, r),
        )


def _explicit_pairs(pairs: list[tuple[str, str, str]]) -> None:
    for explicit, configured, field in pairs:
        check(
            f"explicit-{configured}",
            lambda e=explicit, c=configured, f=field: alias_equivalence(e, c, f, 1.5),
        )


def scenario_explicit_vizier() -> None:
    _explicit_pairs(
        [
            ("vizier:II/349/ps1", "ps1_dr1", "cosmos"),
            ("vizier:I/355/gaiadr3", "gaia_dr3_cds", "cosmos"),
            ("vizier:II/246/out", "twomass_psc", "cosmos"),
        ]
    )


def scenario_explicit_tap() -> None:
    _explicit_pairs(
        [
            ("tap:noirlab/nsc_dr2.object", "nsc_dr2", "nsc"),
            ("tap:noirlab/des_dr2.main", "des_dr2", "des"),
            ("tap:gaia/gaiadr3.gaia_source", "gaia_esa", "cosmos"),
        ]
    )


def scenario_pairwise_optical() -> None:
    for left, right, field, radius in (
        ("ps1_dr1", "gaia_dr3_cds", "cosmos", 1.0),
        ("legacy_dr10", "ps1_dr1", "cosmos", 1.0),
        ("gaia_dr3_cds", "twomass_psc", "cosmos", 1.5),
    ):
        check(
            f"pair-{left}-{right}",
            lambda a=left, b=right, f=field, r=radius: match_check(
                f"{a}__{b}", a, b, f, radius_arcsec=r
            ),
        )


def scenario_pairwise_ir() -> None:
    for left, right, field, radius in (
        ("des_dr2", "unwise_dr1", "des", 2.0),
        ("nsc_dr2", "allwise", "nsc", 2.0),
        ("twomass_psc", "catwise_2020", "cosmos", 2.0),
        ("legacy_dr10", "unwise_dr1", "des", 2.0),
    ):
        check(
            f"pair-{left}-{right}",
            lambda a=left, b=right, f=field, r=radius: match_check(
                f"{a}__{b}", a, b, f, radius_arcsec=r
            ),
        )


def scenario_multi_union_fof() -> None:
    ra, dec, radius_deg = COSMOS
    common = ["--ra", str(ra), "--dec", str(dec), "--radius-deg", str(radius_deg)]

    def multi() -> dict:
        path = out("multi.parquet")
        proc = xmatcher(
            "match", "gaia_dr3_cds", "ps1_dr1", "twomass_psc", *common, "-r", "1.5", "-o", str(path)
        )
        require_ok(proc, "multi")
        rows = parquet_rows(path)
        require(rows > 0, "multi returned no rows")
        return {"rows": rows}

    def union() -> dict:
        path = out("union.parquet")
        proc = xmatcher(
            "match",
            "gaia_dr3_cds",
            "ps1_dr1",
            "twomass_psc",
            *common,
            "--union",
            "-o",
            str(path),
        )
        require_ok(proc, "union")
        code = "import sys,polars as pl; d=pl.read_parquet(sys.argv[1]); print(d.height)"
        proc_rows = pixi_python(code, str(path))
        require_ok(proc_rows, "union rows")
        rows = int(proc_rows.stdout.strip().splitlines()[-1])
        require(rows > 0, "union returned no rows")
        cols = pixi_python(
            "import sys,polars as pl; print(','.join(pl.read_parquet(sys.argv[1]).columns))",
            str(path),
        )
        require_ok(cols, "union columns")
        require("_src_cats" in cols.stdout, f"union lost _src_cats: {cols.stdout[-300:]}")
        return {"rows": rows}

    def fof() -> dict:
        path = out("fof.parquet")
        proc = xmatcher(
            "match",
            "gaia_dr3_cds",
            "ps1_dr1",
            "twomass_psc",
            *common,
            "--fof",
            "-r",
            "2.0",
            "-o",
            str(path),
        )
        require_ok(proc, "fof")
        rows = parquet_rows(path)
        require(rows > 0, "fof returned no rows")
        return {"rows": rows}

    check("multi", multi)
    check("union", union)
    check("fof", fof)


def scenario_matchers() -> None:
    def skyerr() -> dict:
        return match_check(
            "skyerr",
            "gaia_dr3_cds",
            "ps1_dr1",
            "cosmos",
            radius_arcsec=1.0,
            extra=["--matcher", "skyerr", "--max-error", "3"],
        )

    check("skyerr", skyerr)

    targets = out("targets.parquet")
    write_targets(targets, *COSMOS[:2])

    def fallback_warn() -> dict:
        path = out("fallback-warn.parquet")
        proc = xmatcher(
            "match",
            str(targets),
            "ps1_dr1",
            *cone_args("cosmos", 2.0),
            "--matcher",
            "skyerr",
            "--fallback-policy",
            "warn",
            "-o",
            str(path),
        )
        require_ok(proc, "fallback warn")
        text = (proc.stdout + proc.stderr).lower()
        require("falls back" in text or "fallback" in text, "warn produced no fallback note")
        return {"rows": parquet_rows(path)}

    def fallback_error() -> dict:
        proc = xmatcher(
            "match",
            str(targets),
            "ps1_dr1",
            *cone_args("cosmos", 2.0),
            "--matcher",
            "skyerr",
            "--fallback-policy",
            "error",
            "-o",
            str(out("fallback-error.parquet")),
        )
        text = proc.stdout + proc.stderr
        require(proc.returncode != 0, "fallback-policy error exited 0")
        require("Traceback" not in text, text[-1200:])
        require("skyerr" in text.lower() or "positional" in text.lower(), text[-1200:])
        return {"exit": proc.returncode}

    def side_overrides() -> dict:
        path = out("side-overrides.parquet")
        proc = xmatcher(
            "match",
            str(targets),
            "ps1_dr1",
            *cone_args("cosmos", 2.0),
            "--matcher",
            "skyerr",
            "--side1",
            "ra_column=ra,dec_column=dec,ra_err_column=ra_err,dec_err_column=dec_err,pos_err_units=arcsec",
            "--side2",
            "default_pos_error_arcsec=0.2",
            "--fallback-policy",
            "warn",
            "-o",
            str(path),
        )
        require_ok(proc, "side overrides")
        return {"rows": parquet_rows(path)}

    check("fallback-warn", fallback_warn)
    check("fallback-error", fallback_error)
    check("side-overrides", side_overrides)


def scenario_discovery() -> None:
    home = WORK / "home"
    home.mkdir(parents=True, exist_ok=True)
    env = {"HOME": str(home)}

    proc = xmatcher("search", "unWISE", timeout=600)
    require_ok(proc, "search")
    require(len(proc.stdout.strip()) > 0, "search returned nothing")

    proc = xmatcher("discover", "noirlab", timeout=600)
    require_ok(proc, "discover")
    require(len(proc.stdout.strip()) > 0, "discover returned nothing")

    proc = xmatcher("discover", "noirlab", "--schema", "nsc_dr2.object", timeout=600)
    require_ok(proc, "discover --schema")
    require("ra" in proc.stdout.lower(), "schema output lacks RA")

    proc = xmatcher(
        "adopt", "vizier", "II/349/ps1", "--name", "xm_adopt", "--alias", "xm_adopt_alias", env=env
    )
    require_ok(proc, "adopt")

    proc = xmatcher("describe", "xm_adopt_alias", env=env)
    require_ok(proc, "describe adopted alias")
    require("Catalogue: xm_adopt" in proc.stdout, proc.stdout[-500:])

    proc = xmatcher("doctor", env=env)
    require(proc.returncode in (0, 1), f"doctor exited {proc.returncode}")
    require("Traceback" not in (proc.stdout + proc.stderr), "doctor traceback")


def scenario_hats() -> None:
    left = out("left.parquet")
    right = out("right.parquet")
    code = (
        "import sys,polars as pl\n"
        "a,b=sys.argv[1],sys.argv[2]\n"
        "pl.DataFrame({'ra':[10.0+i*0.0001 for i in range(200)],'dec':[5.0]*200,'id':list(range(200))}).write_parquet(a)\n"
        "pl.DataFrame({'ra':[10.0+i*0.0001 for i in range(200)],'dec':[5.0]*200,'id2':list(range(200))}).write_parquet(b)\n"
    )
    require_ok(pixi_python(code, str(left), str(right)), "hats inputs")

    hats_out = out("tiny.hats")
    proc = xmatcher("match", str(left), str(right), "-r", "2.0", "-o", str(hats_out))
    require_ok(proc, "build local HATS")

    proc = xmatcher(
        "match", f"hats:{hats_out}", str(right), "-r", "2.0", "-o", str(out("local-hats.parquet"))
    )
    require_ok(proc, "local hats: input")
    require(parquet_rows(out("local-hats.parquet")) > 0, "local hats returned no rows")

    proc = xmatcher(
        "match",
        "hats:https://data.lsdb.io/hats/two_mass/",
        str(right),
        "-r",
        "2.0",
        "-o",
        str(out("remote-hats.parquet")),
        timeout=300,
    )
    require(proc.returncode != 0, "remote LSDB HATS unexpectedly succeeded")
    require("Traceback" not in (proc.stdout + proc.stderr), "remote HATS raised a traceback")

    proc = xmatcher(
        "match",
        "hats:not_a_registry_name",
        str(right),
        "-r",
        "2.0",
        "-o",
        str(out("bad-hats.parquet")),
        timeout=120,
    )
    require(proc.returncode != 0, "unknown hats name unexpectedly succeeded")
    require("Traceback" not in (proc.stdout + proc.stderr), "unknown hats raised a traceback")


def scenario_ray_probe() -> None:
    """Diagnose a Ray startup that does not finish inside a headless session."""

    def probe() -> dict:
        code = (
            "import os, shutil, time\n"
            "print('dev-shm', shutil.disk_usage('/dev/shm'), flush=True)\n"
            "print('cpus', os.cpu_count(), flush=True)\n"
            "print('ray-env', {k: v for k, v in os.environ.items() if 'RAY' in k}, flush=True)\n"
            "import ray\n"
            "started = time.time()\n"
            "ray.init(num_cpus=1, include_dashboard=False, log_to_driver=True)\n"
            "print('init-seconds', round(time.time() - started, 1), flush=True)\n"
            "print('resources', ray.cluster_resources(), flush=True)\n"
            "ray.shutdown()\n"
        )
        code_path = WORK / "out" / SCENARIO / "ray_probe.py"
        code_path.parent.mkdir(parents=True, exist_ok=True)
        code_path.write_text(code)
        return {"exit": stream(["pixi", "run", "python", "-u", str(code_path)], timeout=300)}

    check("ray-probe", probe)


def scenario_ray() -> None:
    left = out("ray-left.parquet")
    right = out("ray-right.parquet")
    code = (
        "import sys,polars as pl\n"
        "n=20000\n"
        "pl.DataFrame({'ra':[10.0+(i%200)*0.00001 for i in range(n)],"
        "'dec':[5.0+(i//200)*0.00001 for i in range(n)],'id':list(range(n))}).write_parquet(sys.argv[1])\n"
        "pl.DataFrame({'ra':[10.0+(i%200)*0.00001 for i in range(n)],"
        "'dec':[5.0+(i//200)*0.00001 for i in range(n)],'id2':list(range(n))}).write_parquet(sys.argv[2])\n"
    )
    require_ok(pixi_python(code, str(left), str(right)), "ray inputs")

    def union() -> dict:
        path = out("ray-union.hats")
        proc = xmatcher(
            "match",
            str(left),
            str(right),
            "-r",
            "1.0",
            "--engine",
            "ray-union",
            "-o",
            str(path),
            timeout=600,
        )
        require_ok(proc, "ray-union")
        verify = (
            "import sys\n"
            "import hats\n"
            "from xmatcher import hats_native\n"
            "ds=hats.read_hats(sys.argv[1])\n"
            "assert len(ds.partition_info.as_dataframe())>0\n"
            "assert bool([p for _,_,p in hats_native.list_hats_pixels(sys.argv[1])])\n"
            "print('ok')\n"
        )
        require_ok(pixi_python(verify, str(path)), "ray-union output")
        return {"ok": True}

    def spill() -> dict:
        path = out("spill.parquet")
        proc = xmatcher(
            "match",
            str(left),
            str(right),
            "-r",
            "1.0",
            "--memory-budget-bytes",
            "1000000",
            "--scratch-dir",
            str(WORK / "scratch"),
            "-o",
            str(path),
            timeout=1200,
        )
        require_ok(proc, "spill")
        rows = parquet_rows(path)
        require(rows > 0, "spill returned no rows")
        return {"rows": rows}

    check("ray-union", union)
    check("spill", spill)


def scenario_report() -> None:
    root = ARC_ROOT
    if not root.is_dir():
        print(f"no results at {root}")
        return
    for path in sorted(root.glob("*/summary.json")):
        data = json.loads(path.read_text())
        print(
            f"{data.get('scenario')}: {data.get('passed')} passed, "
            f"{data.get('failed')} failed, commit {data.get('commit', '?')[:10]}"
        )
        for entry in data.get("results", []):
            if not entry.get("ok"):
                print(f"  FAIL {entry.get('name')}: {entry.get('error')}")


SCENARIOS = {
    "registry": scenario_registry,
    "aliases-cds": scenario_aliases_cds,
    "aliases-noirlab": scenario_aliases_noirlab,
    "explicit-vizier": scenario_explicit_vizier,
    "explicit-tap": scenario_explicit_tap,
    "pairwise-optical": scenario_pairwise_optical,
    "pairwise-ir": scenario_pairwise_ir,
    "multi-union-fof": scenario_multi_union_fof,
    "matchers": scenario_matchers,
    "discovery": scenario_discovery,
    "hats": scenario_hats,
    "ray": scenario_ray,
    "ray-probe": scenario_ray_probe,
}


def prepare() -> str:
    WORK.mkdir(parents=True, exist_ok=True)
    if not (REPO / ".git").is_dir() and not (REPO / "src" / "xmatcher").is_dir():
        require_ok(sh(["git", "clone", "--depth", "1", REPO_URL, str(REPO)], timeout=900), "clone")
    if REF != "main" and (REPO / ".git").is_dir():
        fetched = sh(["git", "-C", str(REPO), "fetch", "--depth", "1", "origin", REF], timeout=900)
        require_ok(fetched, f"fetch {REF}")
        require_ok(
            sh(["git", "-C", str(REPO), "checkout", "--detach", "FETCH_HEAD"], timeout=120),
            "checkout",
        )
    commit = os.environ.get("XMATCHER_COMMIT")
    if not commit:
        rev = sh(["git", "-C", str(REPO), "rev-parse", "HEAD"], timeout=60)
        require_ok(rev, "rev-parse")
        commit = rev.stdout.strip()
    require_ok(
        sh(["pixi", "install", "--color", "never"], cwd=REPO, timeout=1800),
        "pixi install",
    )
    return commit


def publish() -> None:
    try:
        target = ARC_ROOT / SCENARIO
        target.mkdir(parents=True, exist_ok=True)
        for path in (WORK / "out" / SCENARIO).glob("*.parquet"):
            if path.stat().st_size <= 2_000_000:
                shutil.copy2(path, target / path.name)
        for path in (WORK / "results").glob("*"):
            if path.name.startswith(SCENARIO):
                shutil.copy2(path, target / path.name)
    except OSError as exc:
        print(f"note: could not publish results to {ARC_ROOT}: {exc}", flush=True)


def main() -> int:
    if SCENARIO == "help":
        print("scenarios: " + ", ".join(sorted(SCENARIOS)))
        return 0
    if SCENARIO == "report":
        scenario_report()
        return 0
    if SCENARIO not in SCENARIOS:
        print(f"unknown scenario {SCENARIO!r}; known: {', '.join(sorted(SCENARIOS))}")
        return 2

    started = time.time()
    (WORK / "results").mkdir(parents=True, exist_ok=True)
    commit = prepare()
    print(f"scenario={SCENARIO} commit={commit} run={RUN_ID}", flush=True)

    try:
        SCENARIOS[SCENARIO]()
        if not RESULTS:
            RESULTS.append(
                {"name": SCENARIO, "ok": True, "seconds": 0, "detail": "completed", "error": ""}
            )
    except Exception as exc:  # noqa: BLE001 - surface setup failures in the summary
        RESULTS.append(
            {"name": "scenario", "ok": False, "seconds": 0, "detail": "", "error": str(exc)}
        )
        print(f"FAIL scenario setup ({type(exc).__name__}: {exc})", flush=True)

    passed = sum(1 for entry in RESULTS if entry["ok"])
    failed = len(RESULTS) - passed
    summary = {
        "scenario": SCENARIO,
        "run": RUN_ID,
        "commit": commit,
        "seconds": round(time.time() - started, 1),
        "passed": passed,
        "failed": failed,
        "results": RESULTS,
    }
    (WORK / "results" / f"{SCENARIO}.json").write_text(json.dumps(summary, indent=2))
    publish()
    print("SUMMARY " + json.dumps(summary), flush=True)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
