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
union/fof, HATS inputs, discovery, the registry contract, engine-independent
``skyerr`` pair sets, join-type composition, and RA = 0/360 seam handling.

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


def stream(
    cmd: list[str],
    *,
    cwd: Path | None = None,
    timeout: int = 900,
    env: dict[str, str] | None = None,
) -> int:
    """Run a command with its output left on the session log (no capture)."""
    print("+", " ".join(cmd), flush=True)
    full_env = dict(os.environ)
    if env:
        full_env.update(env)
    return subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        env=full_env,
        timeout=timeout,
        check=False,
    ).returncode


def dump_ray_logs() -> None:
    """Print the tail of the local Ray logs for a wedged ray-union run."""
    logs = Path("/tmp/ray/session_latest/logs")
    if not logs.is_dir():
        print(f"no ray logs at {logs}", flush=True)
        return
    for path in sorted(logs.glob("*"))[-8:]:
        try:
            text = path.read_text(errors="replace")
        except OSError:
            continue
        print(f"--- {path.name}", flush=True)
        print(text[-2500:], flush=True)


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

    # A published collection URL resolves to its tree without downloading it
    # (2MASS is ~41 GB: the point here is resolution, not the transfer).
    code = (
        "from xmatcher.crossmatch import CrossMatch\n"
        "print(CrossMatch().resolve_source('hats:https://data.lsdb.io/hats/two_mass/', {})"
        ".access_identifier)\n"
    )
    proc = pixi_python(code, timeout=300)
    require_ok(proc, "resolve LSDB collection URL")
    resolved = proc.stdout.strip().splitlines()[-1]
    require(
        resolved.rstrip("/").endswith("/two_mass/two_mass"),
        f"LSDB collection resolved to {resolved!r}",
    )

    proc = xmatcher(
        "match",
        f"hats:{resolved}",
        str(right),
        "-r",
        "2.0",
        "-o",
        str(out("remote-hats.parquet")),
        timeout=300,
    )
    require(proc.returncode != 0, "a remote tree matched without being mirrored")
    require(
        "xmatcher sync" in (proc.stdout + proc.stderr),
        "remote HATS error did not name the sync command",
    )
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


def scenario_hats_sources() -> None:
    """Registry name lookup, collection descent, mirroring and a live match.

    The mirror used to fail on every LSDB tree: the collection URL has no
    ``properties`` of its own, and the tree's Apache listing starts with a
    ``/parent/dir/`` row that the walker descended into.  This scenario follows
    the whole path on the smallest published tree (TNS, ~20 MB) so a
    regression shows up as a failed check rather than a support question.
    """
    cache_env = {"XMATCHER_CACHE_ROOT": str(WORK / "hats-cache")}

    def registry_listing() -> dict:
        proc = xmatcher("list", "--hats", env=cache_env, timeout=300)
        require_ok(proc, "list --hats")
        require("two_mass" in proc.stdout, "registry listing lost two_mass")
        require("hf://datasets/" in proc.stdout, "registry listing lost the hf:// entries")
        counts = re.findall(r"(\d+) registered HATS catalogues, (\d+) readable here\.", proc.stdout)
        require(bool(counts), "missing registry count line")
        registered, readable = (int(value) for value in counts[0])
        require(registered > 50, f"registry listing shrank to {registered} catalogues")
        require(
            readable == registered,
            f"{registered - readable} registered catalogues are not readable here",
        )
        return {"registered": registered, "readable": readable}

    def collection_descent() -> dict:
        code = (
            "from xmatcher.crossmatch import CrossMatch\n"
            "print(CrossMatch().resolve_source('hats:gaia_dr3', {}).access_identifier)\n"
        )
        proc = pixi_python(code, timeout=300)
        require_ok(proc, "resolve hats:gaia_dr3")
        url = proc.stdout.strip().splitlines()[-1]
        require(
            url.rstrip("/").endswith("/gaia"),
            f"collection descent picked {url!r}, not the gaia tree",
        )
        require("gaia_10arcs" not in url and "index" not in url, f"picked an auxiliary tree: {url}")
        return {"url": url}

    def dated_collection_is_not_guessed() -> dict:
        proc = xmatcher("sync", "hats:tns", env=cache_env, timeout=300, retries=0)
        require(proc.returncode != 0, "an ambiguous dated collection unexpectedly synced")
        text = proc.stdout + proc.stderr
        require("Traceback" not in text, "ambiguous collection raised a traceback")
        require("pass one tree URL" in text, f"ambiguity was not explained: {text[-400:]}")
        return {"exit": proc.returncode}

    def mirror_and_match() -> dict:
        code = (
            "from xmatcher.mirror import TokenBucket, _http_listing\n"
            "names, ok = _http_listing('https://data.lsdb.io/hats/tns/', TokenBucket(0))\n"
            "dates = sorted({\n"
            "    n.strip('/')\n"
            "    for n in names\n"
            "    if n.strip('/')[:4].isdigit() and not n.strip('/').endswith(('_10arcs', '_5arcs'))\n"
            "})\n"
            "print(dates[-1] if ok and dates else '')\n"
        )
        proc = pixi_python(code, timeout=300)
        require_ok(proc, "list tns dates")
        date = proc.stdout.strip().splitlines()[-1]
        require(bool(date), "no dated TNS tree was listed")
        require("_arcs" not in date, f"picked a margin tree: {date}")

        url = f"hats:https://data.lsdb.io/hats/tns/{date}"
        proc = xmatcher("sync", url, env=cache_env, timeout=900)
        require_ok(proc, "sync live LSDB tree")

        code = (
            "import glob, polars as pl, sys\n"
            "root = glob.glob(sys.argv[1] + '/hats-http-*')[0]\n"
            "pixel = sorted(glob.glob(root + '/dataset/**/*.parquet', recursive=True))[0]\n"
            "frame = pl.read_parquet(pixel)\n"
            "row = frame.row(0, named=True)\n"
            "ra, dec = float(row['ra']), float(row['declination'])\n"
            "print(root)\n"
            "pl.DataFrame({'ra': [ra + 0.5 / 3600], 'dec': [dec], 'id': [1]}).write_parquet(sys.argv[2])\n"
        )
        probe = out(f"tns-{date}-probe.parquet")
        proc = pixi_python(code, str(WORK / "hats-cache" / date), str(probe), timeout=300)
        require_ok(proc, "probe from mirrored tree")
        root = proc.stdout.strip().splitlines()[0]

        result = out(f"tns-{date}-match.parquet")
        proc = xmatcher("match", root, str(probe), "-r", "2.0", "-o", str(result))
        require_ok(proc, "match mirrored LSDB tree")
        rows = parquet_rows(result)
        require(rows == 1, f"live HATS match returned {rows} rows, expected 1")
        require(parquet_max_sep(result) <= 2.0, "live HATS match exceeded its radius")
        return {"date": date, "rows": rows}

    def s3_access() -> dict:
        """An S3-only registry name resolves, descends, and lists anonymously."""
        code = (
            "from xmatcher.crossmatch import CrossMatch\n"
            "from xmatcher.mirror import _properties_dict, _tree_properties_rel\n"
            "from xmatcher.s3 import S3Remote\n"
            "url = CrossMatch().resolve_source('hats:euclid_q1_ipac', {}).access_identifier\n"
            "reader = S3Remote(url)\n"
            "props = _properties_dict(reader.read_bytes(_tree_properties_rel(reader)))\n"
            "index = reader.index()\n"
            "print(url)\n"
            "print(props.get('hats_nrows', ''))\n"
            "print(len(index))\n"
        )
        proc = pixi_python(code, timeout=900)
        require_ok(proc, "resolve and list hats:euclid_q1_ipac")
        lines = proc.stdout.strip().splitlines()[-3:]
        require(len(lines) == 3, f"unexpected S3 probe output: {lines}")
        url, rows, files = lines
        require(url.startswith("s3://"), f"S3 registry name resolved to {url!r}")
        require(
            url.rstrip("/").endswith("euclid_q1_merged_objects-hats"),
            f"S3 collection descent picked {url!r}",
        )
        require(int(rows) > 1_000_000, f"S3 properties reported {rows} rows")
        require(int(files) > 100, f"S3 listing returned only {files} objects")
        return {"url": url, "rows": int(rows), "files": int(files)}

    def hf_access() -> dict:
        """An hf://-only registry name resolves, descends, and lists anonymously."""
        code = (
            "from xmatcher.crossmatch import CrossMatch\n"
            "from xmatcher.mirror import _hats_property_count, _tree_properties_rel\n"
            "from xmatcher.hf import HFRemote\n"
            "url = CrossMatch().resolve_source('hats:mmu_cfa_seccsn', {}).access_identifier\n"
            "reader = HFRemote(url)\n"
            "rows = _hats_property_count(reader.read_bytes(_tree_properties_rel(reader)))\n"
            "index = reader.index()\n"
            "print(url)\n"
            "print(rows if rows is not None else '')\n"
            "print(len(index))\n"
        )
        proc = pixi_python(code, timeout=600)
        require_ok(proc, "resolve and list hats:mmu_cfa_seccsn")
        lines = proc.stdout.strip().splitlines()[-3:]
        require(len(lines) == 3, f"unexpected Hugging Face probe output: {lines}")
        url, rows, files = lines
        require(url.startswith("hf://datasets/"), f"hf:// registry name resolved to {url!r}")
        require(
            url.rstrip("/").endswith("mmu_cfa_seccsn"),
            f"Hugging Face collection descent picked {url!r}",
        )
        require(int(rows) > 0, f"Hugging Face properties reported {rows} rows")
        require(int(files) > 10, f"Hugging Face listing returned only {files} files")
        return {"url": url, "rows": int(rows), "files": int(files)}

    check("hats-registry-list", registry_listing)
    check("hats-collection-descent", collection_descent)
    check("hats-dated-collection-refused", dated_collection_is_not_guessed)
    check("hats-s3-access", s3_access)
    check("hats-hf-access", hf_access)
    check("hats-live-mirror-match", mirror_and_match)


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
            "started = time.time()\n"
            "ray.init(address='local', ignore_reinit_error=True)\n"
            "print('engine-init-seconds', round(time.time() - started, 1), flush=True)\n"
            "print('engine-resources', ray.cluster_resources(), flush=True)\n"
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
    # A sparse field: an 8 arcsec grid where every row has exactly one
    # counterpart 0.5 arcsec away.  A dense field (spacing far below the search
    # radius) legitimately expands into millions of tuples per hub row and is
    # not what this scenario measures; it only measured --max-tuples pressure.
    code = (
        "import sys,polars as pl\n"
        "rows,cols=30,20\n"
        "step=8/3600.0\n"
        "ra=[10.0+(i%cols)*step for i in range(rows*cols)]\n"
        "dec=[5.0+(i//cols)*step for i in range(rows*cols)]\n"
        "pl.DataFrame({'ra':ra,'dec':dec,'id':list(range(len(ra)))}).write_parquet(sys.argv[1])\n"
        "pl.DataFrame({'ra':[r+0.5/3600 for r in ra],'dec':dec,"
        "'id2':list(range(len(ra)))}).write_parquet(sys.argv[2])\n"
    )
    require_ok(pixi_python(code, str(left), str(right)), "ray inputs")

    def union() -> dict:
        path = out("ray-union.hats")
        oracle = out("ray-oracle.parquet")
        # Sequential full-outer union on the same inputs: the distributed
        # engine must reproduce exactly these pairs and this row count.
        require_ok(
            xmatcher(
                "match",
                str(left),
                str(right),
                "-r",
                "4.0",
                "--union",
                "--engine",
                "fast",
                "-o",
                str(oracle),
                timeout=600,
            ),
            "ray oracle",
        )
        # Stream, never capture: Ray workers inherit the pipe descriptors, so
        # a timeout would otherwise wait forever for EOF on orphaned workers.
        try:
            code = stream(
                [
                    "pixi",
                    "run",
                    "xmatcher",
                    "-v",
                    "match",
                    str(left),
                    str(right),
                    "-r",
                    "4.0",
                    "--engine",
                    "ray-union",
                    "-o",
                    str(path),
                ],
                cwd=REPO,
                timeout=900,
                env={"RAY_LOG_TO_STDERR": "1"},
            )
        except subprocess.TimeoutExpired:
            print("ray-union timed out; local ray logs follow", flush=True)
            dump_ray_logs()
            raise
        require(code == 0, f"ray-union exited {code}")
        verify = (
            "import sys\n"
            "from pathlib import Path\n"
            "import polars as pl\n"
            "from xmatcher import hats_native\n"
            "pixels=list(hats_native.list_hats_pixels(Path(sys.argv[1])))\n"
            "assert pixels, 'no HATS partitions written'\n"
            "d=pl.concat([pl.read_parquet(p) for _,_,p in pixels], how='diagonal_relaxed')\n"
            "o=pl.read_parquet(sys.argv[2])\n"
            "assert d.height==o.height, (d.height, o.height)\n"
            "def pairs(df):\n"
            "    return {(a,b) for a,b in zip(df['id'].to_list(), df['id2'].to_list())\n"
            "            if a is not None and b is not None}\n"
            "dp, op = pairs(d), pairs(o)\n"
            "assert dp==op, (len(dp), len(op), len(dp^op))\n"
            "assert len(dp)>0\n"
            "assert float(d['sep_arcsec'].max())<=4.0\n"
            "print(f'rows={d.height} pairs={len(dp)}')\n"
        )
        proc = pixi_python(verify, str(path), str(oracle))
        require_ok(proc, "ray-union output")
        return {"rows": proc.stdout.strip().splitlines()[-1]}

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


def scenario_cds_mix() -> None:
    """Cross-archive pairings that combine CDS and Data Lab tables."""
    for left, right, field, radius in (
        ("galex_ais", "gaia_dr3_cds", "galex", 5.0),
        ("galex_ais", "twomass_psc", "galex", 5.0),
        ("ps1_dr1", "twomass_psc", "cosmos", 1.5),
        ("catwise_2020", "gaia_dr3_cds", "cosmos", 2.0),
        ("vhs_dr5", "des_dr2", "des", 2.0),
    ):
        check(
            f"mix-{left}-{right}",
            lambda a=left, b=right, f=field, r=radius: match_check(
                f"{a}__{b}", a, b, f, radius_arcsec=r
            ),
        )


def scenario_noirlab_mix() -> None:
    """Data Lab pairings that exercise the corrected NSC/unWISE/AllWISE columns."""
    for left, right, field, radius in (
        ("nsc_dr2", "allwise", "nsc", 2.0),
        ("nsc_dr2", "unwise_dr1", "nsc", 2.0),
        ("des_dr2", "allwise", "des", 2.0),
        ("unwise_dr1", "allwise", "cosmos", 2.0),
        ("legacy_dr10", "des_dr2", "des", 2.0),
    ):
        check(
            f"mix-{left}-{right}",
            lambda a=left, b=right, f=field, r=radius: match_check(
                f"{a}__{b}", a, b, f, radius_arcsec=r
            ),
        )


def scenario_skyerr_real() -> None:
    """Per-row N-sigma matching on live catalogues with real uncertainties."""
    for left, right, field, radius in (
        ("gaia_dr3_cds", "ps1_dr1", "cosmos", 1.0),
        ("gaia_dr3_cds", "nsc_dr2", "nsc", 1.0),
        ("nsc_dr2", "allwise", "nsc", 1.5),
        ("des_dr2", "unwise_dr1", "des", 1.5),
    ):
        check(
            f"skyerr-{left}-{right}",
            lambda a=left, b=right, f=field, r=radius: match_check(
                f"skyerr-{a}__{b}",
                a,
                b,
                f,
                radius_arcsec=r,
                extra=["--matcher", "skyerr", "--max-error", "3"],
            ),
        )


def scenario_formats() -> None:
    """The same local catalogue in parquet/CSV/TSV/FITS must match identically."""
    base = out("format-inputs")
    base.mkdir(parents=True, exist_ok=True)
    code = (
        "import sys, polars as pl\n"
        "from pathlib import Path\n"
        "from astropy.table import Table\n"
        "d = Path(sys.argv[1])\n"
        "n = 60\n"
        "frame = pl.DataFrame({'ra': [10.0 + (i % 10) * 0.0002 for i in range(n)],"
        " 'dec': [5.0 + (i // 10) * 0.0002 for i in range(n)], 'id': list(range(n))})\n"
        "frame.write_parquet(d / 'left.parquet')\n"
        "frame.write_csv(d / 'left.csv')\n"
        "frame.write_csv(d / 'left.tsv', separator='\\t')\n"
        "Table.from_pandas(frame.to_pandas()).write(d / 'left.fits', overwrite=True)\n"
        "frame.select(['ra', 'dec']).write_parquet(d / 'right.parquet')\n"
    )
    require_ok(pixi_python(code, str(base)), "format inputs")

    def one(name, left):
        def run() -> dict:
            path = out(f"fmt-{name}.parquet")
            proc = xmatcher(
                "match", str(left), str(base / "right.parquet"), "-r", "1.0", "-o", str(path)
            )
            require_ok(proc, name)
            rows = parquet_rows(path)
            require(rows == 60, f"{name}: expected 60 rows, got {rows}")
            require(parquet_max_sep(path) <= 1.0, f"{name}: separation exceeds radius")
            return {"rows": rows}

        check(name, run)

    for name, filename in (
        ("parquet", "left.parquet"),
        ("csv", "left.csv"),
        ("tsv", "left.tsv"),
        ("fits", "left.fits"),
    ):
        one(f"input-{name}", base / filename)

    def hats_output() -> dict:
        path = out("format-output.hats")
        proc = xmatcher(
            "match",
            str(base / "left.parquet"),
            str(base / "right.parquet"),
            "-r",
            "1.0",
            "-o",
            str(path),
        )
        require_ok(proc, "hats output")
        verify = (
            "import sys, hats\n"
            "ds = hats.read_hats(sys.argv[1])\n"
            "assert len(ds.partition_info.as_dataframe()) > 0\n"
            "print('ok')\n"
        )
        require_ok(pixi_python(verify, str(path)), "read HATS output")
        return {"ok": True}

    check("output-hats", hats_output)


def scenario_multi_3way() -> None:
    """Four-way union/fof at COSMOS and a three-way union at the DES field."""

    def run(keys: list[str], field: str, flags: list[str], name: str) -> dict:
        ra, dec, radius_deg = FIELDS[field]
        path = out(f"{name}.parquet")
        proc = xmatcher(
            "match",
            *keys,
            "--ra",
            str(ra),
            "--dec",
            str(dec),
            "--radius-deg",
            str(radius_deg),
            *flags,
            "-o",
            str(path),
        )
        require_ok(proc, name)
        rows = parquet_rows(path)
        require(rows > 0, f"{name} returned no rows")
        columns = pixi_python(
            "import sys, polars as pl; print(','.join(pl.read_parquet(sys.argv[1]).columns))",
            str(path),
        )
        require_ok(columns, f"{name} columns")
        return {"rows": rows, "has_src_cats": "_src_cats" in columns.stdout}

    check(
        "cosmos-union-4way",
        lambda: run(
            ["gaia_dr3_cds", "ps1_dr1", "twomass_psc", "catwise_2020"],
            "cosmos",
            ["--union"],
            "multi4-union",
        ),
    )
    check(
        "cosmos-fof-4way",
        lambda: run(
            ["gaia_dr3_cds", "ps1_dr1", "twomass_psc", "catwise_2020"],
            "cosmos",
            ["--fof", "-r", "2.0"],
            "multi4-fof",
        ),
    )
    check(
        "des-union-3way",
        lambda: run(["nsc_dr2", "des_dr2", "allwise"], "des", ["--union"], "des3-union"),
    )


def scenario_engines() -> None:
    """fast/zone/astropy must agree on the same live pair set."""
    counts: dict[str, int] = {}

    for engine in ("fast", "zone", "astropy"):

        def run(eng=engine) -> dict:
            path = out(f"engine-{eng}.parquet")
            proc = xmatcher(
                "match",
                "gaia_dr3_cds",
                "ps1_dr1",
                *cone_args("cosmos", 1.0),
                "--engine",
                eng,
                "-o",
                str(path),
            )
            require_ok(proc, f"engine {eng}")
            rows = parquet_rows(path)
            counts[eng] = rows
            require(parquet_max_sep(path) <= 1.0, f"engine {eng}: separation exceeds radius")
            return {"rows": rows}

        check(f"engine-{engine}", run)

    require(len(set(counts.values())) == 1, f"engines disagree: {counts}")


def scenario_skyerr_parity() -> None:
    """`skyerr` pair sets must not depend on the engine.

    PS1 and a few other catalogues carry corrupted per-row errors (hundreds of
    arcsec).  STILTS ranks in error-normalised space, so without capping those
    rows it lets far-away counterparts win ``best`` and then drops them by
    radius, silently losing genuine matches.  Compare every engine on the same
    cone: same row count and the same separation total.
    """
    for left, right, field, radius in (
        ("gaia_dr3_cds", "ps1_dr1", "cosmos", 1.0),
        ("nsc_dr2", "allwise", "nsc", 1.5),
        ("des_dr2", "unwise_dr1", "des", 1.5),
    ):

        def run(a=left, b=right, f=field, r=radius) -> dict:
            rows: dict[str, int] = {}
            seps: dict[str, float] = {}
            for engine in ("fast", "stilts", "astropy"):
                path = out(f"skyerr-{a}-{b}-{engine}.parquet")
                proc = xmatcher(
                    "match",
                    a,
                    b,
                    *cone_args(f, r),
                    "--matcher",
                    "skyerr",
                    "--max-error",
                    "3",
                    "--engine",
                    engine,
                    "-o",
                    str(path),
                )
                require_ok(proc, f"skyerr {engine}")
                rows[engine] = parquet_rows(path)
                seps[engine] = round(parquet_max_sep(path), 3)
                require(rows[engine] > 0, f"skyerr {engine} returned no rows")
                require(
                    seps[engine] <= r + 0.05,
                    f"skyerr {engine} exceeded radius: {seps[engine]}",
                )
            require(
                len(set(rows.values())) == 1 and len(set(seps.values())) == 1,
                f"skyerr engines disagree for {a} x {b}: rows={rows} max_sep={seps}",
            )
            return {"rows": rows["fast"], "max_sep": seps["fast"]}

        check(f"skyerr-parity-{left}-{right}", run)


def scenario_join_mix() -> None:
    """Join types on one live pair must compose consistently."""
    counts: dict[str, int] = {}

    for join in ("1and2", "1or2", "all1", "all2", "1not2", "2not1"):

        def run(kind=join) -> dict:
            path = out(f"join-{kind}.parquet")
            proc = xmatcher(
                "match",
                "gaia_dr3_cds",
                "ps1_dr1",
                *cone_args("cosmos", 1.0),
                "--join",
                kind,
                "-o",
                str(path),
            )
            require_ok(proc, f"join {kind}")
            counts[kind] = parquet_rows(path)
            return {"rows": counts[kind]}

        check(f"join-{join}", run)

    inner = counts["1and2"]
    require(inner > 0, "inner join returned no rows")
    require(
        counts["1or2"] == inner + counts["1not2"] + counts["2not1"],
        f"full outer != inner + anti joins: {counts}",
    )
    require(counts["all1"] == inner + counts["1not2"], f"left join mismatch: {counts}")
    require(counts["all2"] == inner + counts["2not1"], f"right join mismatch: {counts}")
    require(
        counts["1or2"] >= counts["all1"] >= inner and counts["1or2"] >= counts["all2"] >= inner,
        f"join nesting violated: {counts}",
    )

    def find_all() -> dict:
        path = out("find-all.parquet")
        proc = xmatcher(
            "match",
            "gaia_dr3_cds",
            "ps1_dr1",
            *cone_args("cosmos", 1.0, find="all"),
            "-o",
            str(path),
        )
        require_ok(proc, "find all")
        rows = parquet_rows(path)
        require(rows >= inner, f"find=all returned {rows} rows, best returned {inner}")
        return {"rows": rows}

    check("find-all-not-smaller", find_all)


def scenario_wrap_mix() -> None:
    """RA = 0/360 seam handling, locally and on a live all-sky pair."""

    def local_seam() -> dict:
        left = out("seam-left.parquet")
        right = out("seam-right.parquet")
        code = (
            "import sys,polars as pl\n"
            "seam=[359.99950,0.00050,359.99995]\n"
            "pl.DataFrame({'ra':seam,'dec':[5.0,5.0,-5.0],'id':[0,1,2]}).write_parquet(sys.argv[1])\n"
            "pl.DataFrame({'ra':[seam[0]+0.3/3600,seam[1]-0.3/3600,seam[2]+0.3/3600],"
            "'dec':[5.0,5.0,-5.0],'id2':[0,1,2]}).write_parquet(sys.argv[2])\n"
        )
        require_ok(pixi_python(code, str(left), str(right)), "seam inputs")
        path = out("seam-match.parquet")
        proc = xmatcher("match", str(left), str(right), "-r", "1.0", "-o", str(path))
        require_ok(proc, "seam match")
        rows = parquet_rows(path)
        require(rows == 3, f"seam match paired {rows}/3 rows")
        require(parquet_max_sep(path) <= 1.0, "seam match exceeded radius")
        return {"rows": rows}

    def live_zero_and_360() -> dict:
        counts = {}
        for ra in ("0.0", "360.0"):
            path = out(f"wrap-{ra}.parquet")
            proc = xmatcher(
                "match",
                "gaia_dr3_cds",
                "allwise",
                "--ra",
                ra,
                "--dec",
                "0.0",
                "--radius-deg",
                "0.05",
                "-r",
                "1.5",
                "-o",
                str(path),
            )
            require_ok(proc, f"wrap {ra}")
            counts[ra] = parquet_rows(path)
            require(parquet_max_sep(path) <= 1.5, f"wrap {ra} exceeded radius")
        require(counts["0.0"] > 0, "wrap cone returned no rows")
        require(
            counts["0.0"] == counts["360.0"],
            f"RA 0 and 360 disagree: {counts}",
        )
        return {"rows": counts["0.0"]}

    check("wrap-local-seam", local_seam)
    check("wrap-live-0-360", live_zero_and_360)


def scenario_precomputed() -> None:
    """Match NOIRLab's pre-computed pair tables against a live catalogue."""
    for table, field in (("nsc_x_gaia", "nsc"), ("des_x_gaia", "des")):

        def run(name=table, f=field) -> dict:
            ra, dec, radius_deg = FIELDS[f]
            path = out(f"{name}.parquet")
            proc = xmatcher(
                "match",
                name,
                "gaia_dr3_cds",
                "--ra",
                str(ra),
                "--dec",
                str(dec),
                "--radius-deg",
                str(radius_deg),
                "-r",
                "2.0",
                "-o",
                str(path),
            )
            require_ok(proc, name)
            rows = parquet_rows(path)
            require(rows > 0, f"{name} returned no rows")
            columns = pixi_python(
                "import sys, polars as pl; print(','.join(pl.read_parquet(sys.argv[1]).columns))",
                str(path),
            )
            require_ok(columns, f"{name} columns")
            require("distance" in columns.stdout, f"{name} lost the distance column")
            return {"rows": rows}

        check(f"precomputed-{table}", run)


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
    "hats-sources": scenario_hats_sources,
    "ray": scenario_ray,
    "ray-probe": scenario_ray_probe,
    "cds-mix": scenario_cds_mix,
    "noirlab-mix": scenario_noirlab_mix,
    "skyerr-real": scenario_skyerr_real,
    "formats": scenario_formats,
    "multi-3way": scenario_multi_3way,
    "engines": scenario_engines,
    "skyerr-parity": scenario_skyerr_parity,
    "join-mix": scenario_join_mix,
    "wrap-mix": scenario_wrap_mix,
    "precomputed": scenario_precomputed,
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
