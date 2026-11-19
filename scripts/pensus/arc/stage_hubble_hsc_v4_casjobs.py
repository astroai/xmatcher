#!/usr/bin/env python3
"""Stage MAST Hubble Source Catalog v4 (unique-object summary) via CasJobs.

No public HATS exists for HSCv4 (LSDB/TAP/Catalogs.MAST are Subaru or older HSC).
MAST documents CasJobs-only access for v4. Requires env CASJOBS_USERID + CASJOBS_PW
(mastcasjobs). Never embed credentials in this file.

Day-0 pin: unique Match summary (SumMagAper2Cat) → later hats/hubble/hsc_v4.
Shards by MatchRA strips; resume via *.done.json markers. CephFS TMP only for deps.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import pathlib
import sys
import time

EXPECTED_ROWS = 139_162_343  # MAST HSCv4 FAQ unique matches
CONTEXT = "HSCv4"
# Live CasJobs schema (probed 2026-11-19): SumMagAper2Cat is magnitudes-only
# (MatchID + A_*/W2_*/W3_*); MatchRA/MatchDec/CI/Num* live on SumPropMagAper2Cat.
PROP_TABLE = "SumPropMagAper2Cat"
MAG_TABLE = "SumMagAper2Cat"
# Core identity + quality + a practical broadband subset for Day-0 SED anchors.
# Full filter zoo can be widened later; CasJobs MyDB quotas forbid dumping every column at once.
PROP_COLS = "p.MatchID, p.MatchRA, p.MatchDec, p.CI, p.NumImages, p.NumVisits, p.NumFilters"
MAG_COLS = (
    "m.A_F435W, m.A_F475W, m.A_F555W, m.A_F606W, m.A_F775W, m.A_F814W, "
    "m.W3_F275W, m.W3_F336W, m.W3_F438W, m.W3_F555W, m.W3_F606W, m.W3_F814W, "
    "m.W3_F110W, m.W3_F125W, m.W3_F160W, "
    "m.W2_F555W, m.W2_F606W, m.W2_F814W"
)
COLS = f"{PROP_COLS}, {MAG_COLS}"
# Dense HST footprints make 5° JOIN dumps hit CasJobs MyDB/resource limits
# (seen: ra[5,10) → 4.5M rows / 565 MB OK; ra[10,15) → status 4 failed).
MIN_RA_WIDTH_DEG = 0.25


def require_casjobs_env() -> tuple[str, str]:
    user = os.environ.get("CASJOBS_USERID", "").strip()
    pw = os.environ.get("CASJOBS_PW", "").strip()
    if not user or not pw:
        raise SystemExit(
            "BLOCKED: CasJobs credentials missing. Set env CASJOBS_USERID and "
            "CASJOBS_PW (MAST CasJobs account; independent of SDSS). "
            "Do not embed passwords in scripts or git. See "
            "https://mastweb.stsci.edu/hcasjobs/ and mastcasjobs docs."
        )
    return user, pw


def write_pointer(pointer: pathlib.Path, payload: dict) -> None:
    pointer.parent.mkdir(parents=True, exist_ok=True)
    pointer.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def casjobs_error_text(jobs, jobid) -> str:
    """Best-effort CasJobs error string for a failed job (never secrets)."""
    for attr in ("job_info", "status", "get_job", "jobStatus"):
        fn = getattr(jobs, attr, None)
        if not callable(fn):
            continue
        try:
            info = fn(jobid)
        except Exception as exc:  # noqa: BLE001
            print(f"WARN {attr}({jobid}): {type(exc).__name__}: {exc}", flush=True)
            continue
        if info is None:
            continue
        if isinstance(info, dict):
            err = info.get("Error") or info.get("error") or info.get("Message")
            if err:
                return str(err)[:500]
            return str({k: info[k] for k in list(info)[:12]})[:500]
        for field in ("Error", "error", "Message", "message"):
            if hasattr(info, field) and getattr(info, field):
                return str(getattr(info, field))[:500]
        return str(info)[:500]
    return ""


def scrub_pensus_mydb(jobs) -> None:
    """Drop leftover pensus_* MyDB tables so quota/resource limits clear."""
    try:
        tables = jobs.list_tables()
    except Exception as exc:  # noqa: BLE001
        print(f"WARN list_tables: {type(exc).__name__}: {exc}", flush=True)
        return
    names: list[str] = []
    if isinstance(tables, (list, tuple)):
        for t in tables:
            if isinstance(t, str):
                names.append(t)
            elif isinstance(t, dict):
                names.append(str(t.get("Name") or t.get("name") or t.get("TableName") or ""))
            else:
                names.append(str(getattr(t, "Name", getattr(t, "name", t))))
    for name in names:
        if not name.lower().startswith("pensus_"):
            continue
        try:
            jobs.drop_table_if_exists(name)
            print(f"dropped MyDB.{name}", flush=True)
        except Exception as exc:  # noqa: BLE001
            print(f"WARN drop MyDB.{name}: {exc}", flush=True)


def write_shard_parquet(shards: pathlib.Path, name: str, table, extra: dict) -> tuple[int, str]:
    import pyarrow as pa
    import pyarrow.parquet as pq

    # Convert astropy table → parquet
    arrays = []
    names = []
    for col in table.colnames:
        names.append(col)
        arrays.append(pa.array(table[col].tolist()))
    arrow = pa.table(dict(zip(names, arrays, strict=True)))
    out = shards / f"{name}.parquet"
    pq.write_table(arrow, out, compression="zstd")
    nrows = len(table)
    marker = shards / f"{name}.done.json"
    marker.write_text(
        json.dumps(
            {
                **extra,
                "rows": nrows,
                "path": str(out),
                "format": "parquet",
                "finished_at_utc": dt.datetime.now(dt.UTC).isoformat(),
            },
            indent=2,
            sort_keys=True,
        )
        + "\n"
    )
    return nrows, str(out)


def stage_strip(jobs, shards: pathlib.Path, ra0: float, ra1: float) -> int:
    """Dump one MatchRA strip; bisect on CasJobs failure (MyDB/resource limits)."""
    name = f"hscv4_ra_{ra0:06.2f}_{ra1:06.2f}".replace(".", "p")
    marker = shards / f"{name}.done.json"
    if marker.is_file():
        prev = json.loads(marker.read_text())
        print(f"skip {name} rows={prev.get('rows')}", flush=True)
        return int(prev.get("rows", 0))

    width = ra1 - ra0
    mydb = f"pensus_{name}"[:100]
    # Join props (coords/quality) to mags; filter on prop MatchRA.
    query = f"""
SELECT {COLS}
INTO MyDB.{mydb}
FROM {PROP_TABLE} AS p
INNER JOIN {MAG_TABLE} AS m ON p.MatchID = m.MatchID
WHERE p.MatchRA >= {ra0} AND p.MatchRA < {ra1}
"""
    print(f"submit {name} ra=[{ra0},{ra1}) ...", flush=True)
    try:
        jobs.drop_table_if_exists(mydb)
    except Exception as exc:  # noqa: BLE001
        print(f"WARN pre-drop {mydb}: {exc}", flush=True)
    t0 = time.time()
    jobid = jobs.submit(query, task_name=f"pensus-{name}", context=CONTEXT)
    print(f"casjobs_jobid={jobid}", flush=True)
    results = jobs.monitor(jobid)
    print(f"monitor {name} {results} elapsed={time.time() - t0:.1f}s", flush=True)
    # mastcasjobs: status 4 = failed, 5 = finished (typical CasJobs codes)
    status = results[0] if isinstance(results, tuple | list) and results else results
    if status != 5:
        err = casjobs_error_text(jobs, jobid)
        if err:
            print(f"casjobs_error {name}: {err}", flush=True)
        try:
            jobs.drop_table_if_exists(mydb)
        except Exception as exc:  # noqa: BLE001
            print(f"WARN drop after fail {mydb}: {exc}", flush=True)
        if width > MIN_RA_WIDTH_DEG + 1e-9:
            mid = 0.5 * (ra0 + ra1)
            print(
                f"SPLIT {name} width={width:.3f}° → [{ra0},{mid}) + [{mid},{ra1}) "
                f"(CasJobs status={status}; likely MyDB/resource limit)",
                flush=True,
            )
            return stage_strip(jobs, shards, ra0, mid) + stage_strip(jobs, shards, mid, ra1)
        raise RuntimeError(
            f"CasJobs job failed for {name}: monitor={results} error={err!r}. "
            f"Query used JOIN {PROP_TABLE}/{MAG_TABLE} on MatchID; "
            f"strip width {width:.4f}° already at min."
        )
    tab = jobs.fast_table(mydb, verbose=True)
    nrows, path = write_shard_parquet(
        shards,
        name,
        tab,
        {
            "ra0": ra0,
            "ra1": ra1,
            "prop_table": PROP_TABLE,
            "mag_table": MAG_TABLE,
            "context": CONTEXT,
            "source": "MAST CasJobs HSCv4 SumPropMagAper2Cat⋈SumMagAper2Cat",
            "mydb_table": mydb,
        },
    )
    try:
        jobs.drop_table_if_exists(mydb)
    except Exception as exc:  # noqa: BLE001
        print(f"WARN drop MyDB.{mydb}: {exc}", flush=True)
    print(f"wrote {name} rows={nrows} path={path}", flush=True)
    return nrows


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--stage", required=True)
    ap.add_argument("--pointer", required=True)
    ap.add_argument("--planned-hats", default="/arc/projects/hats/hubble/hsc_v4")
    # Default 2° (was 5°): denser footprints exceeded CasJobs MyDB limits.
    ap.add_argument("--ra-step", type=float, default=2.0)
    ap.add_argument("--ra-start", type=float, default=0.0)
    ap.add_argument("--ra-stop", type=float, default=360.0)
    ap.add_argument(
        "--prep-only",
        action="store_true",
        help="Create dirs + SOURCE.json; do not query CasJobs",
    )
    args = ap.parse_args()

    stage = pathlib.Path(args.stage)
    shards = stage / "shards"
    shards.mkdir(parents=True, exist_ok=True)
    tmp = stage / "tmp"
    tmp.mkdir(parents=True, exist_ok=True)
    pointer = pathlib.Path(args.pointer)

    base = {
        "alias": "hubble_hsc_v4_hats",
        "survey": "hubble",
        "release": "hsc_v4",
        "source": "MAST CasJobs HSCv4 SumPropMagAper2Cat⋈SumMagAper2Cat",
        "note": (
            "No public HATS; TAP/Catalogs.MAST still pre-v4. "
            "Distinct from Subaru hats/hsc/pdr3. "
            "Requires CASJOBS_USERID + CASJOBS_PW. "
            "Coords/quality from SumPropMagAper2Cat; mags from SumMagAper2Cat."
        ),
        "stage_root": str(stage),
        "planned_hats_path": args.planned_hats,
        "exists": True,
        "hats_ready": False,
        "official_mirror": False,
        "shard_mode": "ra_strip",
        "ra_step_deg": args.ra_step,
        "expected_rows": EXPECTED_ROWS,
        "prop_table": PROP_TABLE,
        "mag_table": MAG_TABLE,
        "context": CONTEXT,
        "columns": COLS,
        "probed_at_utc": dt.datetime.now(dt.UTC).isoformat(),
    }

    if args.prep_only:
        base.update(status="prep_ready_awaiting_casjobs_credentials", nrows_staged=0, strips=0)
        write_pointer(pointer, base)
        print(f"prep-only pointer={pointer}", flush=True)
        # Soft check: do not fail prep if creds absent
        if not (os.environ.get("CASJOBS_USERID") and os.environ.get("CASJOBS_PW")):
            print(
                "NOTE: CASJOBS_USERID/CASJOBS_PW not set — stage dump not started",
                flush=True,
            )
            return
        print("NOTE: credentials present but --prep-only; skipping dump", flush=True)
        return

    require_casjobs_env()
    try:
        import mastcasjobs
    except ImportError as exc:
        raise SystemExit(
            "FAIL: mastcasjobs not installed. pip install mastcasjobs into CephFS pydeps."
        ) from exc

    jobs = mastcasjobs.MastCasJobs(context="MyDB")
    scrub_pensus_mydb(jobs)
    total = 0
    strips = 0
    ra0 = args.ra_start
    while ra0 < args.ra_stop - 1e-9:
        ra1 = min(ra0 + args.ra_step, args.ra_stop)
        total += stage_strip(jobs, shards, ra0, ra1)
        strips += 1
        ra0 = ra1

    base.update(
        {
            "status": "staged",
            "strips": strips,
            "nrows_staged": total,
            "probed_at_utc": dt.datetime.now(dt.UTC).isoformat(),
        }
    )
    write_pointer(pointer, base)
    print(
        f"done strips={strips} rows={total} expected={EXPECTED_ROWS} pointer={pointer}",
        flush=True,
    )
    if abs(total - EXPECTED_ROWS) > max(50_000, EXPECTED_ROWS // 200):
        print(
            f"WARN row count {total} vs expected {EXPECTED_ROWS} "
            "(column subset / null MatchRA edges possible)",
            flush=True,
        )


if __name__ == "__main__":
    try:
        main()
    except SystemExit as e:
        # Ensure non-zero exits propagate for canfar Failed status when blocked.
        code = e.code if isinstance(e.code, int) else (1 if e.code else 0)
        if e.args and isinstance(e.args[0], str) and e.args[0].startswith("BLOCKED"):
            print(e.args[0], file=sys.stderr, flush=True)
            raise SystemExit(2) from None
        raise SystemExit(code) from None
