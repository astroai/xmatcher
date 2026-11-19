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
TABLE = "SumMagAper2Cat"
# Core identity + quality + a practical broadband subset for Day-0 SED anchors.
# Full filter zoo can be widened later; CasJobs MyDB quotas forbid dumping every column at once.
COLS = (
    "MatchID, MatchRA, MatchDec, CI, NumImages, NumVisits, NumFilters, "
    "A_F435W, A_F475W, A_F555W, A_F606W, A_F775W, A_F814W, "
    "W3_F275W, W3_F336W, W3_F438W, W3_F555W, W3_F606W, W3_F814W, "
    "W3_F110W, W3_F125W, W3_F160W, "
    "W2_F555W, W2_F606W, W2_F814W"
)


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
    name = f"hscv4_ra_{ra0:06.2f}_{ra1:06.2f}".replace(".", "p")
    marker = shards / f"{name}.done.json"
    if marker.is_file():
        prev = json.loads(marker.read_text())
        print(f"skip {name} rows={prev.get('rows')}", flush=True)
        return int(prev.get("rows", 0))

    mydb = f"pensus_{name}"[:100]
    query = f"""
SELECT {COLS}
INTO MyDB.{mydb}
FROM {TABLE}
WHERE MatchRA >= {ra0} AND MatchRA < {ra1}
"""
    print(f"submit {name} ra=[{ra0},{ra1}) ...", flush=True)
    jobs.drop_table_if_exists(mydb)
    t0 = time.time()
    jobid = jobs.submit(query, task_name=f"pensus-{name}", context=CONTEXT)
    print(f"casjobs_jobid={jobid}", flush=True)
    results = jobs.monitor(jobid)
    print(f"monitor {name} {results} elapsed={time.time() - t0:.1f}s", flush=True)
    tab = jobs.fast_table(mydb, verbose=True)
    nrows, path = write_shard_parquet(
        shards,
        name,
        tab,
        {
            "ra0": ra0,
            "ra1": ra1,
            "table": TABLE,
            "context": CONTEXT,
            "source": "MAST CasJobs HSCv4 SumMagAper2Cat",
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
    ap.add_argument("--ra-step", type=float, default=5.0)
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
        "source": "MAST CasJobs HSCv4 SumMagAper2Cat (unique matches)",
        "note": (
            "No public HATS; TAP/Catalogs.MAST still pre-v4. "
            "Distinct from Subaru hats/hsc/pdr3. "
            "Requires CASJOBS_USERID + CASJOBS_PW."
        ),
        "stage_root": str(stage),
        "planned_hats_path": args.planned_hats,
        "exists": True,
        "hats_ready": False,
        "official_mirror": False,
        "shard_mode": "ra_strip",
        "ra_step_deg": args.ra_step,
        "expected_rows": EXPECTED_ROWS,
        "table": TABLE,
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
