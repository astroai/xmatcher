#!/usr/bin/env python3
"""Stage SDSS DR16 PhotoPrimary via SciServer Keystone + CasJobs REST.

Pin target: /arc/projects/hats/sdss/dr16 (survey/release — not 'sweeps').
Auth: SKYSERVER_USERID + SKYSERVER_PW. Never embed secrets.
Prefers CDS HATS when reachable (--cds-check).
No SciServer PyPI package (not published); uses urllib against Keystone/CasJobs.
"""

from __future__ import annotations

import argparse
import datetime as dt
import io
import json
import os
import pathlib
import time
import urllib.error
import urllib.request

EXPECTED_PRIMARY = 469_050_976  # VizieR mode=1 scale
CONTEXT = "DR16"
COLS = (
    "objID, ra, dec, mode, type, clean, "
    "u, g, r, i, z, err_u, err_g, err_r, err_i, err_z, "
    "run, camcol, field, rerun"
)
MIN_RA_WIDTH = 0.05

AUTH_URL = "https://apps.sciserver.org/login-portal/keystone/v3/tokens"
CASJOBS = "https://skyserver.sdss.org/CasJobs/RestApi"
UA = "pensus-sdss-dr16/1.1"


def require_env() -> None:
    if not (os.environ.get("SKYSERVER_USERID") and os.environ.get("SKYSERVER_PW")):
        raise SystemExit(
            "BLOCKED: SKYSERVER_USERID/SKYSERVER_PW missing "
            "(eval export lines from ~/.bashrc past interactive guard)"
        )


def write_pointer(path: pathlib.Path, doc: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(doc, indent=2, sort_keys=True) + "\n")


def cds_up() -> bool:
    url = "https://vizcat.cds.unistra.fr/hats/sdss_dr16/main/properties"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": UA})
        with urllib.request.urlopen(req, timeout=30) as r:
            text = r.read(2000).decode("utf-8", "replace")
        return "hats_nrows" in text or "obs_collection" in text
    except Exception as exc:  # noqa: BLE001
        print(f"CDS_DOWN ({type(exc).__name__}): {exc}", flush=True)
        return False


class CasJobsREST:
    """Minimal Keystone login + CasJobs RestApi (no SciServer package)."""

    def __init__(self) -> None:
        user = os.environ["SKYSERVER_USERID"].strip()
        pw = os.environ["SKYSERVER_PW"]
        print(
            f"SKYSERVER_USERID_set=yes SKYSERVER_USERID_len={len(user)}",
            flush=True,
        )
        print(f"SKYSERVER_PW_set=yes SKYSERVER_PW_len={len(pw)}", flush=True)
        self.token = self._login(user, pw)
        print("SKYSERVER_AUTH_OK=yes", flush=True)
        self.context = CONTEXT

    @staticmethod
    def _login(user: str, pw: str) -> str:
        body = json.dumps(
            {
                "auth": {
                    "identity": {
                        "password": {"user": {"name": user, "password": pw}}
                    }
                }
            }
        ).encode()
        req = urllib.request.Request(
            AUTH_URL + "?TaskName=pensus.sdss.login",
            data=body,
            method="POST",
            headers={"Content-Type": "application/json", "User-Agent": UA},
        )
        with urllib.request.urlopen(req, timeout=120) as resp:
            token = resp.headers.get("X-Subject-Token") or ""
        if not token:
            raise RuntimeError("Keystone login returned empty X-Subject-Token")
        return token

    def _headers(self, accept: str = "application/json") -> dict[str, str]:
        return {
            "Content-Type": "application/json",
            "Accept": accept,
            "X-Auth-Token": self.token,
            "User-Agent": UA,
        }

    def execute(self, sql: str, context: str, accept: str = "application/json+array") -> bytes:
        url = f"{CASJOBS}/contexts/{context}/query?TaskName=pensus.sdss.execute"
        payload = json.dumps({"Query": sql, "TaskName": "pensus.sdss.execute"}).encode()
        req = urllib.request.Request(
            url, data=payload, method="POST", headers=self._headers(accept)
        )
        with urllib.request.urlopen(req, timeout=1800) as resp:
            return resp.read()

    def drop_table_if_exists(self, table: str) -> None:
        # CasJobs MyDB: unprefixed name only (MyDB.foo → "too many prefixes").
        sql = (
            f"IF OBJECT_ID('{table}','U') IS NOT NULL DROP TABLE {table}"
        )
        try:
            self.execute(sql, context="MyDB")
            print(f"dropped MyDB.{table}", flush=True)
        except Exception as exc:  # noqa: BLE001
            print(f"WARN drop {table}: {exc}", flush=True)

    def list_tables(self) -> list[dict]:
        url = f"{CASJOBS}/contexts/MyDB/Tables?TaskName=pensus.sdss.tables"
        req = urllib.request.Request(url, method="GET", headers=self._headers())
        with urllib.request.urlopen(req, timeout=60) as resp:
            return json.loads(resp.read().decode())

    def cleanup_pensus_tables(self) -> None:
        try:
            tables = self.list_tables()
        except Exception as exc:  # noqa: BLE001
            print(f"WARN list MyDB tables: {exc}", flush=True)
            return
        for meta in tables:
            name = str(meta.get("Name") or "")
            if name.startswith("pensus_"):
                print(
                    f"cleanup MyDB.{name} rows={meta.get('Rows')} size={meta.get('Size')}",
                    flush=True,
                )
                self.drop_table_if_exists(name)

    def submit(self, query: str, task_name: str) -> int:
        url = (
            f"{CASJOBS}/contexts/{self.context}/jobs"
            f"?TaskName=pensus.sdss.submit"
        )
        payload = json.dumps(
            {"Query": query, "TaskName": f"pensus-{task_name}"}
        ).encode()
        headers = self._headers(accept="text/plain")
        req = urllib.request.Request(url, data=payload, method="PUT", headers=headers)
        with urllib.request.urlopen(req, timeout=120) as resp:
            jobid = int(resp.read().decode().strip())
        print(f"casjobs_jobid={jobid} task={task_name}", flush=True)
        return jobid

    def monitor(self, jobid: int, timeout_s: int = 7200) -> tuple[int, str]:
        # Status: 0 ready, 1 started, 2 canceling, 3 cancelled, 4 failed, 5 finished
        t0 = time.time()
        url = f"{CASJOBS}/jobs/{jobid}?TaskName=pensus.sdss.status"
        while time.time() - t0 < timeout_s:
            req = urllib.request.Request(url, method="GET", headers=self._headers())
            with urllib.request.urlopen(req, timeout=60) as resp:
                meta = json.loads(resp.read().decode())
            st = int(meta.get("Status", -1))
            if st == 5:
                return 5, "finished"
            if st in (3, 4):
                msg = meta.get("Message") or meta.get("Error") or ""
                label = "failed" if st == 4 else "cancelled"
                return st, f"{label}:{msg}"
            time.sleep(20)
        return -1, "timeout"

    def fetch_table_parquet(self, table: str, out: pathlib.Path) -> int:
        import pandas as pd
        import pyarrow as pa
        import pyarrow.parquet as pq

        sql = f"SELECT * FROM MyDB.{table}"
        # Prefer CSV Accept for large MyDB dumps
        try:
            raw = self.execute(sql, context="MyDB", accept="text/plain")
            df = pd.read_csv(io.BytesIO(raw))
        except Exception as exc:  # noqa: BLE001
            print(f"WARN csv fetch: {exc}; trying json+array", flush=True)
            raw = self.execute(sql, context="MyDB", accept="application/json+array")
            payload = json.loads(raw.decode())
            result = payload["Result"][0]
            df = pd.DataFrame(result["Data"], columns=result["Columns"])
        pq.write_table(pa.Table.from_pandas(df), out, compression="zstd")
        return len(df)


def stage_strip(jobs: CasJobsREST, shards: pathlib.Path, ra0: float, ra1: float) -> int:
    name = f"sdss_pp_ra_{ra0:06.2f}_{ra1:06.2f}".replace(".", "p")
    marker = shards / f"{name}.done.json"
    if marker.is_file():
        prev = json.loads(marker.read_text())
        print(f"skip {name} rows={prev.get('rows')}", flush=True)
        return int(prev.get("rows", 0))

    width = ra1 - ra0
    mydb = f"pensus_{name}"[:100]
    query = f"""
SELECT {COLS}
INTO MyDB.{mydb}
FROM PhotoPrimary
WHERE ra >= {ra0} AND ra < {ra1}
"""
    print(f"submit {name} ra=[{ra0},{ra1})", flush=True)
    jobs.drop_table_if_exists(mydb)
    t0 = time.time()
    jobid = jobs.submit(query, task_name=name)
    st, label = jobs.monitor(jobid)
    print(
        f"monitor {name} ({st}, '{label}') elapsed={time.time() - t0:.1f}s",
        flush=True,
    )
    if st != 5:
        jobs.drop_table_if_exists(mydb)
        if width > MIN_RA_WIDTH + 1e-9:
            mid = 0.5 * (ra0 + ra1)
            print(f"SPLIT {name} → [{ra0},{mid}) + [{mid},{ra1})", flush=True)
            return stage_strip(jobs, shards, ra0, mid) + stage_strip(
                jobs, shards, mid, ra1
            )
        raise RuntimeError(f"CasJobs failed for {name}: status={st}")

    out = shards / f"{name}.parquet"
    try:
        nrows = jobs.fetch_table_parquet(mydb, out)
    except Exception as exc:  # noqa: BLE001
        jobs.drop_table_if_exists(mydb)
        if width > MIN_RA_WIDTH + 1e-9:
            mid = 0.5 * (ra0 + ra1)
            print(
                f"SPLIT after download fail {name}: {exc} → [{ra0},{mid}) + [{mid},{ra1})",
                flush=True,
            )
            return stage_strip(jobs, shards, ra0, mid) + stage_strip(
                jobs, shards, mid, ra1
            )
        raise
    marker.write_text(
        json.dumps(
            {
                "ra0": ra0,
                "ra1": ra1,
                "rows": nrows,
                "path": str(out),
                "table": "PhotoPrimary",
                "context": CONTEXT,
                "finished_at_utc": dt.datetime.now(dt.UTC).isoformat(),
            },
            indent=2,
            sort_keys=True,
        )
        + "\n"
    )
    jobs.drop_table_if_exists(mydb)
    print(f"wrote {name} rows={nrows}", flush=True)
    return nrows


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--stage", required=True)
    ap.add_argument("--pointer", required=True)
    ap.add_argument("--planned-hats", default="/arc/projects/hats/sdss/dr16")
    ap.add_argument("--ra-step", type=float, default=2.0)
    ap.add_argument("--ra-start", type=float, default=0.0)
    ap.add_argument("--ra-stop", type=float, default=360.0)
    ap.add_argument("--prep-only", action="store_true")
    ap.add_argument("--cds-check", action="store_true", default=True)
    ap.add_argument("--no-cds-check", action="store_false", dest="cds_check")
    args = ap.parse_args()

    if args.cds_check and cds_up():
        print(
            "CDS_UP: prefer sync_then_promote_sdss_dr16.sh — aborting SkyServer rebuild",
            flush=True,
        )
        raise SystemExit(0)

    stage = pathlib.Path(args.stage)
    shards = stage / "shards"
    shards.mkdir(parents=True, exist_ok=True)
    pointer = pathlib.Path(args.pointer)
    base = {
        "alias": "sdss_dr16_hats",
        "survey": "sdss",
        "release": "dr16",
        "source": "SciServer/CasJobs PhotoPrimary (CDS HATS unavailable)",
        "planned_hats_path": args.planned_hats,
        "stage_root": str(stage),
        "expected_rows_primary": EXPECTED_PRIMARY,
        "shard_mode": "ra_strip",
        "ra_step_deg": args.ra_step,
        "exists": True,
        "hats_ready": False,
        "official_mirror": False,
        "probed_at_utc": dt.datetime.now(dt.UTC).isoformat(),
    }
    if args.prep_only:
        base["status"] = "prep_ready"
        write_pointer(pointer, base)
        print("prep-only", pointer, flush=True)
        return

    require_env()
    jobs = CasJobsREST()
    # Free MyDB quota from prior failed/partial pensus_* tables before staging.
    jobs.cleanup_pensus_tables()
    # Keep successfully downloaded shard markers; only drop leftover MyDB tables.
    total = 0
    strips = 0
    ra0 = args.ra_start
    while ra0 < args.ra_stop - 1e-9:
        ra1 = min(ra0 + args.ra_step, args.ra_stop)
        total += stage_strip(jobs, shards, ra0, ra1)
        strips += 1
        ra0 = ra1
    base.update(status="staged", strips=strips, nrows_staged=total)
    write_pointer(pointer, base)
    print(f"done strips={strips} rows={total}", flush=True)


if __name__ == "__main__":
    try:
        main()
    except SystemExit as e:
        code = e.code if isinstance(e.code, int) else (1 if e.code else 0)
        raise SystemExit(code) from None
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", "replace")[:500]
        raise SystemExit(f"HTTPError {e.code}: {body}") from e
