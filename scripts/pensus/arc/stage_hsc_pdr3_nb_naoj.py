#!/usr/bin/env python3
"""Stage HSC-SSP PDR3 narrow-band forced photometry via NAOJ catalog_jobs API.

Bands: NB0387/0816/0921/1010 → DB short names n387/n816/n921/n1010 on
pdr3_dud_rev.summary (D/UD; not on LSDB).
Auth: HSC_USERID + HSC_PW. Never embed secrets.
Planned hats: /arc/projects/hats/hsc/pdr3/forced_nb{0387,0816,0921,1010}
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import pathlib
import sys
import time
import urllib.error
import urllib.request

API = "https://hsc-release.mtk.nao.ac.jp/datasearch/api/catalog_jobs/"
RELEASE = "pdr3"
# Official pdr3 hscReleaseQuery clientVersion (omit → "CAS client is too old")
CLIENT_VERSION = 20190514.1
SCHEMA = "pdr3_dud_rev"
TABLE = "summary"
# (hats band suffix, DB column prefix)
BANDS = (
    ("nb0387", "n387"),
    ("nb0816", "n816"),
    ("nb0921", "n921"),
    ("nb1010", "n1010"),
)
FIELDS = ("cosmos", "deep2-3", "elais-n1", "xmm-lss")


def require_env() -> tuple[str, str]:
    user = os.environ.get("HSC_USERID", "").strip()
    pw = os.environ.get("HSC_PW", "").strip()
    if not user or not pw:
        raise SystemExit("BLOCKED: HSC_USERID/HSC_PW missing")
    return user, pw


def http_json(url: str, payload: dict, timeout: int = 120) -> dict:
    body = dict(payload)
    body.setdefault("clientVersion", CLIENT_VERSION)
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        url,
        data=data,
        method="POST",
        headers={"Content-Type": "application/json", "User-Agent": "pensus-hsc-nb/1.2"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", "replace")[:800]
        raise RuntimeError(f"HTTP {exc.code}: {detail}") from exc


def submit_job(cred: dict, sql: str, out_format: str = "csv.gz") -> dict:
    return http_json(
        API + "submit",
        {
            "credential": cred,
            "catalog_job": {
                "sql": sql,
                "out_format": out_format,
                "include_metainfo_to_body": False,
                "release_version": RELEASE,
            },
            "nomail": True,
            "skip_syntax_check": False,
        },
    )


def job_status(cred: dict, job_id: str) -> dict:
    return http_json(API + "status", {"credential": cred, "id": job_id})


def download_job(cred: dict, job_id: str, dest: pathlib.Path) -> None:
    payload = json.dumps(
        {
            "credential": cred,
            "id": job_id,
            "clientVersion": CLIENT_VERSION,
        }
    ).encode()
    req = urllib.request.Request(
        API + "download",
        data=payload,
        method="POST",
        headers={"Content-Type": "application/json", "User-Agent": "pensus-hsc-nb/1.2"},
    )
    with urllib.request.urlopen(req, timeout=3600) as resp, open(dest, "wb") as out:
        while True:
            chunk = resp.read(1024 * 1024)
            if not chunk:
                break
            out.write(chunk)


def delete_job(cred: dict, job_id: str) -> None:
    try:
        http_json(API + "delete", {"credential": cred, "id": job_id})
    except Exception as exc:  # noqa: BLE001
        print(f"WARN delete job {job_id}: {exc}", flush=True)


def wait_job(cred: dict, job_id: str, timeout_s: int = 7200) -> dict:
    t0 = time.time()
    while time.time() - t0 < timeout_s:
        try:
            st = job_status(cred, job_id)
        except Exception as exc:  # noqa: BLE001
            # Transient NAOJ drops (connection refused / 5xx) — keep polling.
            print(f"WARN status {job_id}: {exc}", flush=True)
            time.sleep(30)
            continue
        status = (st.get("status") or st.get("job", {}).get("status") or "").lower()
        print(f"job {job_id} status={status}", flush=True)
        if status in ("done", "success", "finished", "ok"):
            return st
        if status in ("error", "failed", "aborted"):
            raise RuntimeError(f"NAOJ job failed: {st}")
        time.sleep(30)
    raise TimeoutError(f"NAOJ job {job_id} timeout")


def sql_for_band_field(db_band: str, field: str) -> str:
    # PDR3 summary short names: n387/n816/n921/n1010 (not nb0387_*).
    peak = f"merge_peak_{db_band}"
    return f"""
SELECT
  object_id,
  ra,
  dec,
  field,
  {peak} AS merge_peak,
  {db_band}_cmodel_flux AS flux_cmodel,
  {db_band}_cmodel_mag AS mag_cmodel,
  {db_band}_psfflux_flux AS flux_psf,
  {db_band}_psfflux_mag AS mag_psf,
  {db_band}_apertureflux_20_flux AS flux_aper20
FROM {SCHEMA}.{TABLE}
WHERE field = '{field}'
  AND ({peak} OR {db_band}_cmodel_flux IS NOT NULL)
"""


def stage_one(
    cred: dict, stage: pathlib.Path, hats_band: str, db_band: str, field: str
) -> pathlib.Path | None:
    safe_field = field.replace("+", "p").replace("-", "_")
    name = f"hsc_nb_{hats_band}_{safe_field}"
    dest = stage / "shards" / f"{name}.csv.gz"
    marker = stage / "shards" / f"{name}.done.json"
    if marker.is_file() and dest.is_file():
        print(f"skip {name}", flush=True)
        return dest
    sql = sql_for_band_field(db_band, field)
    print(f"submit {name}", flush=True)
    try:
        job = submit_job(cred, sql)
    except Exception as exc:  # noqa: BLE001
        print(f"WARN submit {name}: {exc}", flush=True)
        marker.write_text(
            json.dumps(
                {
                    "band": hats_band,
                    "db_band": db_band,
                    "field": field,
                    "skipped": True,
                    "error": str(exc),
                },
                indent=2,
            )
            + "\n"
        )
        return None
    job_id = str(job.get("id") or job.get("job", {}).get("id"))
    print(f"naoj_job_id={job_id}", flush=True)
    wait_job(cred, job_id)
    dest.parent.mkdir(parents=True, exist_ok=True)
    download_job(cred, job_id, dest)
    delete_job(cred, job_id)
    marker.write_text(
        json.dumps(
            {
                "band": hats_band,
                "db_band": db_band,
                "field": field,
                "path": str(dest),
                "bytes": dest.stat().st_size,
                "finished_at_utc": dt.datetime.now(dt.UTC).isoformat(),
            },
            indent=2,
            sort_keys=True,
        )
        + "\n"
    )
    print(f"wrote {dest} bytes={dest.stat().st_size}", flush=True)
    return dest


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--stage", required=True)
    ap.add_argument("--pointer-root", required=True, help="inputs dir for per-band SOURCE.json")
    ap.add_argument("--prep-only", action="store_true")
    args = ap.parse_args()
    stage = pathlib.Path(args.stage)
    stage.mkdir(parents=True, exist_ok=True)
    (stage / "shards").mkdir(parents=True, exist_ok=True)
    pointer_root = pathlib.Path(args.pointer_root)
    pointer_root.mkdir(parents=True, exist_ok=True)

    plan = {
        "survey": "hsc",
        "release": "pdr3",
        "bands": [b[0] for b in BANDS],
        "db_bands": [b[1] for b in BANDS],
        "fields": list(FIELDS),
        "schema": SCHEMA,
        "table": TABLE,
        "client_version": CLIENT_VERSION,
        "planned_hats_paths": {
            b: f"/arc/projects/hats/hsc/pdr3/forced_{b}" for b, _ in BANDS
        },
        "stage_root": str(stage),
        "note": "Narrow bands via NAOJ catalog_jobs on summary; broad GRIZY from LSDB separate",
        "probed_at_utc": dt.datetime.now(dt.UTC).isoformat(),
    }
    (stage / "STAGE.json").write_text(json.dumps(plan, indent=2, sort_keys=True) + "\n")
    if args.prep_only:
        print("prep-only", stage, flush=True)
        return

    user, pw = require_env()
    print(f"HSC_USERID_set=yes HSC_USERID_len={len(user)}", flush=True)
    print(f"HSC_PW_set=yes HSC_PW_len={len(pw)}", flush=True)
    cred = {"account_name": user, "password": pw}

    # summary already primary-selected (no isprimary column on this table).
    smoke = submit_job(
        cred,
        f"SELECT object_id, ra, dec, n387_cmodel_flux FROM {SCHEMA}.{TABLE} "
        f"WHERE field='cosmos' LIMIT 3",
        out_format="csv",
    )
    smoke_id = str(smoke.get("id") or smoke.get("job", {}).get("id"))
    print(f"auth_smoke_job={smoke_id}", flush=True)
    wait_job(cred, smoke_id, timeout_s=600)
    delete_job(cred, smoke_id)
    print("HSC_AUTH_OK=yes", flush=True)

    for hats_band, db_band in BANDS:
        for field in FIELDS:
            stage_one(cred, stage, hats_band, db_band, field)
        p = pointer_root / f"hsc_pdr3_forced_{hats_band}_hats" / "SOURCE.json"
        p.parent.mkdir(parents=True, exist_ok=True)
        shards = sorted((stage / "shards").glob(f"hsc_nb_{hats_band}_*.csv.gz"))
        p.write_text(
            json.dumps(
                {
                    "alias": f"hsc_pdr3_forced_{hats_band}_hats",
                    "survey": "hsc",
                    "release": f"pdr3_forced_{hats_band}",
                    "band": hats_band,
                    "db_band": db_band,
                    "source": f"NAOJ {SCHEMA}.{TABLE} narrow-band",
                    "stage_root": str(stage),
                    "shard_files": [str(s) for s in shards],
                    "planned_hats_path": f"/arc/projects/hats/hsc/pdr3/forced_{hats_band}",
                    "hats_ready": False,
                    "probed_at_utc": dt.datetime.now(dt.UTC).isoformat(),
                },
                indent=2,
                sort_keys=True,
            )
            + "\n"
        )
    print("all narrow-band stage submits done", flush=True)


if __name__ == "__main__":
    main()
