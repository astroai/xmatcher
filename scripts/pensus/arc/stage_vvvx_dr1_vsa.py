#!/usr/bin/env python3
"""Stage VVVX DR1 seamless sources from VSA freeform SQL into CephFS CSV.gz strips.

Pin: VSA VVVXDR1 (public 2026-05-19), programmeID=170, table vvvxSource.
JHKs photometry only (no Z/Y on this table). VIRAC2 proper motions are ESO
phase-3 (not in VSA) and are deferred — this job stages VVVX catalog photometry.
Form path is /vdfs/ (not /wsa/).
"""

from __future__ import annotations

import argparse
import gzip
import http.cookiejar
import json
import pathlib
import re
import ssl
import time
import urllib.error
import urllib.request

PROGRAMME_ID = "170"
DATABASE = "VVVXDR1"
RELEASE_LABEL = "VVVXDR1"
TABLE = "vvvxSource"
FORM = (
    f"http://vsa.roe.ac.uk:8080/vdfs/VSQL_form.jsp?programmeID={PROGRAMME_ID}&database={DATABASE}"
)
POST = "http://vsa.roe.ac.uk:8080/vdfs/WSASQL"
COLS = (
    "sourceID, ra, dec, mergedClass, "
    "jAperMag3, jAperMag3Err, hAperMag3, hAperMag3Err, "
    "ksAperMag3, ksAperMag3Err, priOrSec, frameSetID"
)


def opener() -> urllib.request.OpenerDirector:
    ctx = ssl._create_unverified_context()
    cj = http.cookiejar.CookieJar()
    return urllib.request.build_opener(
        urllib.request.HTTPCookieProcessor(cj),
        urllib.request.HTTPSHandler(context=ctx),
        urllib.request.HTTPHandler(),
    )


def multipart_encode(fields: dict[str, str]) -> tuple[bytes, str]:
    boundary = "----PensusBoundaryVVVX1"
    parts: list[str] = []
    for k, v in fields.items():
        parts.append(f'--{boundary}\r\nContent-Disposition: form-data; name="{k}"\r\n\r\n{v}\r\n')
    parts.append(f"--{boundary}--\r\n")
    return "".join(parts).encode("utf-8"), boundary


def run_query(op: urllib.request.OpenerDirector, sql: str, timeout: int = 3600) -> str:
    op.open(FORM, timeout=60).read()
    fields = {
        "archive": "VSA",
        "disp": "",
        "formaction": "freeform",
        "programmeID": PROGRAMME_ID,
        "database": DATABASE,
        "sqlstmt": sql,
        "emailAddress": "",
        "format": "CSV",
        "compress": "GZIP",
        "rows": "30",
        "timeout": str(timeout),
    }
    body, boundary = multipart_encode(fields)
    delays = (0, 5, 15, 45, 90, 180)
    last_err: Exception | None = None
    for delay in delays:
        if delay:
            time.sleep(delay)
        try:
            req = urllib.request.Request(POST, data=body, method="POST")
            req.add_header("Content-Type", f"multipart/form-data; boundary={boundary}")
            resp = op.open(req, timeout=120)
            buf = b""
            t0 = time.time()
            while time.time() - t0 < timeout + 120:
                chunk = resp.read(65536)
                if not chunk:
                    break
                buf += chunk
                text = buf.decode("latin-1", "replace")
                if "SQL Error:" in text and "QUERY FINISHED" in text:
                    m = re.search(r"SQL Error:.*", text)
                    msg = m.group(0) if m else text[-1000:]
                    # VSA buffer-pool OOM on dense plane strips — retryable.
                    if "insufficient memory" in msg.lower() or "buffer pool" in msg.lower():
                        raise TimeoutError(msg)
                    raise RuntimeError(msg)
                if "Error in run():" in text and "QUERY FINISHED" in text:
                    raise RuntimeError("VSA Error in run(); check archive/multipart")
                m = re.search(r'href="(http://vsa\.roe\.ac\.uk/tmp/tmp_sql/[^"]+)"', text)
                if m and "QUERY FINISHED" in text:
                    return m.group(1)
            raise TimeoutError(f"VSA query did not finish in {timeout}s")
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as e:
            last_err = e
            if isinstance(e, urllib.error.HTTPError) and e.code not in (
                429,
                500,
                502,
                503,
                504,
            ):
                raise
            print(f"retry after {type(e).__name__}: {e}", flush=True)
    assert last_err is not None
    raise last_err


def download(op: urllib.request.OpenerDirector, url: str, dest: pathlib.Path) -> int:
    dest.parent.mkdir(parents=True, exist_ok=True)
    partial = pathlib.Path(str(dest) + ".partial")
    with op.open(url, timeout=600) as resp, open(partial, "wb") as out:
        while True:
            chunk = resp.read(1024 * 1024)
            if not chunk:
                break
            out.write(chunk)
    partial.replace(dest)
    return dest.stat().st_size


def count_rows(path: pathlib.Path) -> int:
    n = 0
    header_seen = False
    with gzip.open(path, "rt", errors="replace") as fh:
        for line in fh:
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            if not header_seen and s.lower().startswith("sourceid"):
                header_seen = True
                continue
            n += 1
    return n


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--stage", required=True)
    ap.add_argument("--pointer", required=True)
    ap.add_argument("--planned-hats", default="/arc/projects/hats/vvvx/dr1")
    ap.add_argument("--ra-step", type=float, default=0.25)
    ap.add_argument("--ra-start", type=float, default=0.0)
    ap.add_argument("--ra-stop", type=float, default=360.0)
    ap.add_argument("--timeout", type=int, default=3600)
    ap.add_argument("--sleep", type=float, default=2.0)
    args = ap.parse_args()

    stage = pathlib.Path(args.stage)
    strips = stage / "strips"
    strips.mkdir(parents=True, exist_ok=True)
    op = opener()
    manifest: list[dict] = []
    ra0 = args.ra_start
    while ra0 < args.ra_stop - 1e-9:
        ra1 = min(ra0 + args.ra_step, args.ra_stop)
        name = f"vvvx_ra_{ra0:06.2f}_{ra1:06.2f}".replace(".", "p") + ".csv.gz"
        dest = strips / name
        meta = pathlib.Path(str(dest) + ".json")
        if dest.exists() and meta.exists():
            info = json.loads(meta.read_text())
            print(f"skip {name} rows={info.get('rows')}", flush=True)
            manifest.append(info)
            ra0 = ra1
            continue
        sql = (
            f"SELECT {COLS} FROM {TABLE} "
            f"WHERE priOrSec IN (0, frameSetID) AND ra >= {ra0} AND ra < {ra1}"
        )
        print(f"query {ra0}-{ra1} ...", flush=True)
        url = run_query(op, sql, timeout=args.timeout)
        nbytes = download(op, url, dest)
        rows = count_rows(dest)
        info = {
            "file": dest.name,
            "ra0": ra0,
            "ra1": ra1,
            "rows": rows,
            "bytes": nbytes,
            "wsa_url": url,
            "sql": sql,
            "database": DATABASE,
            "programmeID": int(PROGRAMME_ID),
            "table": TABLE,
            "release_label": RELEASE_LABEL,
            "finished_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        meta.write_text(json.dumps(info, indent=2, sort_keys=True) + "\n")
        print(f"wrote {dest} rows={rows} bytes={nbytes}", flush=True)
        manifest.append(info)
        ra0 = ra1
        time.sleep(args.sleep)

    # Include any prior strip sidecars (e.g. 0.5° pass before a finer resume).
    by_file = {x["file"]: x for x in manifest}
    for meta_path in sorted(strips.glob("*.csv.gz.json")):
        try:
            info = json.loads(meta_path.read_text())
        except Exception:
            continue
        fname = info.get("file") or meta_path.name[: -len(".json")]
        by_file.setdefault(fname, info)
    manifest = sorted(by_file.values(), key=lambda x: (float(x.get("ra0", 0)), float(x.get("ra1", 0))))

    total_rows = sum(int(x["rows"]) for x in manifest)
    total_bytes = sum(int(x["bytes"]) for x in manifest)
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    pointer = {
        "survey": "vvvx",
        "release": "dr1",
        "alias": "vvvx_dr1_hats",
        "stage_path": str(stage),
        "temporary_host": True,
        "host": "/arc/projects/unions",
        "planned_hats_path": args.planned_hats,
        "source": (
            f"VSA {DATABASE} freeform SQL {TABLE} seamless priOrSec "
            f"(programmeID={PROGRAMME_ID}, {RELEASE_LABEL}); "
            "VIRAC2 PMs deferred (ESO phase-3, not in VSA)"
        ),
        "database": DATABASE,
        "release_label": RELEASE_LABEL,
        "table": TABLE,
        "programmeID": int(PROGRAMME_ID),
        "ra_step_deg": args.ra_step,
        "strips": len(manifest),
        "nrows_staged": total_rows,
        "bytes_staged": total_bytes,
        "exists": True,
        "hats_ready": False,
        "virac2_note": "VIRAC2 is ESO safcat.VVVX_VIRAC_V2_SOURCES; not staged here",
        "probed_at_utc": now,
        "manifest": manifest,
    }
    pathlib.Path(args.pointer).parent.mkdir(parents=True, exist_ok=True)
    pathlib.Path(args.pointer).write_text(json.dumps(pointer, indent=2, sort_keys=True) + "\n")
    (stage / "STAGE_MANIFEST.json").write_text(json.dumps(pointer, indent=2, sort_keys=True) + "\n")
    print(
        f"done strips={len(manifest)} rows={total_rows} bytes={total_bytes} pointer={args.pointer}"
    )


if __name__ == "__main__":
    main()
