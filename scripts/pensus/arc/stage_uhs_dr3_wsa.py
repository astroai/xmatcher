#!/usr/bin/env python3
"""Stage UHS DR3 seamless sources from WSA freeform SQL into CephFS CSV.gz strips."""
from __future__ import annotations

import argparse
import gzip
import http.cookiejar
import json
import pathlib
import re
import ssl
import time
import urllib.parse
import urllib.request

WSA_FORM = "http://wsa.roe.ac.uk:8080/wsa/SQL_form.jsp"
WSA_SQL = "http://wsa.roe.ac.uk:8080/wsa/WSASQL"
COLS = (
    "sourceID, ra, dec, mergedClass, "
    "jAperMag3, jAperMag3Err, hAperMag3, hAperMag3Err, kAperMag3, kAperMag3Err, "
    "priOrSec, frameSetID"
)


def opener() -> urllib.request.OpenerDirector:
    ctx = ssl._create_unverified_context()
    cj = http.cookiejar.CookieJar()
    return urllib.request.build_opener(
        urllib.request.HTTPCookieProcessor(cj),
        urllib.request.HTTPSHandler(context=ctx),
        urllib.request.HTTPHandler(),
    )


def run_query(op: urllib.request.OpenerDirector, sql: str, timeout: int = 3600) -> tuple[str, str]:
    op.open(WSA_FORM, timeout=60).read()
    data = urllib.parse.urlencode(
        {
            "formaction": "freeform",
            "database": "UHSDR3",
            "sqlstmt": sql,
            "emailAddress": "",
            "format": "CSV",
            "compress": "GZIP",
            "rows": "30",
            "timeout": str(timeout),
        }
    ).encode()
    req = urllib.request.Request(WSA_SQL, data=data, method="POST")
    resp = op.open(req, timeout=120)
    buf = b""
    t0 = time.time()
    while time.time() - t0 < timeout + 120:
        chunk = resp.read(65536)
        if not chunk:
            break
        buf += chunk
        text = buf.decode("latin-1", "replace")
        if "SQL Error:" in text or "Error Message" in text:
            m = re.search(r"SQL Error:.*", text)
            raise RuntimeError(m.group(0) if m else text[-1000:])
        m = re.search(r'href="(http://wsa\.roe\.ac\.uk/tmp/tmp_sql/[^"]+)"', text)
        if m and "QUERY FINISHED" in text:
            return m.group(1), text
    raise TimeoutError(f"WSA query did not finish in {timeout}s; tail={buf[-500:]!r}")


def download(op: urllib.request.OpenerDirector, url: str, dest: pathlib.Path) -> int:
    dest.parent.mkdir(parents=True, exist_ok=True)
    partial = dest.with_suffix(dest.suffix + ".partial")
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
    with gzip.open(path, "rt", errors="replace") as fh:
        header = next(fh, "")
        if not header:
            return 0
        for _ in fh:
            n += 1
    return n


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--stage", required=True)
    ap.add_argument("--pointer", required=True)
    ap.add_argument("--planned-hats", default="/arc/projects/hats/uhs/dr3")
    ap.add_argument("--ra-step", type=float, default=2.0)
    ap.add_argument("--ra-start", type=float, default=0.0)
    ap.add_argument("--ra-stop", type=float, default=360.0)
    ap.add_argument("--timeout", type=int, default=3600)
    ap.add_argument("--sleep", type=float, default=2.0)
    args = ap.parse_args()

    stage = pathlib.Path(args.stage)
    strips = stage / "strips"
    strips.mkdir(parents=True, exist_ok=True)
    op = opener()
    manifest = []
    ra0 = args.ra_start
    while ra0 < args.ra_stop - 1e-9:
        ra1 = min(ra0 + args.ra_step, args.ra_stop)
        name = f"uhs_ra_{ra0:06.2f}_{ra1:06.2f}.csv.gz".replace(".", "p")
        dest = strips / name
        meta = dest.with_suffix(".json")
        if dest.exists() and meta.exists():
            info = json.loads(meta.read_text())
            print(f"skip {name} rows={info.get('rows')}")
            manifest.append(info)
            ra0 = ra1
            continue
        sql = (
            f"SELECT {COLS} FROM uhsSource "
            f"WHERE priOrSec IN (0, frameSetID) AND ra >= {ra0} AND ra < {ra1}"
        )
        print(f"query {ra0}-{ra1} ...", flush=True)
        url, _ = run_query(op, sql, timeout=args.timeout)
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
            "finished_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        meta.write_text(json.dumps(info, indent=2, sort_keys=True) + "\n")
        print(f"wrote {dest} rows={rows} bytes={nbytes}", flush=True)
        manifest.append(info)
        ra0 = ra1
        time.sleep(args.sleep)

    total_rows = sum(int(x["rows"]) for x in manifest)
    total_bytes = sum(int(x["bytes"]) for x in manifest)
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    pointer = {
        "survey": "uhs",
        "release": "dr3",
        "alias": "uhs_dr3_hats",
        "stage_path": str(stage),
        "temporary_host": True,
        "host": "/arc/projects/mlao",
        "planned_hats_path": args.planned_hats,
        "source": "WSA UHSDR3 freeform SQL uhsSource seamless priOrSec",
        "ra_step_deg": args.ra_step,
        "strips": len(manifest),
        "nrows_staged": total_rows,
        "bytes_staged": total_bytes,
        "exists": True,
        "hats_ready": False,
        "probed_at_utc": now,
        "manifest": manifest,
    }
    pathlib.Path(args.pointer).parent.mkdir(parents=True, exist_ok=True)
    pathlib.Path(args.pointer).write_text(json.dumps(pointer, indent=2, sort_keys=True) + "\n")
    (stage / "STAGE_MANIFEST.json").write_text(json.dumps(pointer, indent=2, sort_keys=True) + "\n")
    print(f"done strips={len(manifest)} rows={total_rows} bytes={total_bytes} pointer={args.pointer}")


if __name__ == "__main__":
    main()
