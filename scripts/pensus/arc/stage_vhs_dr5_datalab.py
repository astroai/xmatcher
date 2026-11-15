#!/usr/bin/env python3
"""Stage VHS DR5 from NOIRLab Data Lab TAP into CephFS CSV.gz RA strips."""

from __future__ import annotations

import argparse
import gzip
import json
import pathlib
import time
import urllib.parse
import urllib.request

DL = "https://datalab.noirlab.edu/tap/sync"
COLS = "sourceid, ra2000, dec2000, japermag3, japermag3err, hapermag3, hapermag3err, ksapermag3, ksapermag3err"


def fetch_csv(sql: str, maxrec: int = 2_000_000, timeout: int = 600) -> bytes:
    url = f"{DL}?REQUEST=doQuery&LANG=ADQL&FORMAT=csv&MAXREC={maxrec}&QUERY=" + urllib.parse.quote(
        sql
    )
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return resp.read()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--stage", required=True)
    ap.add_argument("--pointer", required=True)
    ap.add_argument("--planned-hats", default="/arc/projects/hats/vhs/dr5")
    ap.add_argument("--ra-step", type=float, default=0.25)
    ap.add_argument("--ra-start", type=float, default=0.0)
    ap.add_argument("--ra-stop", type=float, default=360.0)
    ap.add_argument("--sleep", type=float, default=1.0)
    args = ap.parse_args()

    stage = pathlib.Path(args.stage)
    strips = stage / "strips"
    strips.mkdir(parents=True, exist_ok=True)
    manifest = []
    ra0 = args.ra_start
    while ra0 < args.ra_stop - 1e-9:
        ra1 = min(ra0 + args.ra_step, args.ra_stop)
        name = f"vhs_ra_{ra0:07.3f}_{ra1:07.3f}".replace(".", "p") + ".csv.gz"
        dest = strips / name
        meta = dest.with_suffix(dest.suffix + ".json")  # .csv.gz.json awkward
        meta = pathlib.Path(str(dest) + ".json")
        if dest.exists() and meta.exists():
            info = json.loads(meta.read_text())
            print(f"skip {name} rows={info.get('rows')}", flush=True)
            manifest.append(info)
            ra0 = ra1
            continue
        sql = f"SELECT {COLS} FROM vhs_dr5.vhs_cat_v3 WHERE ra2000 >= {ra0} AND ra2000 < {ra1}"
        print(f"query {ra0}-{ra1} ...", flush=True)
        t0 = time.time()
        raw = fetch_csv(sql)
        # verify header + count
        text = raw.decode("utf-8", "replace")
        lines = text.splitlines()
        if not lines or not lines[0].lower().startswith("sourceid"):
            raise RuntimeError(f"bad header for {ra0}-{ra1}: {lines[:2]!r}")
        rows = max(0, len(lines) - 1)
        if rows >= 1_900_000:
            raise RuntimeError(f"row cap risk {rows} for {ra0}-{ra1}; shrink --ra-step")
        partial = dest.with_suffix(dest.suffix + ".partial")
        with gzip.open(partial, "wt", compresslevel=3) as out:
            out.write(text)
            if not text.endswith("\n"):
                out.write("\n")
        partial.replace(dest)
        nbytes = dest.stat().st_size
        info = {
            "file": dest.name,
            "ra0": ra0,
            "ra1": ra1,
            "rows": rows,
            "bytes": nbytes,
            "elapsed_s": round(time.time() - t0, 1),
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
        "survey": "vhs",
        "release": "dr5",
        "alias": "vhs_dr5_hats",
        "stage_path": str(stage),
        "temporary_host": True,
        "host": "/arc/projects/unions",
        "planned_hats_path": args.planned_hats,
        "source": "NOIRLab Data Lab TAP vhs_dr5.vhs_cat_v3",
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
    print(f"done strips={len(manifest)} rows={total_rows} bytes={total_bytes}")


if __name__ == "__main__":
    main()
