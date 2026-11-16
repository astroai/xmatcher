#!/usr/bin/env python3
"""Stage VST ATLAS DR5 seamless sources from OSA freeform SQL (ATLASDR5..atlasSource).

OSA CSV/GZIP currently fails with 'temp directory not available for writing'.
HTML format still works with an inline table, capped at 1000 displayed rows, so
this stager pages with sourceID cursors inside fine RA strips.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import html as htmlmod
import http.cookiejar
import json
import pathlib
import re
import ssl
import time
import urllib.parse
import urllib.request

OSA_FORM = "http://osa.roe.ac.uk:8080/osa/OSQL_form.jsp"
OSA_SQL = "http://osa.roe.ac.uk:8080/osa/WSASQL"
DATABASE = "ATLASDR5"
COLS = (
    "sourceID, ra, dec, mergedClass, "
    "uAperMag3, uAperMag3Err, gAperMag3, gAperMag3Err, "
    "rAperMag3, rAperMag3Err, iAperMag3, iAperMag3Err, "
    "zAperMag3, zAperMag3Err, priOrSec, frameSetID"
)
PAGE_ROWS = 1000


def opener() -> urllib.request.OpenerDirector:
    ctx = ssl._create_unverified_context()
    cj = http.cookiejar.CookieJar()
    return urllib.request.build_opener(
        urllib.request.HTTPCookieProcessor(cj),
        urllib.request.HTTPSHandler(context=ctx),
        urllib.request.HTTPHandler(),
    )


def run_html_query(op: urllib.request.OpenerDirector, sql: str, timeout: int = 600) -> str:
    op.open(OSA_FORM, timeout=60).read(256)
    data = urllib.parse.urlencode(
        {
            "formaction": "freeform",
            "database": DATABASE,
            "sqlstmt": sql,
            "emailAddress": "",
            "format": "HTML",
            "compress": "NONE",
            "rows": str(PAGE_ROWS),
            "timeout": str(timeout),
        }
    ).encode()
    req = urllib.request.Request(OSA_SQL, data=data, method="POST")
    resp = op.open(req, timeout=timeout + 60)
    text = resp.read().decode("latin-1", "replace")
    if "SQL Error:" in text:
        m = re.search(r"SQL Error:.*", text)
        raise RuntimeError(m.group(0) if m else "OSA SQL error")
    if "QUERY FINISHED" not in text:
        raise TimeoutError(f"OSA HTML query incomplete; tail={text[-400]!r}")
    return text


def parse_html_table(text: str) -> tuple[list[str], list[list[str]]]:
    """Parse the first results <table> into header + rows (string cells)."""
    m = re.search(
        r"<table[^>]*>\s*<tr[^>]*>\s*(?:<th[^>]*>.*?</th>\s*)+</tr>(.*?)</table>",
        text,
        re.I | re.S,
    )
    if not m:
        # Empty result is OK.
        if re.search(r"0\s+rows?", text, re.I) or "No results" in text:
            return [], []
        raise RuntimeError(f"no HTML results table; head={text[:400]!r}")
    body = m.group(0)
    header_cells = re.findall(r"<th[^>]*>(.*?)</th>", body, re.I | re.S)
    headers = [
        re.sub(r"<[^>]+>", "", htmlmod.unescape(h)).strip()
        for h in header_cells
        if re.sub(r"<[^>]+>", "", h).strip()
    ]
    # Drop leading blank row-number header if present as empty after strip of &nbsp;
    if headers and headers[0] in ("", "\xa0", "&nbsp;"):
        headers = headers[1:]
    rows: list[list[str]] = []
    for tr in re.findall(r"<tr[^>]*class=\"Row(?:Odd|Even)\"[^>]*>(.*?)</tr>", body, re.I | re.S):
        cells = re.findall(r"<td[^>]*>(.*?)</td>", tr, re.I | re.S)
        vals = [re.sub(r"<[^>]+>", "", htmlmod.unescape(c)).strip() for c in cells]
        if not vals:
            continue
        # First cell is row index.
        if vals and re.fullmatch(r"\d+", vals[0] or ""):
            vals = vals[1:]
        if len(vals) != len(headers):
            # Be tolerant if trailing empties differ.
            if len(vals) < len(headers):
                vals = vals + [""] * (len(headers) - len(vals))
            else:
                vals = vals[: len(headers)]
        rows.append(vals)
    return headers, rows


def page_strip(
    op: urllib.request.OpenerDirector,
    ra0: float,
    ra1: float,
    timeout: int,
    retries: int,
) -> tuple[list[str], list[list[str]]]:
    headers: list[str] = []
    all_rows: list[list[str]] = []
    last_id = 0
    while True:
        sql = (
            f"SELECT TOP {PAGE_ROWS} {COLS} FROM atlasSource "
            f"WHERE priOrSec IN (0, frameSetID) AND ra >= {ra0} AND ra < {ra1} "
            f"AND sourceID > {last_id} ORDER BY sourceID"
        )
        text = None
        last_err: Exception | None = None
        for attempt in range(1, retries + 1):
            try:
                text = run_html_query(op, sql, timeout=timeout)
                break
            except Exception as exc:  # noqa: BLE001
                last_err = exc
                print(
                    f"WARN ra=[{ra0},{ra1}) after={last_id} attempt={attempt}: {exc}",
                    flush=True,
                )
                time.sleep(min(20 * attempt, 120))
        if text is None:
            raise RuntimeError(f"failed page ra=[{ra0},{ra1}) after={last_id}: {last_err}")
        hdr, rows = parse_html_table(text)
        if not rows:
            break
        # OSA injects a getFSLink / "view" column ahead of selected fields — drop it.
        if "getFSLink" in hdr:
            drop = hdr.index("getFSLink")
            hdr = [h for i, h in enumerate(hdr) if i != drop]
            rows = [[c for i, c in enumerate(r) if i != drop] for r in rows]
        if not headers:
            headers = hdr
        all_rows.extend(rows)
        try:
            sid_idx = headers.index("sourceID")
        except ValueError as exc:
            raise RuntimeError(f"sourceID missing in headers={headers}") from exc
        last_id = int(float(rows[-1][sid_idx]))
        print(
            f"  page ra=[{ra0},{ra1}) got={len(rows)} total={len(all_rows)} last_id={last_id}",
            flush=True,
        )
        if len(rows) < PAGE_ROWS:
            break
    return headers, all_rows


def write_csv_gz(path: pathlib.Path, headers: list[str], rows: list[list[str]]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_suffix(path.suffix + ".partial")
    with gzip.open(partial, "wt", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(headers)
        w.writerows(rows)
    partial.replace(path)
    return path.stat().st_size


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--stage", required=True)
    ap.add_argument("--pointer", required=True)
    ap.add_argument("--planned-hats", default="/arc/projects/hats/atlas/dr5")
    ap.add_argument("--ra-step", type=float, default=0.25)
    ap.add_argument("--ra-start", type=float, default=0.0)
    ap.add_argument("--ra-stop", type=float, default=360.0)
    ap.add_argument("--timeout", type=int, default=600)
    ap.add_argument("--retries", type=int, default=6)
    args = ap.parse_args()

    stage = pathlib.Path(args.stage)
    strips = stage / "strips"
    strips.mkdir(parents=True, exist_ok=True)
    pointer = pathlib.Path(args.pointer)
    pointer.parent.mkdir(parents=True, exist_ok=True)
    op = opener()

    ra0 = args.ra_start
    manifest: list[dict] = []
    while ra0 < args.ra_stop - 1e-9:
        ra1 = min(args.ra_stop, ra0 + args.ra_step)
        name = f"atlas_ra_{ra0:06.2f}_{ra1:06.2f}".replace(".", "p")
        dest = strips / f"{name}.csv.gz"
        marker = strips / f"{name}.done.json"
        if marker.is_file() and dest.is_file():
            meta = json.loads(marker.read_text())
            manifest.append(meta)
            ra0 = ra1
            continue
        headers, rows = page_strip(op, ra0, ra1, timeout=args.timeout, retries=args.retries)
        if not headers:
            headers = [c.strip() for c in COLS.split(",")]
        nbytes = write_csv_gz(dest, headers, rows)
        meta = {
            "file": dest.name,
            "ra0": ra0,
            "ra1": ra1,
            "rows": len(rows),
            "bytes": nbytes,
            "mode": "osa_html_paged",
            "finished_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        marker.write_text(json.dumps(meta, indent=2, sort_keys=True) + "\n")
        manifest.append(meta)
        print(f"wrote {dest} rows={len(rows)} bytes={nbytes}", flush=True)
        ra0 = ra1

    total_rows = sum(int(m["rows"]) for m in manifest)
    pointer.write_text(
        json.dumps(
            {
                "alias": "atlas_dr5_hats",
                "survey": "atlas",
                "release": "dr5",
                "source": "OSA ATLASDR5 atlasSource seamless (HTML paged; CSV temp-dir broken)",
                "note": (
                    "OSA ATLASDR5 is the final public OSA release (2021). "
                    "ESO QI catalog 438 (DR5, 2025-10) needs login — preferred once credentials available."
                ),
                "stage_root": str(stage),
                "planned_hats_path": args.planned_hats,
                "nrows_staged": total_rows,
                "manifest": manifest,
                "exists": True,
                "hats_ready": False,
                "official_mirror": False,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n"
    )
    print(f"done strips={len(manifest)} rows={total_rows}", flush=True)


if __name__ == "__main__":
    main()
