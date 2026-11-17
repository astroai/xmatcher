#!/usr/bin/env python3
"""Stage KiDS DR5 multi-band catalogue tiles via anonymous ESO ObsCore + dataportal.

Discovery: archive.eso.org/tap_obs ObsCore rows with
  obs_collection='KIDS'
  dataproduct_subtype LIKE '%catalogtile%'
  publication_date >= '2025-01-01'
Download: datalink JSON → dataportal.eso.org/dataPortal/file/ADP… (no login).
Leiden kids_dr5.0_cat_wget.sh remains 404; do not wait on it.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import io
import json
import pathlib
import re
import time
import urllib.error
import urllib.parse
import urllib.request

TAP_OBS = "https://archive.eso.org/tap_obs/sync"
DATALINK = "https://archive.eso.org/datalink/links"
UA = {"User-Agent": "pensus-stage-kids-dr5/1.0 (anonymous; astronomy research)"}
ADQL = (
    "SELECT obs_publisher_did, publication_date, access_estsize, access_url "
    "FROM ivoa.ObsCore "
    "WHERE obs_collection = 'KIDS' "
    "AND dataproduct_subtype LIKE '%catalogtile%' "
    "AND publication_date >= '2025-01-01' "
    "ORDER BY obs_publisher_did"
)


def http_open(url: str, timeout: int = 120, headers: dict | None = None):
    req = urllib.request.Request(url, headers={**UA, **(headers or {})})
    return urllib.request.urlopen(req, timeout=timeout)


def tap_csv(query: str, timeout: int = 300) -> str:
    url = (
        TAP_OBS
        + "?REQUEST=doQuery&LANG=ADQL&FORMAT=csv&QUERY="
        + urllib.parse.quote(query)
        + "&MAXREC=10000"
    )
    last: Exception | None = None
    for attempt in range(1, 6):
        try:
            with http_open(url, timeout=timeout) as resp:
                return resp.read().decode("utf-8", "replace")
        except Exception as exc:  # noqa: BLE001
            last = exc
            time.sleep(min(2**attempt, 30))
    raise RuntimeError(f"ObsCore TAP failed: {last}")


def csv_rows(raw: str) -> list[dict[str, str]]:
    lines = [ln for ln in raw.splitlines() if ln and not ln.startswith("#")]
    if not lines:
        return []
    return list(csv.DictReader(io.StringIO("\n".join(lines))))


def adp_from_publisher_did(did: str) -> str:
    # ivo://eso.org/ID?ADP.2025-03-12T10:03:42.271
    m = re.search(r"(ADP\.[0-9T:\.\-]+)", did or "")
    if not m:
        raise ValueError(f"no ADP id in {did!r}")
    return m.group(1)


def resolve_download(adp: str, timeout: int = 120) -> dict:
    url = DATALINK + "?ID=" + urllib.parse.quote(f"ivo://eso.org/ID?{adp}") + "&RESPONSEFORMAT=json"
    last: Exception | None = None
    for attempt in range(1, 6):
        try:
            with http_open(url, timeout=timeout) as resp:
                payload = json.loads(resp.read().decode("utf-8", "replace"))
            if not isinstance(payload, list):
                raise RuntimeError(f"unexpected datalink payload type {type(payload)}")
            for row in payload:
                if row.get("semantics") == "#this" and row.get("access_url"):
                    return {
                        "adp": adp,
                        "access_url": row["access_url"],
                        "content_length": int(row["content_length"])
                        if row.get("content_length") not in (None, "")
                        else None,
                        "origfile": row.get("eso_origfile") or f"{adp}.fits",
                        "content_type": row.get("content_type"),
                    }
            raise RuntimeError(f"no #this link for {adp}")
        except Exception as exc:  # noqa: BLE001
            last = exc
            time.sleep(min(2**attempt, 30))
    raise RuntimeError(f"datalink failed for {adp}: {last}")


def download_file(url: str, dest: pathlib.Path, expected: int | None, timeout: int) -> int:
    dest.parent.mkdir(parents=True, exist_ok=True)
    partial = dest.with_suffix(dest.suffix + ".partial")
    existing = partial.stat().st_size if partial.is_file() else 0
    headers = {}
    mode = "wb"
    if existing > 0:
        headers["Range"] = f"bytes={existing}-"
        mode = "ab"
    last: Exception | None = None
    for attempt in range(1, 8):
        try:
            req = urllib.request.Request(url, headers={**UA, **headers})
            with urllib.request.urlopen(req, timeout=timeout) as resp, open(partial, mode) as out:
                while True:
                    chunk = resp.read(1024 * 1024)
                    if not chunk:
                        break
                    out.write(chunk)
            nbytes = partial.stat().st_size
            # Validate FITS magic
            with open(partial, "rb") as fh:
                magic = fh.read(6)
            if magic != b"SIMPLE":
                partial.unlink(missing_ok=True)
                raise RuntimeError(f"not a FITS file (magic={magic!r}) url={url}")
            # Allow oversize if server ignored Range and rewrote a full file.
            if expected is not None and nbytes < expected:
                raise RuntimeError(f"size mismatch got={nbytes} expected={expected} url={url}")
            partial.replace(dest)
            return nbytes
        except Exception as exc:  # noqa: BLE001
            last = exc
            # Reset partial on hard failures other than timeout mid-stream.
            if isinstance(exc, urllib.error.HTTPError) and exc.code in (416,):
                partial.unlink(missing_ok=True)
                existing = 0
                headers = {}
                mode = "wb"
            time.sleep(min(15 * attempt, 120))
            existing = partial.stat().st_size if partial.is_file() else 0
            if existing > 0:
                headers = {"Range": f"bytes={existing}-"}
                mode = "ab"
            else:
                headers = {}
                mode = "wb"
    raise RuntimeError(f"download failed {url}: {last}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--stage", required=True)
    ap.add_argument("--pointer", required=True)
    ap.add_argument("--planned-hats", default="/arc/projects/hats/kids/dr5")
    ap.add_argument("--max-tiles", type=int, default=0, help="0 = all")
    ap.add_argument("--timeout", type=int, default=600)
    args = ap.parse_args()

    stage = pathlib.Path(args.stage)
    tiles_dir = stage / "tiles"
    tiles_dir.mkdir(parents=True, exist_ok=True)
    pointer = pathlib.Path(args.pointer)
    pointer.parent.mkdir(parents=True, exist_ok=True)

    raw = tap_csv(ADQL)
    rows = csv_rows(raw)
    print(f"obscore_tiles={len(rows)}", flush=True)
    if not rows:
        raise RuntimeError("ObsCore returned zero KiDS catalog tiles")

    items: list[dict] = []
    seen: set[str] = set()
    for row in rows:
        did = row.get("obs_publisher_did") or row.get("access_url") or ""
        try:
            adp = adp_from_publisher_did(did)
        except ValueError:
            # Fall back: parse ADP from access_url query
            m = re.search(r"(ADP\.[0-9T:\.\-]+)", row.get("access_url") or "")
            if not m:
                print(f"WARN skip unparseable row={row}", flush=True)
                continue
            adp = m.group(1)
        if adp in seen:
            continue
        seen.add(adp)
        items.append(
            {
                "adp": adp,
                "publication_date": row.get("publication_date"),
                "access_estsize": row.get("access_estsize"),
                "access_url": row.get("access_url"),
            }
        )
    if args.max_tiles > 0:
        items = items[: args.max_tiles]
    print(f"unique_adp={len(items)}", flush=True)

    manifest: list[dict] = []
    done = 0
    total_bytes = 0
    for item in items:
        adp = item["adp"]
        marker = tiles_dir / f"{adp}.done.json"
        # Prefer already-downloaded dest named in marker.
        if marker.is_file():
            meta = json.loads(marker.read_text())
            dest = pathlib.Path(meta.get("path", ""))
            if dest.is_file() and dest.stat().st_size > 0:
                manifest.append(meta)
                total_bytes += int(meta.get("bytes", dest.stat().st_size))
                done += 1
                continue

        info = resolve_download(adp, timeout=min(args.timeout, 180))
        orig = info["origfile"] or f"{adp}.fits"
        # Sanitize filename
        safe = re.sub(r"[^A-Za-z0-9._\-]+", "_", orig)
        dest = tiles_dir / safe
        if dest.is_file() and dest.stat().st_size > 0:
            with open(dest, "rb") as fh:
                if fh.read(6) == b"SIMPLE":
                    meta = {
                        "adp": adp,
                        "origfile": orig,
                        "path": str(dest),
                        "bytes": dest.stat().st_size,
                        "content_length": info.get("content_length"),
                        "finished_at_utc": dt.datetime.now(dt.UTC).isoformat(),
                        "resumed_existing": True,
                    }
                    marker.write_text(json.dumps(meta, indent=2, sort_keys=True) + "\n")
                    manifest.append(meta)
                    total_bytes += meta["bytes"]
                    done += 1
                    continue

        nbytes = download_file(
            info["access_url"],
            dest,
            expected=info.get("content_length"),
            timeout=args.timeout,
        )
        meta = {
            "adp": adp,
            "origfile": orig,
            "path": str(dest),
            "bytes": nbytes,
            "content_length": info.get("content_length"),
            "dataportal_url": info["access_url"],
            "publication_date": item.get("publication_date"),
            "finished_at_utc": dt.datetime.now(dt.UTC).isoformat(),
        }
        marker.write_text(json.dumps(meta, indent=2, sort_keys=True) + "\n")
        manifest.append(meta)
        total_bytes += nbytes
        done += 1
        if done % 10 == 0 or done == len(items):
            print(
                f"progress {done}/{len(items)} bytes={total_bytes} last={safe}",
                flush=True,
            )

    now = dt.datetime.now(dt.UTC).isoformat()
    pointer.write_text(
        json.dumps(
            {
                "alias": "kids_dr5_hats",
                "survey": "kids",
                "release": "dr5",
                "source": (
                    "ESO ObsCore anonymous catalogtile,multi-band (publ>=2025) "
                    "+ dataportal.eso.org tile FITS"
                ),
                "stage_root": str(stage),
                "planned_hats_path": args.planned_hats,
                "tiles_staged": done,
                "tiles_expected": len(items),
                "bytes_staged": total_bytes,
                "exists": done > 0,
                "hats_ready": False,
                "official_mirror": True,
                "blocker": None,
                "note": (
                    "Leiden kids_dr5.0_cat_wget.sh still 404; "
                    "QI login not required for tile dataportal path"
                ),
                "probed_at_utc": now,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n"
    )
    # Compact manifest sidecar (paths only) for resume tooling
    (stage / "MANIFEST.json").write_text(
        json.dumps(
            {"tiles": manifest, "written_at_utc": now},
            indent=2,
            sort_keys=True,
        )
        + "\n"
    )
    print(
        f"done tiles={done}/{len(items)} bytes={total_bytes} pointer={pointer}",
        flush=True,
    )


if __name__ == "__main__":
    main()
