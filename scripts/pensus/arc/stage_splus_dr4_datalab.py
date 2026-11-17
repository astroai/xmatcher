#!/usr/bin/env python3
"""Stage S-PLUS DR4 dual photometry from NOIRLab Data Lab TAP (public).

Day-0 pin: public DR4 only — do not use iDR5 / splus.cloud collab tables.
Table: splus_dr4.dual. Shard by RA strips (default 2°).
"""

from __future__ import annotations

import argparse
import datetime as dt
import gzip
import json
import pathlib
import time
import urllib.parse
import urllib.request

TAP = "https://datalab.noirlab.edu/tap/sync"
COLS = (
    "id, ra, dec, field, class_star, "
    "u_auto, g_auto, r_auto, i_auto, z_auto, "
    "e_u_auto, e_g_auto, e_r_auto, e_i_auto, e_z_auto, "
    "u_aper_6, g_aper_6, r_aper_6, i_aper_6, z_aper_6, "
    "j0378_auto, j0395_auto, j0410_auto, j0430_auto, j0515_auto, j0660_auto, j0861_auto, "
    "e_j0378_auto, e_j0395_auto, e_j0410_auto, e_j0430_auto, e_j0515_auto, e_j0660_auto, e_j0861_auto"
)
# Official public DR4 row count (Data Lab COUNT(*) 2026-11-17).
EXPECTED_ROWS = 80_155_547


def tap_csv(query: str, timeout: int = 900) -> bytes:
    url = TAP + "?REQUEST=doQuery&LANG=ADQL&FORMAT=csv&QUERY=" + urllib.parse.quote(query)
    last: Exception | None = None
    for attempt in range(1, 8):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "pensus-stage/1.0"})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                data = resp.read()
            text = data[:200].decode("utf-8", "replace")
            if "<VOTABLE" in text and "ERROR" in text:
                raise RuntimeError(f"Data Lab TAP error: {text[:500]}")
            return data
        except Exception as exc:  # noqa: BLE001
            last = exc
            time.sleep(min(2**attempt, 60))
    raise RuntimeError(f"TAP failed after retries: {last}")


def write_shard(
    shards: pathlib.Path, name: str, raw: bytes, extra: dict
) -> tuple[pathlib.Path, int, str]:
    marker = shards / f"{name}.done.json"
    csv_path = shards / f"{name}.csv"
    # Strip any comment lines before parquet conversion.
    text = raw.decode("utf-8", "replace")
    cleaned = "\n".join(ln for ln in text.splitlines() if ln and not ln.startswith("#"))
    csv_path.write_text(cleaned + ("\n" if cleaned else ""), encoding="utf-8")
    # header + data
    nrows = max(0, cleaned.count("\n") - (1 if cleaned else 0))
    try:
        import pyarrow.csv as pacsv
        import pyarrow.parquet as pq

        out = shards / f"{name}.parquet"
        pq.write_table(pacsv.read_csv(csv_path), out, compression="zstd")
        csv_path.unlink(missing_ok=True)
        fmt = "parquet"
    except Exception as exc:  # noqa: BLE001
        print(f"parquet fallback {name}: {exc}", flush=True)
        out = shards / f"{name}.csv.gz"
        with open(csv_path, "rb") as src, gzip.open(out, "wb") as dst:
            dst.write(src.read())
        csv_path.unlink(missing_ok=True)
        fmt = "csv.gz"
    marker.write_text(
        json.dumps(
            {
                **extra,
                "rows": nrows,
                "path": str(out),
                "format": fmt,
                "finished_at_utc": dt.datetime.now(dt.UTC).isoformat(),
            },
            indent=2,
            sort_keys=True,
        )
        + "\n"
    )
    return out, nrows, fmt


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--stage", required=True)
    ap.add_argument("--pointer", required=True)
    ap.add_argument("--planned-hats", default="/arc/projects/hats/splus/dr4")
    ap.add_argument("--ra-step", type=float, default=2.0)
    ap.add_argument("--ra-start", type=float, default=0.0)
    ap.add_argument("--ra-stop", type=float, default=360.0)
    args = ap.parse_args()

    stage = pathlib.Path(args.stage)
    shards = stage / "shards"
    shards.mkdir(parents=True, exist_ok=True)
    pointer = pathlib.Path(args.pointer)
    pointer.parent.mkdir(parents=True, exist_ok=True)

    total = 0
    strips = 0
    ra0 = args.ra_start
    while ra0 < args.ra_stop - 1e-9:
        ra1 = min(ra0 + args.ra_step, args.ra_stop)
        name = f"splus_ra_{ra0:06.2f}_{ra1:06.2f}".replace(".", "p")
        marker = shards / f"{name}.done.json"
        if marker.is_file():
            prev = json.loads(marker.read_text())
            total += int(prev.get("rows", 0))
            strips += 1
            print(f"skip {name} rows={prev.get('rows')}", flush=True)
            ra0 = ra1
            continue
        sql = f"SELECT {COLS} FROM splus_dr4.dual WHERE ra >= {ra0} AND ra < {ra1}"
        print(f"query {ra0}-{ra1} ...", flush=True)
        raw = tap_csv(sql)
        _, nrows, fmt = write_shard(
            shards,
            name,
            raw,
            {
                "ra0": ra0,
                "ra1": ra1,
                "table": "splus_dr4.dual",
                "source": "NOIRLab Data Lab TAP",
            },
        )
        total += nrows
        strips += 1
        print(f"wrote {name} rows={nrows} fmt={fmt} cumulative={total}", flush=True)
        ra0 = ra1

    now = dt.datetime.now(dt.UTC).isoformat()
    out = {
        "alias": "splus_dr4_hats",
        "survey": "splus",
        "release": "dr4",
        "source": "NOIRLab Data Lab TAP splus_dr4.dual (public)",
        "note": "Day-0 pin is public DR4; iDR5 / splus.cloud collab not used",
        "stage_root": str(stage),
        "planned_hats_path": args.planned_hats,
        "exists": True,
        "hats_ready": False,
        "official_mirror": False,
        "shard_mode": "ra_strip",
        "ra_step_deg": args.ra_step,
        "strips": strips,
        "nrows_staged": total,
        "expected_rows": EXPECTED_ROWS,
        "probed_at_utc": now,
    }
    pointer.write_text(json.dumps(out, indent=2, sort_keys=True) + "\n")
    print(
        f"done strips={strips} rows={total} expected={EXPECTED_ROWS} pointer={pointer}", flush=True
    )
    if abs(total - EXPECTED_ROWS) > max(10_000, EXPECTED_ROWS // 1000):
        raise SystemExit(f"FAIL row count {total} vs expected {EXPECTED_ROWS}")


if __name__ == "__main__":
    main()
