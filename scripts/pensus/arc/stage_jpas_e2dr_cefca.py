#!/usr/bin/env python3
"""Stage J-PAS E2DR MagABDualObj from CEFCA TAP into parquet/CSV shards.

Primary shard key: TILE_ID. Fallback: 5-degree ALPHA_J2000 strips. CEFCA CSV
responses start with #ADQL comment lines that must be stripped before DictReader.

MagABDualObj AB mags are multi-element vectors (narrow-band set); store
MAG_APER_3_0 / MAG_APER_COR_3_0 vectors as released.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import gzip
import io
import json
import pathlib
import time
import urllib.parse
import urllib.request

TAP = "https://archive.cefca.es/catalogues/vo/tap/jpas-e2dr/sync"
COLS = (
    "TILE_ID, NUMBER, ALPHA_J2000, DELTA_J2000, CLASS_STAR, FLAGS, MASK_FLAGS, "
    "MAG_APER_3_0, MAG_ERR_APER_3_0, MAG_APER_COR_3_0, MAG_ERR_APER_COR_3_0, "
    "MAG_AUTO, MAG_ERR_AUTO"
)


def tap_csv(query: str, timeout: int = 600) -> str:
    url = TAP + "?REQUEST=doQuery&LANG=ADQL&FORMAT=csv&QUERY=" + urllib.parse.quote(query)
    last: Exception | None = None
    for attempt in range(1, 6):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "pensus-stage/1.0"})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read().decode("utf-8", "replace")
        except Exception as exc:  # noqa: BLE001
            last = exc
            time.sleep(min(2**attempt, 30))
    raise RuntimeError(f"TAP failed after retries: {last}")


def csv_rows(raw: str) -> list[dict[str, str]]:
    if "<VOTABLE" in raw and "QUERY_STATUS" in raw and "ERROR" in raw:
        raise RuntimeError(f"TAP error VOTable: {raw[:500]}")
    lines = [ln for ln in raw.splitlines() if ln and not ln.startswith("#")]
    if not lines:
        return []
    return list(csv.DictReader(io.StringIO("\n".join(lines))))


def write_shard(
    shards: pathlib.Path, name: str, raw: str, extra: dict
) -> tuple[pathlib.Path, int, str]:
    marker = shards / f"{name}.done.json"
    # Strip CEFCA #ADQL comment lines before parquet conversion (else pyarrow
    # treats `#SELECT ...` as the header and corrupts column names).
    cleaned = "\n".join(ln for ln in raw.splitlines() if ln and not ln.startswith("#"))
    csv_path = shards / f"{name}.csv"
    csv_path.write_text(cleaned + ("\n" if cleaned else ""))
    rows = max(0, len(csv_rows(raw)))
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
                "rows": rows,
                "path": str(out),
                "format": fmt,
                "finished_at_utc": dt.datetime.now(dt.UTC).isoformat(),
            },
            indent=2,
            sort_keys=True,
        )
        + "\n"
    )
    return out, rows, fmt


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--stage", required=True)
    ap.add_argument("--pointer", required=True)
    ap.add_argument("--planned-hats", default="/arc/projects/hats/jpas/e2dr")
    ap.add_argument("--max-tiles", type=int, default=0)
    ap.add_argument("--ra-strip-deg", type=int, default=5)
    args = ap.parse_args()

    stage = pathlib.Path(args.stage)
    shards = stage / "shards"
    shards.mkdir(parents=True, exist_ok=True)
    pointer = pathlib.Path(args.pointer)
    pointer.parent.mkdir(parents=True, exist_ok=True)

    tiles_csv = tap_csv("SELECT DISTINCT TILE_ID FROM jpas.MagABDualObj")
    print(f"tile_query_head={tiles_csv[:240]!r}", flush=True)
    tiles = sorted(
        {
            int(float(val))
            for row in csv_rows(tiles_csv)
            if (val := row.get("TILE_ID") or row.get("tile_id")) not in (None, "")
        }
    )
    use_ra_strips = False
    if not tiles:
        use_ra_strips = True
        print("WARN empty tiles; falling back to RA strips", flush=True)
    if args.max_tiles > 0 and tiles:
        tiles = tiles[: args.max_tiles]
    print(f"tiles={len(tiles)} use_ra_strips={use_ra_strips}", flush=True)

    total_rows = 0
    done = 0
    n_units = 0

    if use_ra_strips:
        step = max(1, int(args.ra_strip_deg))
        units = list(range(0, 360, step))
        n_units = len(units)
        for ra0 in units:
            ra1 = ra0 + step
            name = f"ra_{ra0:03d}_{ra1:03d}"
            marker = shards / f"{name}.done.json"
            out_parq = shards / f"{name}.parquet"
            out_gz = shards / f"{name}.csv.gz"
            if marker.is_file() and (out_parq.is_file() or out_gz.is_file()):
                total_rows += int(json.loads(marker.read_text()).get("rows", 0))
                done += 1
                continue
            q = (
                f"SELECT {COLS} FROM jpas.MagABDualObj "
                f"WHERE ALPHA_J2000 >= {ra0} AND ALPHA_J2000 < {ra1}"
            )
            raw = tap_csv(q, timeout=900)
            if "QUERY_STATUS" in raw and "ERROR" in raw:
                raise RuntimeError(f"{name} TAP error: {raw[:500]}")
            _, rows, _ = write_shard(
                shards, name, raw, {"ra0": ra0, "ra1": ra1, "mode": "ra_strip"}
            )
            total_rows += rows
            done += 1
            if done % 12 == 0 or done == n_units:
                print(f"progress {done}/{n_units} rows={total_rows}", flush=True)
    else:
        n_units = len(tiles)
        for tid in tiles:
            name = f"tile_{tid}"
            marker = shards / f"{name}.done.json"
            out_parq = shards / f"{name}.parquet"
            out_gz = shards / f"{name}.csv.gz"
            if marker.is_file() and (out_parq.is_file() or out_gz.is_file()):
                total_rows += int(json.loads(marker.read_text()).get("rows", 0))
                done += 1
                continue
            raw = tap_csv(
                f"SELECT {COLS} FROM jpas.MagABDualObj WHERE TILE_ID = {tid}",
                timeout=900,
            )
            if "QUERY_STATUS" in raw and "ERROR" in raw:
                raise RuntimeError(f"tile {tid} TAP error: {raw[:500]}")
            _, rows, _ = write_shard(shards, name, raw, {"tile_id": tid, "mode": "tile"})
            total_rows += rows
            done += 1
            if done % 20 == 0 or done == n_units:
                print(f"progress {done}/{n_units} rows={total_rows}", flush=True)

    now = dt.datetime.now(dt.UTC).isoformat()
    pointer.write_text(
        json.dumps(
            {
                "alias": "jpas_e2dr_hats",
                "survey": "jpas",
                "release": "e2dr",
                "source": "CEFCA TAP jpas-e2dr / MagABDualObj",
                "note": "Narrow-band mag vectors; see jpas.Filter for band order",
                "stage_root": str(stage),
                "planned_hats_path": args.planned_hats,
                "nrows_staged": total_rows,
                "tiles": 0 if use_ra_strips else n_units,
                "ra_strips": n_units if use_ra_strips else 0,
                "shard_mode": "ra_strip" if use_ra_strips else "tile",
                "exists": True,
                "official_mirror": False,
                "hats_ready": False,
                "probed_at_utc": now,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n"
    )
    print(f"done rows={total_rows} units={n_units} pointer={pointer}", flush=True)


if __name__ == "__main__":
    main()
