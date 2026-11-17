"""Stage 2 of the Gaia DR3 XP continuous import: lay stage-1 Parquet out on the Gaia DR3 HATS partitions.

For each stage-1 file, ra/dec/_healpix_29 are joined from the Gaia catalogue on source_id and each row is
written to the Gaia partition that holds its source, as a fragment under `Npix=N/` (hats_npix_suffix=/).
The source_id HEALPix (level 12) picks the candidate partitions; sources not found there are looked up in
the partitions of the 8 neighbouring level-12 pixels. A source still unmatched fails the file. The stage-1
file is deleted only after its fragments are written and their row count equals its own.

Usage:
  xp_layout.py run <gaia_root> <stage1_dir> <out_root> <shard> <nshards> <workers>
  xp_layout.py finalize <gaia_root> <stage1_dir> <out_root> <expected_rows>
"""

from __future__ import annotations

import concurrent.futures as cf
import csv
import datetime
import functools
import json
import sys
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from cdshealpix.nested import neighbours

sys.path.insert(0, str(Path(__file__).resolve().parent))
import xp_convert  # noqa: E402

POS_COLS = ["source_id", "ra", "dec", "_healpix_29"]


class Partitions:
    def __init__(self, gaia_root: Path):
        self.root = gaia_root
        with open(gaia_root / "partition_info.csv") as fh:
            self.parts = sorted((int(r["Norder"]), int(r["Npix"])) for r in csv.DictReader(fh))
        self.orders = sorted({o for o, _ in self.parts})
        self.by_order = {o: {p for oo, p in self.parts if oo == o} for o in self.orders}

    def containing(self, hp12: np.ndarray) -> list[tuple[int, int]]:
        out: set[tuple[int, int]] = set()
        for o in self.orders:
            parent = np.unique(hp12 >> (2 * (12 - o)))
            out.update((o, int(p)) for p in parent if int(p) in self.by_order[o])
        return sorted(out)

    def path(self, order: int, pix: int) -> Path:
        return (
            self.root
            / "dataset"
            / f"Norder={order}"
            / f"Dir={pix // 10000 * 10000}"
            / f"Npix={pix}.parquet"
        )


@functools.lru_cache(maxsize=64)
def _positions(gaia_root: str, order: int, pix: int) -> pa.Table:
    p = Partitions(Path(gaia_root)).path(order, pix)
    t = pq.read_table(p, columns=POS_COLS)
    return t.append_column("_norder", pa.array(np.full(t.num_rows, order, np.int8))).append_column(
        "_npix", pa.array(np.full(t.num_rows, pix, np.int64))
    )


def _join(xp: pa.Table, gaia_root: Path, parts: Partitions, hp12: np.ndarray) -> pa.Table:
    """Return xp with ra/dec/_healpix_29/_norder/_npix appended; rows without a Gaia match are dropped."""
    keys = pa.table(
        {"source_id": xp["source_id"], "_row": pa.array(np.arange(xp.num_rows, dtype=np.int64))}
    )
    cands = parts.containing(hp12)
    pos = pa.concat_tables([_positions(str(gaia_root), o, p) for o, p in cands])
    matched = keys.join(pos, "source_id", join_type="inner")
    if matched.num_rows < xp.num_rows:
        rest = keys.filter(pc.invert(pc.is_in(keys["source_id"], matched["source_id"])))
        rhp = pc.shift_right(rest["source_id"], 35).to_numpy()
        nb = neighbours(np.unique(rhp).astype(np.uint64), 12)
        nb = np.unique(nb[nb >= 0]).astype(np.int64)
        extra = [c for c in parts.containing(nb) if c not in cands]
        if extra:
            pos2 = pa.concat_tables([_positions(str(gaia_root), o, p) for o, p in extra])
            matched = pa.concat_tables([matched, rest.join(pos2, "source_id", join_type="inner")])
    matched = matched.sort_by("_row")
    out = xp.take(matched["_row"])
    for c in ("ra", "dec", "_healpix_29", "_norder", "_npix"):
        out = out.append_column(c, matched[c])
    return out


def layout_one(gaia_root: str, stage1: str, out_root: str, name: str) -> dict:
    gaia, s1, out = Path(gaia_root), Path(stage1), Path(out_root)
    marker = out / "_done" / f"{name}.json"
    if marker.exists():
        return json.loads(marker.read_text())
    src = s1 / f"{name}.parquet"
    xp = pq.read_table(src)
    parts = Partitions(gaia)
    hp12 = pc.shift_right(xp["source_id"], 35).to_numpy()
    joined = _join(xp, gaia, parts, hp12)
    if (
        joined.num_rows != xp.num_rows
        or pc.count_distinct(joined["source_id"]).as_py() != xp.num_rows
    ):
        raise RuntimeError(f"{name}: joined {joined.num_rows} of {xp.num_rows} rows")
    lead = ["source_id", "ra", "dec", "_healpix_29"]
    cols = lead + [c for c in xp.column_names if c != "source_id"]
    written, frags = 0, []
    keys = (
        pa.table({"o": joined["_norder"], "p": joined["_npix"]})
        .group_by(["o", "p"])
        .aggregate([])
        .to_pylist()
    )
    for k in keys:
        o, p = int(k["o"]), int(k["p"])
        sel = joined.filter(pc.and_(pc.equal(joined["_norder"], o), pc.equal(joined["_npix"], p)))
        sel = sel.select(cols).sort_by("_healpix_29")
        d = out / "dataset" / f"Norder={o}" / f"Dir={p // 10000 * 10000}" / f"Npix={p}"
        d.mkdir(parents=True, exist_ok=True)
        f = d / f"{name}.parquet"
        tmp = f.with_suffix(".parquet.partial")
        xp_convert.write(sel, tmp)
        tmp.rename(f)
        written += pq.read_metadata(f).num_rows
        frags.append([o, p, sel.num_rows])
    if written != xp.num_rows:
        raise RuntimeError(f"{name}: wrote {written} of {xp.num_rows}")
    rec = {"file": name, "rows": xp.num_rows, "fragments": frags}
    marker.write_text(json.dumps(rec) + "\n")
    src.unlink()
    return rec


def run(gaia_root: str, stage1: str, out_root: str, shard: int, nshards: int, workers: int) -> int:
    out = Path(out_root)
    (out / "_done").mkdir(parents=True, exist_ok=True)
    names = sorted(p.name.removesuffix(".json") for p in (Path(stage1) / "_done").glob("*.json"))
    names = [n.removesuffix(".csv.gz") for n in names]
    mine = [n for i, n in enumerate(names) if i % nshards == shard]
    todo = [n for n in mine if not (out / "_done" / f"{n}.json").exists()]
    print(f"shard {shard}/{nshards}: {len(mine)} staged files, {len(todo)} to lay out", flush=True)
    failed = 0
    with cf.ProcessPoolExecutor(workers) as ex:
        futs = {ex.submit(layout_one, gaia_root, stage1, out_root, n): n for n in todo}
        for f in cf.as_completed(futs):
            try:
                r = f.result()
                print(
                    json.dumps(
                        {"file": r["file"], "rows": r["rows"], "nfrag": len(r["fragments"])}
                    ),
                    flush=True,
                )
            except Exception as exc:  # noqa: BLE001
                failed += 1
                print(f"FAIL {futs[f]} {exc!r}", flush=True)
    print(f"shard {shard}: failed {failed}", flush=True)
    return 1 if failed else 0


def finalize(gaia_root: str, stage1: str, out_root: str, expected_rows: int) -> int:
    out = Path(out_root)
    md5 = [
        ln.split()[1].removesuffix(".csv.gz")
        for ln in (Path(stage1).parent / "_MD5SUM.txt").read_text().splitlines()
        if ln.strip()
    ]
    done = {
        p.name.removesuffix(".json"): json.loads(p.read_text())
        for p in (out / "_done").glob("*.json")
    }
    missing = sorted(set(md5) - set(done))
    counts: dict[tuple[int, int], int] = {}
    for rec in done.values():
        for o, p, n in rec["fragments"]:
            counts[(o, p)] = counts.get((o, p), 0) + n
    total = sum(counts.values())
    footer = 0
    for (o, p), n in counts.items():
        d = out / "dataset" / f"Norder={o}" / f"Dir={p // 10000 * 10000}" / f"Npix={p}"
        ids = pa.concat_arrays(
            [
                pq.read_table(f, columns=["source_id"])["source_id"].combine_chunks()
                for f in sorted(d.glob("*.parquet"))
            ]
        )
        if len(ids) != n or pc.count_distinct(ids).as_py() != n:
            print(f"FAIL partition {o}/{p}: {len(ids)} rows, expected {n} unique")
            return 1
        footer += n
    ok = not missing and total == expected_rows == footer
    summary = {
        "files_expected": len(md5),
        "files_done": len(done),
        "missing": missing[:10],
        "rows": total,
        "expected_rows": expected_rows,
        "partitions": len(counts),
        "ok": ok,
    }
    print(json.dumps(summary, indent=1))
    if not ok:
        return 1
    with open(out / "partition_info.csv", "w") as fh:
        fh.write("Norder,Npix\n" + "".join(f"{o},{p}\n" for o, p in sorted(counts)))
    gprops = dict(
        ln.split("=", 1)
        for ln in (Path(gaia_root) / "properties").read_text().splitlines()
        if "=" in ln and not ln.startswith("#")
    )
    props = {
        "obs_collection": "gaia_dr3_xp_continuous",
        "dataproduct_type": "object",
        "hats_col_ra": "ra",
        "hats_col_dec": "dec",
        "hats_npix_suffix": "/",
        "hats_nrows": str(total),
        "hats_order": str(max(o for o, _ in counts)),
        "hats_cols_survey_id": "source_id",
        "hats_cols_sort": "_healpix_29",
        "hats_builder": "pensus xp_layout (ESA CDN xp_continuous_mean_spectrum, positions from gaia/dr3)",
        "hats_creation_date": datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%dT%H:%MUTC"),
        "prov_progenitor": "https://cdn.gea.esac.esa.int/Gaia/gdr3/Spectroscopy/xp_continuous_mean_spectrum/",
        "pensus_position_source_creation_date": gprops.get("hats_creation_date", ""),
    }
    (out / "hats.properties").write_text(
        "# Gaia DR3 XP continuous mean spectra, on Gaia DR3 partitions\n"
        + "".join(f"{k}={v}\n" for k, v in props.items())
    )
    print("wrote partition_info.csv and hats.properties")
    return 0


if __name__ == "__main__":
    cmd, *a = sys.argv[1:]
    if cmd == "run":
        raise SystemExit(run(a[0], a[1], a[2], int(a[3]), int(a[4]), int(a[5])))
    if cmd == "finalize":
        raise SystemExit(finalize(a[0], a[1], a[2], int(a[3])))
    raise SystemExit(f"unknown command {cmd}")
