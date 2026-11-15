#!/bin/bash
set -euo pipefail
KP=/arc/projects/k-pop/pensus
CODE=$KP/code
HOST=/arc/projects/mlao
STAGE=$HOST/pensus/staging/2mass_xsc
BUILD=$HOST/pensus/hats_staging/2mass/_build
DST=$HOST/pensus/hats_staging/2mass/xsc
TMP=$HOST/pensus/hats_build/2mass_xsc_tmp
PLANNED=/arc/projects/hats/2mass/xsc
ALIAS=twomass_xsc_hats
ROWS=1647599
LOG=$KP/logs/2mass_xsc_build_$(date -u +%Y%m%dT%H%M%SZ).log
exec > >(tee -a "$LOG") 2>&1
echo "start $(date -Is) host=$(hostname)"
[[ -e "$DST" ]] && { echo "FAIL: $DST exists"; exit 1; }
mkdir -p "$STAGE" "$BUILD" "$TMP" "$KP/inputs/$ALIAS"
MMAX=$(getfattr --only-values -n ceph.quota.max_bytes /arc/projects/mlao)
MUSED=$(getfattr --only-values -n ceph.dir.rbytes /arc/projects/mlao)
echo "mlao free=$((MMAX-MUSED))"
(( MMAX - MUSED > 400 * 2**30 )) || { echo FAIL mlao free; exit 1; }
BASE=https://irsa.ipac.caltech.edu/2MASS/download/allsky
for f in xsc_aaa.gz xsc_baa.gz; do
  if [[ ! -f "$STAGE/$f" ]]; then
    curl -fL --retry 8 --retry-all-errors -o "$STAGE/$f.partial" "$BASE/$f"
    mv "$STAGE/$f.partial" "$STAGE/$f"
  fi
  ls -la "$STAGE/$f"
done
python3 -m venv --system-site-packages /scratch/hatsenv
/scratch/hatsenv/bin/python -m pip install --quiet --upgrade pip
/scratch/hatsenv/bin/python -m pip install --quiet 'hats-import==0.11.1' polars pyarrow
PY=/scratch/hatsenv/bin/python
"$PY" - "$STAGE" <<'PY'
import gzip, pathlib, sys
import pyarrow as pa
import pyarrow.parquet as pq
stage = pathlib.Path(sys.argv[1])
out = stage / "xsc.parquet"
schema = pa.schema([("designation", pa.string()), ("ra", pa.float64()), ("dec", pa.float64())])
writer = pq.ParquetWriter(out, schema, compression="zstd", compression_level=3)
batch_des, batch_ra, batch_dec = [], [], []
rows = 0

def flush():
    global rows, batch_des, batch_ra, batch_dec
    if not batch_des:
        return
    writer.write_batch(pa.RecordBatch.from_arrays([
        pa.array(batch_des, pa.string()),
        pa.array(batch_ra, pa.float64()),
        pa.array(batch_dec, pa.float64()),
    ], schema=schema))
    rows += len(batch_des)
    batch_des, batch_ra, batch_dec = [], [], []

for f in sorted(stage.glob("xsc_*.gz")):
    with gzip.open(f, "rt", errors="replace") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            p = [c.strip() for c in line.split("|")]
            if p and p[0] == "":
                p = p[1:]
            # IRSA All-Sky XSC: jdate | designation | ra | dec | ...
            if len(p) < 4:
                continue
            des, ra_s, dec_s = p[1], p[2], p[3]
            if not ra_s or not dec_s or ra_s == r"\N" or dec_s == r"\N":
                continue
            batch_des.append(des)
            batch_ra.append(float(ra_s))
            batch_dec.append(float(dec_s))
            if len(batch_des) >= 200_000:
                flush()
flush()
writer.close()
print(f"wrote {out} rows={rows}")
if rows != 1647599:
    raise SystemExit(f"FAIL rows {rows} != 1647599")
PY

"$PY" "$CODE/hats_import_run.py" --inputs "$STAGE/xsc.parquet" --reader parquet \
  --name xsc --out "$BUILD" --tmp "$TMP" --ra ra --dec dec --sort designation --workers 4
N=$("$PY" -c "from pathlib import Path; p=next(q for q in (Path('$BUILD/xsc/hats.properties'), Path('$BUILD/xsc/properties')) if q.exists()); print(next(int(l.split('=',1)[1]) for l in p.read_text().splitlines() if l.startswith('hats_nrows=')))")
[[ "$N" == "$ROWS" ]] || { echo "FAIL nrows $N"; exit 1; }
mv "$BUILD/xsc" "$DST"
rmdir "$BUILD" 2>/dev/null || true
rm -rf "$TMP"
PARTS=$("$PY" -c "import csv; print(sum(1 for r in csv.DictReader(open('$DST/partition_info.csv')) if r.get('Npix')))")
BYTES=$(du -sb "$DST" | cut -f1)
"$PY" - "$DST" "$KP/inputs/$ALIAS/SOURCE.json" "$BYTES" "$ROWS" "$PARTS" "$PLANNED" <<'PY'
import datetime, json, pathlib, sys
dst, pointer, nbytes, rows, parts, planned = sys.argv[1:]
now = datetime.datetime.now(datetime.timezone.utc).isoformat()
prov = {
  "survey":"2mass","release":"xsc",
  "source":"https://irsa.ipac.caltech.edu/2MASS/download/allsky/",
  "input":"xsc_aaa.gz + xsc_baa.gz (pipe: jdate|designation|ra|dec|...)",
  "nrows": int(rows), "partitions": int(parts), "bytes": int(nbytes),
  "temporary_host": True, "planned_hats_path": planned,
  "built_at_utc": now, "ra_column":"ra","dec_column":"dec",
}
pathlib.Path(dst,"PENSUS_BUILD.json").write_text(json.dumps(prov, indent=2, sort_keys=True)+"\n")
pathlib.Path(pointer).write_text(json.dumps({
  "survey":"twomass_xsc","alias":"twomass_xsc_hats",
  "hats_path": dst, "temporary_hats_path": dst,
  "planned_hats_path": planned, "promoted_hats_path": planned,
  "official_mirror": False, "exists": True, "nrows": int(rows),
  "build": prov, "probed_at_utc": now,
}, indent=2, sort_keys=True)+"\n")
print("wrote", pointer)
PY
export PYTHONPATH="$CODE/vendor/xmatcher/src:$CODE"
"$PY" -m pip install --quiet astropy cdshealpix pyyaml
"$PY" "$CODE/verify_hats_tree.py" "$DST"
"$PY" "$CODE/qa_hats_release.py" "$DST" "$ALIAS" "$ROWS" "$PARTS"
echo "done $(date -Is) DST=$DST planned=$PLANNED"
