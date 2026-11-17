#!/bin/bash
set -euo pipefail
# J-PLUS DR3: staged MagABDualObj parquet tiles -> HATS on mlao (temp; no hats promote/ACL here).
KP=/arc/projects/k-pop/pensus
CODE=$KP/code
HOST=/arc/projects/mlao
STAGE=$HOST/pensus/staging/jplus_dr3
SHARDS_RAW=$STAGE/shards
SHARDS=$STAGE/parquet_fixed
BUILD=$HOST/pensus/hats_staging/jplus/_build
DST=$HOST/pensus/hats_staging/jplus/dr3
TMP=$HOST/pensus/hats_build/jplus_dr3_tmp
PLANNED=/arc/projects/hats/jplus/dr3
ALIAS=jplus_dr3_hats
POINTER=$KP/inputs/$ALIAS/SOURCE.json
LOG=$KP/logs/jplus_dr3_build_$(date -u +%Y%m%dT%H%M%SZ).log
exec > >(tee -a "$LOG") 2>&1
echo "start $(date -Is) host=$(hostname) nproc=$(nproc)"

ceph_bytes() {
  python3 -c 'import os,subprocess,sys
path,attr=sys.argv[1],sys.argv[2]
try:
  print(int(os.getxattr(path,attr)))
except OSError:
  try:
    out=subprocess.check_output(["getfattr","--only-values","-n",attr,path],text=True).strip()
    print(int(out) if out else 0)
  except Exception:
    print(0)' "$1" "$2"
}

[[ -e "$DST" ]] && { echo "FAIL: $DST exists"; exit 1; }
[[ -d "$SHARDS_RAW" ]] || { echo "FAIL missing shards $SHARDS_RAW"; exit 1; }
STAGED_ROWS=$(python3 -c 'import json; print(json.load(open("'"$POINTER"'"))["nrows_staged"])')
echo "staged_rows_claim=$STAGED_ROWS"
NPARQ=$(find "$SHARDS_RAW" -maxdepth 1 -name 'tile_*.parquet' | wc -l)
echo "parquet_files_raw=$NPARQ"
(( NPARQ >= 1600 )) || { echo FAIL too few parquet shards; exit 1; }

MMAX=$(ceph_bytes /arc/projects/mlao ceph.quota.max_bytes)
MUSED=$(ceph_bytes /arc/projects/mlao ceph.dir.rbytes)
FREE=$((MMAX-MUSED))
echo "mlao free_gib=$((FREE/2**30))"
# ~16M rows; require ≥500 GiB free (TMP 1.3× + 300 GiB floor)
(( FREE > 500 * 2**30 )) || { echo FAIL mlao free; exit 1; }

mkdir -p "$BUILD" "$TMP" "$SHARDS" "$KP/inputs/$ALIAS" "$HOST/pensus/hats_staging/jplus" "$HOST/pensus/hats_build"
python3 -m venv --system-site-packages /scratch/hatsenv
/scratch/hatsenv/bin/python -m pip install --quiet --upgrade pip
/scratch/hatsenv/bin/python -m pip install --quiet 'hats-import==0.11.1' polars pyarrow
PY=/scratch/hatsenv/bin/python

"$PY" "$CODE/fix_cefca_magab_parquet.py" --shards "$SHARDS_RAW" --out "$SHARDS" --survey jplus

ROWS=$("$PY" -c 'import pathlib,pyarrow.parquet as pq
n=0
for p in pathlib.Path("'"$SHARDS"'").glob("tile_*.parquet"):
  n+=pq.read_metadata(p).num_rows
print(n)')
echo "parquet_rows=$ROWS staged_claim=$STAGED_ROWS"
"$PY" -c 'import sys
got, claim = int(sys.argv[1]), int(sys.argv[2])
if got <= 0: raise SystemExit("FAIL zero parquet rows")
if abs(got - claim) > max(5000, claim // 200):
    raise SystemExit(f"FAIL parquet {got} vs staged claim {claim}")
print("row_check_ok", got, claim)' "$ROWS" "$STAGED_ROWS"

"$PY" -c 'import pathlib,pyarrow.parquet as pq
p=next(pathlib.Path("'"$SHARDS"'").glob("tile_*.parquet"))
names=set(pq.read_schema(p).names)
need={"ALPHA_J2000","DELTA_J2000"}
missing=need-names
if missing: raise SystemExit(f"FAIL missing columns {missing} have={sorted(names)[:40]}")
print("schema_ok", p.name, "ncols", len(names))'

WORKERS=${WORKERS:-4}
MEMORY_LIMIT=${MEMORY_LIMIT:-12GB}
"$PY" "$CODE/hats_import_run.py" \
  --inputs "$SHARDS/tile_*.parquet" --reader parquet \
  --name dr3 --out "$BUILD" --tmp "$TMP" \
  --ra ALPHA_J2000 --dec DELTA_J2000 --sort NUMBER \
  --pixel-threshold 1000000 \
  --workers "$WORKERS" --memory-limit "$MEMORY_LIMIT"

N=$("$PY" -c "from pathlib import Path
p=next(q for q in (Path('$BUILD/dr3/hats.properties'), Path('$BUILD/dr3/properties')) if q.exists())
print(next(int(l.split('=',1)[1]) for l in p.read_text().splitlines() if l.startswith('hats_nrows=')))")
[[ "$N" == "$ROWS" ]] || { echo "FAIL hats_nrows $N != $ROWS"; exit 1; }
mv "$BUILD/dr3" "$DST"
rmdir "$BUILD" 2>/dev/null || true
rm -rf "$TMP"

PARTS=$("$PY" -c "import csv; print(sum(1 for r in csv.DictReader(open('$DST/partition_info.csv')) if r.get('Npix')))")
BYTES=$(du -sb "$DST" | cut -f1)
"$PY" - "$DST" "$POINTER" "$BYTES" "$ROWS" "$PARTS" "$PLANNED" <<'PY'
import datetime, json, pathlib, sys
dst, pointer, nbytes, rows, parts, planned = sys.argv[1:]
now = datetime.datetime.now(datetime.timezone.utc).isoformat()
prev = json.loads(pathlib.Path(pointer).read_text()) if pathlib.Path(pointer).exists() else {}
prov = {
  "survey": "jplus",
  "release": "dr3",
  "source": "CEFCA TAP jplus-dr3 / MagABDualObj tile parquet",
  "nrows": int(rows),
  "partitions": int(parts),
  "bytes": int(nbytes),
  "temporary_host": True,
  "planned_hats_path": planned,
  "built_at_utc": now,
  "ra_column": "ALPHA_J2000",
  "dec_column": "DELTA_J2000",
  "host": "/arc/projects/mlao",
  "note": "Public ACL only after promote to /arc/projects/hats/jplus/dr3",
}
pathlib.Path(dst, "PENSUS_BUILD.json").write_text(json.dumps(prov, indent=2, sort_keys=True) + "\n")
out = {
  **prev,
  "alias": "jplus_dr3_hats",
  "hats_path": dst,
  "temporary_hats_path": dst,
  "planned_hats_path": planned,
  "promoted_hats_path": planned,
  "official_mirror": False,
  "exists": True,
  "hats_ready": True,
  "nrows": int(rows),
  "build": prov,
  "probed_at_utc": now,
}
pathlib.Path(pointer).write_text(json.dumps(out, indent=2, sort_keys=True) + "\n")
print("wrote", pointer)
PY

"$PY" -m pip install --quiet astropy cdshealpix pyyaml
export PYTHONPATH="$CODE/vendor/xmatcher/src:$CODE${PYTHONPATH:+:$PYTHONPATH}"
"$PY" "$CODE/verify_hats_tree.py" "$DST"
"$PY" "$CODE/qa_hats_release.py" "$DST" "$ALIAS" "$ROWS" "$PARTS"
echo "done $(date -Is) DST=$DST rows=$ROWS parts=$PARTS bytes=$BYTES planned=$PLANNED"
echo "NOTE: no promote / no public ACL yet — temp host only"
