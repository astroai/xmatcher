#!/bin/bash
set -euo pipefail
# S-PLUS DR4: staged Data Lab dual parquet strips -> HATS on skyai (temp; promote separately or via wrapper).
KP=/arc/projects/k-pop/pensus
CODE=$KP/code
HOST=/arc/projects/skyai
STAGE=$HOST/pensus/staging/splus_dr4
SHARDS=$STAGE/shards
BUILD=$HOST/pensus/hats_staging/splus/_build
DST=$HOST/pensus/hats_staging/splus/dr4
TMP=$HOST/pensus/hats_build/splus_dr4_tmp
PLANNED=/arc/projects/hats/splus/dr4
ALIAS=splus_dr4_hats
POINTER=$KP/inputs/$ALIAS/SOURCE.json
LOG=$KP/logs/splus_dr4_build_$(date -u +%Y%m%dT%H%M%SZ).log
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
[[ -d "$SHARDS" ]] || { echo "FAIL missing shards $SHARDS"; exit 1; }
STAGED_ROWS=$(python3 -c 'import json; print(json.load(open("'"$POINTER"'"))["nrows_staged"])')
echo "staged_rows_claim=$STAGED_ROWS"
NPARQ=$(find "$SHARDS" -maxdepth 1 -name 'splus_ra_*.parquet' | wc -l)
echo "parquet_files=$NPARQ"
(( NPARQ >= 100 )) || { echo FAIL too few parquet shards; exit 1; }

MMAX=$(ceph_bytes /arc/projects/skyai ceph.quota.max_bytes)
MUSED=$(ceph_bytes /arc/projects/skyai ceph.dir.rbytes)
FREE=$((MMAX-MUSED))
echo "skyai free_gib=$((FREE/2**30))"
# ~80M rows: HATS out ~few×10 GiB; TMP 1.3× + 300 GiB floor → require ≥800 GiB free
(( FREE > 800 * 2**30 )) || { echo FAIL skyai free; exit 1; }

mkdir -p "$BUILD" "$TMP" "$KP/inputs/$ALIAS" "$HOST/pensus/hats_staging/splus" "$HOST/pensus/hats_build"
python3 -m venv --system-site-packages /scratch/hatsenv
/scratch/hatsenv/bin/python -m pip install --quiet --upgrade pip
/scratch/hatsenv/bin/python -m pip install --quiet 'hats-import==0.11.1' polars pyarrow
PY=/scratch/hatsenv/bin/python

ROWS=$("$PY" -c 'import pathlib,pyarrow.parquet as pq
n=0
for p in pathlib.Path("'"$SHARDS"'").glob("splus_ra_*.parquet"):
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
p=next(pathlib.Path("'"$SHARDS"'").glob("splus_ra_*.parquet"))
names=set(pq.read_schema(p).names)
need={"ra","dec"}
missing=need-names
if missing: raise SystemExit(f"FAIL missing columns {missing} have={sorted(names)[:40]}")
print("schema_ok", p.name, "ncols", len(names))'

WORKERS=${WORKERS:-8}
MEMORY_LIMIT=${MEMORY_LIMIT:-16GB}
"$PY" "$CODE/hats_import_run.py" \
  --inputs "$SHARDS/splus_ra_*.parquet" --reader parquet \
  --name dr4 --out "$BUILD" --tmp "$TMP" \
  --ra ra --dec dec --sort id \
  --pixel-threshold 1000000 \
  --workers "$WORKERS" --memory-limit "$MEMORY_LIMIT"

N=$("$PY" -c "from pathlib import Path
p=next(q for q in (Path('$BUILD/dr4/hats.properties'), Path('$BUILD/dr4/properties')) if q.exists())
print(next(int(l.split('=',1)[1]) for l in p.read_text().splitlines() if l.startswith('hats_nrows=')))")
[[ "$N" == "$ROWS" ]] || { echo "FAIL hats_nrows $N != $ROWS"; exit 1; }
mv "$BUILD/dr4" "$DST"
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
  "survey": "splus",
  "release": "dr4",
  "source": "NOIRLab Data Lab TAP splus_dr4.dual",
  "nrows": int(rows),
  "partitions": int(parts),
  "bytes": int(nbytes),
  "temporary_host": True,
  "planned_hats_path": planned,
  "built_at_utc": now,
  "ra_column": "ra",
  "dec_column": "dec",
  "host": "/arc/projects/skyai",
  "note": "Public ACL only after promote to /arc/projects/hats/splus/dr4",
}
pathlib.Path(dst, "PENSUS_BUILD.json").write_text(json.dumps(prov, indent=2, sort_keys=True) + "\n")
out = {
  **prev,
  "alias": "splus_dr4_hats",
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
echo "NOTE: temp host only until promote"
