#!/bin/bash
set -euo pipefail
# VVVX DR1: CSV.gz strips -> parquet -> HATS on unions (temp; no hats promote/ACL here).
KP=/arc/projects/k-pop/pensus
CODE=$KP/code
HOST=/arc/projects/unions
STAGE=$HOST/pensus/staging/vvvx_dr1
PARQ=$STAGE/parquet
BUILD=$HOST/pensus/hats_staging/vvvx/_build
DST=$HOST/pensus/hats_staging/vvvx/dr1
TMP=$HOST/pensus/hats_build/vvvx_dr1_tmp
PLANNED=/arc/projects/hats/vvvx/dr1
ALIAS=vvvx_dr1_hats
POINTER=$KP/inputs/$ALIAS/SOURCE.json
LOG=$KP/logs/vvvx_dr1_build_$(date -u +%Y%m%dT%H%M%SZ).log
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
STAGED_ROWS=$(python3 -c 'import json; print(json.load(open("'"$POINTER"'"))["nrows_staged"])')
echo "staged_rows_claim=$STAGED_ROWS"
MMAX=$(ceph_bytes /arc/projects/unions ceph.quota.max_bytes)
MUSED=$(ceph_bytes /arc/projects/unions ceph.dir.rbytes)
FREE=$((MMAX-MUSED))
echo "unions free_gib=$((FREE/2**30))"
# ~0.89B rows / ~27 GiB CSV; require ≥5 TiB free
(( FREE > 5000 * 2**30 )) || { echo FAIL unions free; exit 1; }

mkdir -p "$PARQ" "$BUILD" "$TMP" "$KP/inputs/$ALIAS" "$HOST/pensus/hats_staging/vvvx" "$HOST/pensus/hats_build"
python3 -m venv --system-site-packages /scratch/hatsenv
/scratch/hatsenv/bin/python -m pip install --quiet --upgrade pip
/scratch/hatsenv/bin/python -m pip install --quiet 'hats-import==0.11.1' polars pyarrow
PY=/scratch/hatsenv/bin/python

"$PY" "$CODE/ukidss_strips_to_parquet.py" --strips "$STAGE/strips" --out "$PARQ"
ROWS=$("$PY" -c 'import pathlib,pyarrow.parquet as pq
n=0
for p in pathlib.Path("'"$PARQ"'").glob("*.parquet"):
  n+=pq.read_metadata(p).num_rows
print(n)')
echo "parquet_rows=$ROWS staged_claim=$STAGED_ROWS"
"$PY" -c 'import sys
got, claim = int(sys.argv[1]), int(sys.argv[2])
if got <= 0: raise SystemExit("FAIL zero parquet rows")
if got > claim: raise SystemExit(f"FAIL parquet {got} > staged claim {claim}")
if claim - got > max(2000, claim // 10000):
    raise SystemExit(f"FAIL parquet {got} too far below staged claim {claim}")
print("row_check_ok", got, claim)' "$ROWS" "$STAGED_ROWS"

WORKERS=${WORKERS:-8}
MEMORY_LIMIT=${MEMORY_LIMIT:-24GB}
"$PY" "$CODE/hats_import_run.py" \
  --inputs "$PARQ/*.parquet" --reader parquet \
  --name dr1 --out "$BUILD" --tmp "$TMP" \
  --ra ra --dec dec --sort sourceid \
  --pixel-threshold 1000000 \
  --workers "$WORKERS" --memory-limit "$MEMORY_LIMIT"

N=$("$PY" -c "from pathlib import Path
p=next(q for q in (Path('$BUILD/dr1/hats.properties'), Path('$BUILD/dr1/properties')) if q.exists())
print(next(int(l.split('=',1)[1]) for l in p.read_text().splitlines() if l.startswith('hats_nrows=')))")
[[ "$N" == "$ROWS" ]] || { echo "FAIL hats_nrows $N != $ROWS"; exit 1; }
mv "$BUILD/dr1" "$DST"
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
  "survey": "vvvx",
  "release": "dr1",
  "source": "VSA VVVXDR1 vvvxSource seamless priOrSec (programmeID=170)",
  "nrows": int(rows),
  "partitions": int(parts),
  "bytes": int(nbytes),
  "temporary_host": True,
  "planned_hats_path": planned,
  "built_at_utc": now,
  "ra_column": "ra",
  "dec_column": "dec",
  "host": "/arc/projects/unions",
  "note": "Public ACL only after promote to /arc/projects/hats/vvvx/dr1",
}
pathlib.Path(dst, "PENSUS_BUILD.json").write_text(json.dumps(prov, indent=2, sort_keys=True) + "\n")
out = {
  **prev,
  "alias": "vvvx_dr1_hats",
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
rm -rf "$PARQ"
echo "done $(date -Is) DST=$DST rows=$ROWS parts=$PARTS bytes=$BYTES planned=$PLANNED"
echo "NOTE: no promote / no public ACL yet — temp host only"
