#!/bin/bash
set -euo pipefail
# SDSS DR16 stargal-primary parquet -> TEMP HATS on skyai.
# Pin remains /arc/projects/hats/sdss/dr16 — DO NOT promote in this job.
# CephFS TMP only (never /scratch for hats-import tmp/dask).
# Do not disturb y0k6nhqc (HSC) / n662j3fl (ATLAS).
KP=/arc/projects/k-pop/pensus
CODE=$KP/code
HOST=/arc/projects/skyai
STAGE=$HOST/pensus/staging/sdss_dr16_sweeps
PARQ=$STAGE/parquet
BUILD=$HOST/pensus/hats_staging/sdss/_build_dr16
DST=$HOST/pensus/hats_staging/sdss/dr16
TMP=$HOST/pensus/hats_build/sdss_dr16_tmp
PLANNED=/arc/projects/hats/sdss/dr16
ALIAS=sdss_dr16_hats
POINTER=$KP/inputs/$ALIAS/SOURCE.json
LOG=$KP/logs/sdss_dr16_build_$(date -u +%Y%m%dT%H%M%SZ).log
exec > >(tee -a "$LOG") 2>&1
echo "start $(date -Is) host=$(hostname) nproc=$(nproc) WORKERS=${WORKERS:-} MEMORY_LIMIT=${MEMORY_LIMIT:-}"
echo "NOTE: temp HATS only; no promote to $PLANNED; leave y0k6nhqc n662j3fl alone"

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
[[ -d "$PARQ" ]] || { echo "FAIL missing parquet $PARQ"; exit 1; }
STAGED_ROWS=$(python3 -c 'import json; print(json.load(open("'"$POINTER"'"))["nrows_staged"])')
NPARQ=$(find "$PARQ" -maxdepth 1 -name '*.parquet' | wc -l)
echo "parquet_files=$NPARQ staged_rows_claim=$STAGED_ROWS"
(( NPARQ >= 3700 )) || { echo FAIL too few parquet shards; exit 1; }

MMAX=$(ceph_bytes /arc/projects/skyai ceph.quota.max_bytes)
MUSED=$(ceph_bytes /arc/projects/skyai ceph.dir.rbytes)
FREE=$((MMAX-MUSED))
echo "skyai free_gib=$((FREE/2**30))"
# HATS + TMP 1.3× + 300 GiB floor → require ≥900 GiB free
(( FREE > 900 * 2**30 )) || { echo FAIL skyai free for SDSS DR16 hats-import; exit 1; }

mkdir -p "$BUILD" "$TMP" "$KP/inputs/$ALIAS" "$HOST/pensus/hats_staging/sdss" "$HOST/pensus/hats_build"
# Keep hats-import / dask spill on CephFS
export TMPDIR=$TMP
python3 -m venv --system-site-packages /scratch/hatsenv
/scratch/hatsenv/bin/python -m pip install --quiet --upgrade pip
/scratch/hatsenv/bin/python -m pip install --quiet 'hats-import==0.11.1' polars pyarrow
PY=/scratch/hatsenv/bin/python

ROWS=$("$PY" -c 'import pathlib,pyarrow.parquet as pq
n=0
for p in pathlib.Path("'"$PARQ"'").glob("*.parquet"):
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
p=next(pathlib.Path("'"$PARQ"'").glob("*.parquet"))
names=set(pq.read_schema(p).names)
need={"RA","DEC"}
missing=need-names
if missing: raise SystemExit(f"FAIL missing columns {missing} have={sorted(names)[:40]}")
print("schema_ok", p.name, "ncols", len(names), "has_id", "id" in names or "THING_ID" in names)'

SORT_COL=$("$PY" -c 'import pathlib,pyarrow.parquet as pq
p=next(pathlib.Path("'"$PARQ"'").glob("*.parquet"))
names=set(pq.read_schema(p).names)
print("id" if "id" in names else ("THING_ID" if "THING_ID" in names else ""))')
[[ -n "$SORT_COL" ]] || { echo FAIL no sort column; exit 1; }
echo "sort_column=$SORT_COL"

WORKERS=${WORKERS:-8}
MEMORY_LIMIT=${MEMORY_LIMIT:-14GB}
"$PY" "$CODE/hats_import_run.py" \
  --inputs "$PARQ/*.parquet" --reader parquet \
  --name dr16 --out "$BUILD" --tmp "$TMP" \
  --ra RA --dec DEC --sort "$SORT_COL" \
  --pixel-threshold 1000000 \
  --workers "$WORKERS" --memory-limit "$MEMORY_LIMIT"

N=$("$PY" -c "from pathlib import Path
p=next(q for q in (Path('$BUILD/dr16/hats.properties'), Path('$BUILD/dr16/properties')) if q.exists())
print(next(int(l.split('=',1)[1]) for l in p.read_text().splitlines() if l.startswith('hats_nrows=')))")
[[ "$N" == "$ROWS" ]] || { echo "FAIL hats_nrows $N != $ROWS"; exit 1; }
mv "$BUILD/dr16" "$DST"
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
  "survey": "sdss",
  "release": "dr16",
  "source": "https://dr16.sdss.org/sas/dr16/eboss/sweeps/dr13_final/301/ calibObj-*-stargal-primary",
  "nrows": int(rows),
  "partitions": int(parts),
  "bytes": int(nbytes),
  "temporary_host": True,
  "planned_hats_path": planned,
  "built_at_utc": now,
  "ra_column": "RA",
  "dec_column": "DEC",
  "host": "/arc/projects/skyai",
  "note": "TEMP HATS only; public ACL / promote to /arc/projects/hats/sdss/dr16 only after deep QA + headroom",
}
pathlib.Path(dst, "PENSUS_BUILD.json").write_text(json.dumps(prov, indent=2, sort_keys=True) + "\n")
out = {
  **prev,
  "alias": "sdss_dr16_hats",
  "hats_path": dst,
  "temporary_hats_path": dst,
  "planned_hats_path": planned,
  "promoted_hats_path": planned,
  "official_mirror": False,
  "exists": True,
  "hats_ready": True,
  "nrows": int(rows),
  "nrows_staged": int(rows),
  "build": prov,
  "probed_at_utc": now,
  "note": "TEMP HATS on skyai; do not promote until deep QA + hats headroom",
}
pathlib.Path(pointer).write_text(json.dumps(out, indent=2, sort_keys=True) + "\n")
print("wrote", pointer)
PY

"$PY" -m pip install --quiet astropy cdshealpix pyyaml
export PYTHONPATH="$CODE/vendor/xmatcher/src:$CODE${PYTHONPATH:+:$PYTHONPATH}"
"$PY" "$CODE/verify_hats_tree.py" "$DST"
"$PY" "$CODE/qa_hats_release.py" "$DST" "$ALIAS" "$ROWS" "$PARTS"
echo "done $(date -Is) DST=$DST rows=$ROWS parts=$PARTS bytes=$BYTES planned=$PLANNED"
echo "NOTE: temp host only — NO promote to $PLANNED in this job"
