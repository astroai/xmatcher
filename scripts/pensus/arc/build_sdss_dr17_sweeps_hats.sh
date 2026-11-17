#!/bin/bash
set -euo pipefail
# SDSS DR17 stargal-primary parquet -> HATS on skyai (temp; promote separately or via wrapper).
KP=/arc/projects/k-pop/pensus
CODE=$KP/code
HOST=/arc/projects/skyai
STAGE=$HOST/pensus/staging/sdss_dr17_sweeps
PARQ=$STAGE/parquet
BUILD=$HOST/pensus/hats_staging/sdss/_build
DST=$HOST/pensus/hats_staging/sdss/dr17_sweeps
TMP=$HOST/pensus/hats_build/sdss_dr17_sweeps_tmp
PLANNED=/arc/projects/hats/sdss/dr17_sweeps
ALIAS=sdss_dr17_sweeps_hats
POINTER=$KP/inputs/$ALIAS/SOURCE.json
LOG=$KP/logs/sdss_dr17_build_$(date -u +%Y%m%dT%H%M%SZ).log
exec > >(tee -a "$LOG") 2>&1
echo "start $(date -Is) host=$(hostname) nproc=$(nproc) WORKERS=${WORKERS:-} MEMORY_LIMIT=${MEMORY_LIMIT:-}"

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
# HATS ~0.1–0.2 TiB; TMP 1.3× + 300 GiB floor → require ≥900 GiB free after stage
(( FREE > 900 * 2**30 )) || { echo FAIL skyai free for SDSS hats-import; exit 1; }

mkdir -p "$BUILD" "$TMP" "$KP/inputs/$ALIAS" "$HOST/pensus/hats_staging/sdss" "$HOST/pensus/hats_build"
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

SORT_COL=id
"$PY" -c 'import pathlib,pyarrow.parquet as pq,sys
p=next(pathlib.Path("'"$PARQ"'").glob("*.parquet"))
names=set(pq.read_schema(p).names)
print("id" if "id" in names else ("THING_ID" if "THING_ID" in names else ""))' > /scratch/sdss_sort_col.txt
SORT_COL=$(cat /scratch/sdss_sort_col.txt)
[[ -n "$SORT_COL" ]] || { echo FAIL no sort column; exit 1; }

WORKERS=${WORKERS:-8}
MEMORY_LIMIT=${MEMORY_LIMIT:-14GB}
"$PY" "$CODE/hats_import_run.py" \
  --inputs "$PARQ/*.parquet" --reader parquet \
  --name dr17_sweeps --out "$BUILD" --tmp "$TMP" \
  --ra RA --dec DEC --sort "$SORT_COL" \
  --pixel-threshold 1000000 \
  --workers "$WORKERS" --memory-limit "$MEMORY_LIMIT"

N=$("$PY" -c "from pathlib import Path
p=next(q for q in (Path('$BUILD/dr17_sweeps/hats.properties'), Path('$BUILD/dr17_sweeps/properties')) if q.exists())
print(next(int(l.split('=',1)[1]) for l in p.read_text().splitlines() if l.startswith('hats_nrows=')))")
[[ "$N" == "$ROWS" ]] || { echo "FAIL hats_nrows $N != $ROWS"; exit 1; }
mv "$BUILD/dr17_sweeps" "$DST"
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
  "release": "dr17_sweeps",
  "source": "https://dr17.sdss.org/sas/dr17/eboss/sweeps/dr13_final/301/ calibObj-*-stargal-primary",
  "nrows": int(rows),
  "partitions": int(parts),
  "bytes": int(nbytes),
  "temporary_host": True,
  "planned_hats_path": planned,
  "built_at_utc": now,
  "ra_column": "RA",
  "dec_column": "DEC",
  "host": "/arc/projects/skyai",
  "note": "Public ACL only after promote to /arc/projects/hats/sdss/dr17_sweeps",
}
pathlib.Path(dst, "PENSUS_BUILD.json").write_text(json.dumps(prov, indent=2, sort_keys=True) + "\n")
out = {
  **prev,
  "alias": "sdss_dr17_sweeps_hats",
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
