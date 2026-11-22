#!/bin/bash
# Hubble HSCv4: normalize null-typed mag columns, hats-import on mlao
# (exclude orphan 004–006), then promote to hats/hubble/hsc_v4 + ACL + deep QA.
# CephFS TMP only. Do NOT disturb eszzswpn (DES Y6 promote).
set -euo pipefail
KP=/arc/projects/k-pop/pensus
CODE=$KP/code
HOST=/arc/projects/mlao
STAGE=$HOST/pensus/staging/hubble_hsc_v4
SHARDS=$STAGE/shards
BUILD=$HOST/pensus/hats_staging/hubble/_build_hsc_v4
DST_TEMP=$HOST/pensus/hats_staging/hubble/hsc_v4
TMP=$HOST/pensus/hats_build/hubble_hsc_v4_tmp
PLANNED=/arc/projects/hats/hubble/hsc_v4
ALIAS=hubble_hsc_v4_hats
POINTER=$KP/inputs/$ALIAS/SOURCE.json
ORPHAN=hscv4_ra_004p00_006p00.parquet
EXPECTED_UNIQUE=139162343
LOG=$KP/logs/hubble_hsc_v4_build_promote_$(date -u +%Y%m%dT%H%M%SZ).log
exec > >(tee -a "$LOG") 2>&1
echo "start $(date -Is) host=$(hostname) nproc=$(nproc)"
echo "NOTE: leave eszzswpn alone; CephFS TMP=$TMP; exclude $ORPHAN"

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

[[ -d "$SHARDS" ]] || { echo "FAIL missing $SHARDS"; exit 1; }
[[ -e "$DST_TEMP" ]] && { echo "FAIL: $DST_TEMP exists"; exit 1; }
[[ -e "$PLANNED/properties" || -e "$PLANNED/hats.properties" ]] && { echo "FAIL: $PLANNED already live"; exit 1; }

MMAX=$(ceph_bytes /arc/projects/mlao ceph.quota.max_bytes)
MUSED=$(ceph_bytes /arc/projects/mlao ceph.dir.rbytes)
FREE=$((MMAX-MUSED))
echo "mlao free_gib=$((FREE/2**30))"
(( FREE > 400 * 2**30 )) || { echo FAIL mlao free for Hubble hats-import; exit 1; }

# Clean prior failed hats-import residue
rm -rf "$TMP" "$BUILD"
mkdir -p "$BUILD" "$TMP" "$HOST/pensus/hats_staging/hubble" "$HOST/pensus/hats_build" "$(dirname "$POINTER")"
export TMPDIR=$TMP

mapfile -t INPUTS < <(find "$SHARDS" -maxdepth 1 -name 'hscv4_ra_*.parquet' ! -name "$ORPHAN" | LC_ALL=C sort)
N=${#INPUTS[@]}
echo "input_parquet=$N orphan_excluded=$ORPHAN"
(( N >= 600 )) || { echo FAIL too few shards; exit 1; }

python3 -m venv --system-site-packages /scratch/hatsenv
/scratch/hatsenv/bin/python -m pip install --quiet --upgrade pip
/scratch/hatsenv/bin/python -m pip install --quiet 'hats-import==0.11.1' polars pyarrow hats
PY=/scratch/hatsenv/bin/python

echo "==== normalize null-typed mag columns $(date -Is) ===="
"$PY" "$CODE/normalize_parquet_null_columns.py" "$SHARDS" --exclude-name "$ORPHAN"

"$PY" - "$EXPECTED_UNIQUE" "${INPUTS[@]}" <<'PY'
import pathlib, sys
import pyarrow as pa
import pyarrow.parquet as pq

expected = int(sys.argv[1])
files = [pathlib.Path(p) for p in sys.argv[2:]]
schema = pq.read_schema(files[0])
names = set(schema.names)
need = {"MatchRA", "MatchDec", "MatchID"}
missing = need - names
if missing:
    raise SystemExit(f"FAIL missing columns {missing}; have={sorted(names)[:50]}")
# After normalize, no null types should remain on mag columns.
nullish = [f.name for f in schema if pa.types.is_null(f.type)]
# Spot-check a few historically null-heavy files
spot = []
for p in files:
    if "W3_F438W" in {f.name for f in pq.read_schema(p) if pa.types.is_null(f.type)}:
        spot.append(p.name)
    if len(spot) >= 3:
        break
if spot:
    raise SystemExit(f"FAIL still null-typed columns in {spot}")
rows = sum(pq.read_metadata(p).num_rows for p in files)
print(f"schema_ok ncols={len(names)} files={len(files)} parquet_rows={rows} expected_unique={expected}")
if rows != expected:
    raise SystemExit(f"FAIL parquet rows {rows} != expected unique {expected}")
open("/scratch/hubble_rowcount.txt", "w").write(str(rows))
PY
PARQUET_ROWS=$(cat /scratch/hubble_rowcount.txt)
echo "parquet_rows=$PARQUET_ROWS"

"$PY" - "$POINTER" "$STAGE" "$PLANNED" "$EXPECTED_UNIQUE" "$PARQUET_ROWS" "$N" <<'PY'
import datetime, json, pathlib, sys
p = pathlib.Path(sys.argv[1])
stage, planned = sys.argv[2], sys.argv[3]
expected, parquet_rows, nfiles = int(sys.argv[4]), int(sys.argv[5]), int(sys.argv[6])
d = json.loads(p.read_text()) if p.is_file() else {"alias": "hubble_hsc_v4_hats"}
d.update(
    alias="hubble_hsc_v4_hats",
    survey="hubble",
    release="hsc_v4",
    stage_root=stage,
    planned_hats_path=planned,
    expected_rows=expected,
    nrows_staged=expected,
    nrows=expected,
    parquet_rows_sum_excluding_orphan=parquet_rows,
    shard_parquet_files_excluding_orphan=nfiles,
    orphan_excluded="hscv4_ra_004p00_006p00.parquet",
    null_columns_normalized_to_float64=True,
    hats_ready=False,
    official_mirror=False,
    status="hatsifying",
    note=(
        "Unique CasJobs stage excluding orphan overlap strip 004–006. "
        "Null-typed mag columns normalized to float64 before hats-import."
    ),
    probed_at_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),
)
p.parent.mkdir(parents=True, exist_ok=True)
p.write_text(json.dumps(d, indent=2, sort_keys=True) + "\n")
print("updated", p, "nrows_staged=", expected)
PY

WORKERS=${WORKERS:-8}
MEMORY_LIMIT=${MEMORY_LIMIT:-14GB}
"$PY" "$CODE/hats_import_run.py" \
  --inputs "${INPUTS[@]}" \
  --reader parquet \
  --name hsc_v4 \
  --out "$BUILD" \
  --tmp "$TMP" \
  --ra MatchRA \
  --dec MatchDec \
  --sort MatchID \
  --pixel-threshold 1000000 \
  --workers "$WORKERS" \
  --memory-limit "$MEMORY_LIMIT"

if [[ -d "$BUILD/hsc_v4" ]]; then
  mv "$BUILD/hsc_v4" "$DST_TEMP"
elif [[ -f "$BUILD/properties" || -f "$BUILD/hats.properties" ]]; then
  mkdir -p "$(dirname "$DST_TEMP")"
  mv "$BUILD" "$DST_TEMP"
else
  FOUND=$(find "$BUILD" -maxdepth 2 \( -name properties -o -name hats.properties \) | head -1 || true)
  [[ -n "$FOUND" ]] || { echo FAIL no HATS root under "$BUILD"; ls -laR "$BUILD" | head -50; exit 1; }
  ROOT=$(dirname "$FOUND")
  mkdir -p "$(dirname "$DST_TEMP")"
  mv "$ROOT" "$DST_TEMP"
fi
test -f "$DST_TEMP/properties" -o -f "$DST_TEMP/hats.properties" || { echo FAIL bad temp HATS; exit 1; }

BYTES=$(du -sb "$DST_TEMP" | awk '{print $1}')
PARTS=$(python3 -c 'import pathlib; p=pathlib.Path("'"$DST_TEMP"'")/"partition_info.csv"; print(max(0,sum(1 for _ in open(p))-1) if p.is_file() else -1)')
ROWS_HAT=$("$PY" -c 'from hats import read_hats; t=read_hats("'"$DST_TEMP"'"); print(int(t.catalog_info.total_rows))' 2>/dev/null || echo "$EXPECTED_UNIQUE")
echo "temp_hats rows=$ROWS_HAT parts=$PARTS bytes=$BYTES"

"$PY" - "$POINTER" "$DST_TEMP" "$ROWS_HAT" "$PARTS" "$BYTES" <<'PY'
import datetime, json, pathlib, sys
p = pathlib.Path(sys.argv[1])
d = json.loads(p.read_text())
d.update(
    hats_path=sys.argv[2],
    temporary_hats_path=sys.argv[2],
    hats_ready=True,
    nrows=int(sys.argv[3]),
    partitions=int(sys.argv[4]),
    bytes=int(sys.argv[5]),
    temporary_host=True,
    status="temp_hats_ready",
    built_at_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),
)
p.write_text(json.dumps(d, indent=2, sort_keys=True) + "\n")
print("pointer temp ready", p)
PY

echo "==== PROMOTE Hubble HSCv4 $(date -Is) ===="
bash "$CODE/promote_hats_tree.sh" \
  "$DST_TEMP" \
  "$PLANNED" \
  "$ALIAS" \
  "$EXPECTED_UNIQUE" \
  "$PARTS"

rm -rf "$TMP" "$BUILD" 2>/dev/null || true
echo "all done Hubble HSCv4 $(date -Is) LOG=$LOG"
