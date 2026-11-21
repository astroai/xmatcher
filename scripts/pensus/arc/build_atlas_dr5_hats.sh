#!/bin/bash
set -euo pipefail
# ATLAS DR5/ER5: staged ESO multi-band FITS tiles -> parquet -> HATS on skyai (temp).
KP=/arc/projects/k-pop/pensus
CODE=$KP/code
HOST=/arc/projects/skyai
STAGE=$HOST/pensus/staging/atlas_dr5_eso
TILES=$STAGE/tiles
PARQ=$STAGE/parquet
BUILD=$HOST/pensus/hats_staging/atlas/_build
DST=$HOST/pensus/hats_staging/atlas/dr5
TMP=$HOST/pensus/hats_build/atlas_dr5_tmp
PLANNED=/arc/projects/hats/atlas/dr5
ALIAS=atlas_dr5_hats
POINTER=$KP/inputs/$ALIAS/SOURCE.json
EXPECTED_ROWS=${EXPECTED_ROWS:-163755167}
LOG=$KP/logs/atlas_dr5_build_$(date -u +%Y%m%dT%H%M%SZ).log
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
[[ -d "$TILES" ]] || { echo "FAIL missing tiles $TILES"; exit 1; }
NFITS=$(find "$TILES" -maxdepth 1 \( -name '*.fits' -o -name '*.fits.gz' \) | wc -l)
echo "fits_tiles=$NFITS"
(( NFITS >= 5000 )) || { echo FAIL too few FITS tiles; exit 1; }

MMAX=$(ceph_bytes /arc/projects/skyai ceph.quota.max_bytes)
MUSED=$(ceph_bytes /arc/projects/skyai ceph.dir.rbytes)
FREE=$((MMAX-MUSED))
echo "skyai free_gib=$((FREE/2**30))"
# FITS ~118 GiB + parquet + HATS TMP 1.3× + 300 GiB floor
(( FREE > 1500 * 2**30 )) || { echo FAIL skyai free; exit 1; }

mkdir -p "$BUILD" "$TMP" "$PARQ" "$KP/inputs/$ALIAS" "$HOST/pensus/hats_staging/atlas" "$HOST/pensus/hats_build"
python3 -m venv --system-site-packages /scratch/hatsenv
/scratch/hatsenv/bin/python -m pip install --quiet --upgrade pip
/scratch/hatsenv/bin/python -m pip install --quiet 'hats-import==0.11.1' polars pyarrow astropy
PY=/scratch/hatsenv/bin/python

echo "converting FITS -> parquet"
"$PY" "$CODE/atlas_fits_to_parquet.py" --tiles "$TILES" --out "$PARQ"

NPARQ=$(find "$PARQ" -maxdepth 1 -name '*.parquet' ! -name 'CONVERT*' | wc -l)
echo "parquet_files=$NPARQ"
(( NPARQ >= 5000 )) || { echo FAIL too few parquet shards; exit 1; }

ROWS=$("$PY" -c 'import pathlib,pyarrow.parquet as pq
n=0
for p in pathlib.Path("'"$PARQ"'").glob("*.parquet"):
  if p.name.startswith("CONVERT"): continue
  n+=pq.read_metadata(p).num_rows
print(n)')
echo "parquet_rows=$ROWS expected=$EXPECTED_ROWS"
(( ROWS > 100000000 )) || { echo FAIL too few rows; exit 1; }

"$PY" -c 'import pathlib,pyarrow.parquet as pq
ps=sorted(p for p in pathlib.Path("'"$PARQ"'").glob("*.parquet") if not p.name.startswith("CONVERT"))
p=ps[0]
names=set(pq.read_schema(p).names)
need={"ra","dec"}
missing=need-names
if missing: raise SystemExit(f"FAIL missing columns {missing} have={sorted(names)[:40]}")
print("schema_ok", p.name, "ncols", len(names), "has_id", "id" in names)'

HAS_ID=$("$PY" -c 'import pathlib,pyarrow.parquet as pq
ps=sorted(p for p in pathlib.Path("'"$PARQ"'").glob("*.parquet") if not p.name.startswith("CONVERT"))
print("yes" if "id" in pq.read_schema(ps[0]).names else "no")')
WORKERS=${WORKERS:-8}
MEMORY_LIMIT=${MEMORY_LIMIT:-16GB}
if [[ "$HAS_ID" == "yes" ]]; then
  "$PY" "$CODE/hats_import_run.py" \
    --inputs "$PARQ/*.parquet" --reader parquet \
    --name dr5 --out "$BUILD" --tmp "$TMP" \
    --ra ra --dec dec --sort id \
    --pixel-threshold 1000000 \
    --workers "$WORKERS" --memory-limit "$MEMORY_LIMIT"
else
  "$PY" "$CODE/hats_import_run.py" \
    --inputs "$PARQ/*.parquet" --reader parquet \
    --name dr5 --out "$BUILD" --tmp "$TMP" \
    --ra ra --dec dec \
    --pixel-threshold 1000000 \
    --workers "$WORKERS" --memory-limit "$MEMORY_LIMIT"
fi

N=$("$PY" -c "from pathlib import Path
p=next(q for q in (Path('$BUILD/dr5/hats.properties'), Path('$BUILD/dr5/properties')) if q.exists())
print(next(int(l.split('=',1)[1]) for l in p.read_text().splitlines() if l.startswith('hats_nrows=')))")
[[ "$N" == "$ROWS" ]] || { echo "FAIL hats_nrows $N != $ROWS"; exit 1; }
mv "$BUILD/dr5" "$DST"
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
  "survey": "atlas",
  "release": "dr5",
  "source": "ESO ObsCore VST-ATLAS catalogtile multi-band FITS via dataportal -> parquet",
  "nrows": int(rows),
  "partitions": int(parts),
  "bytes": int(nbytes),
  "temporary_host": True,
  "planned_hats_path": planned,
  "built_at_utc": now,
  "ra_column": "ra",
  "dec_column": "dec",
  "host": "/arc/projects/skyai",
  "note": "Public ACL only after promote to /arc/projects/hats/atlas/dr5",
}
pathlib.Path(dst, "PENSUS_BUILD.json").write_text(json.dumps(prov, indent=2, sort_keys=True) + "\n")
out = {
  **prev,
  "alias": "atlas_dr5_hats",
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
