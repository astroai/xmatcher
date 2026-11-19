#!/bin/bash
set -euo pipefail
# HSC-SSP PDR3 narrow bands: staged NAOJ csv.gz -> parquet -> HATS on mlao (temp).
# Does NOT promote to /arc/projects/hats. Planned paths recorded in SOURCE.json.
# Do not disturb y0k6nhqc n662j3fl i5qvnrr3 kue9aual.
KP=/arc/projects/k-pop/pensus
CODE=$KP/code
HOST=/arc/projects/mlao
STAGE=$HOST/pensus/staging/hsc_pdr3_nb
SHARDS=$STAGE/shards
PARQ_ROOT=$STAGE/parquet
HATS_STAGING=$HOST/pensus/hats_staging/hsc/pdr3
BUILD_ROOT=$HOST/pensus/hats_staging/hsc/_build_nb
TMP_ROOT=$HOST/pensus/hats_build
LOG=$KP/logs/hsc_pdr3_nb_build_$(date -u +%Y%m%dT%H%M%SZ).log
BANDS=(nb0387 nb0816 nb0921 nb1010)

mkdir -p "$PARQ_ROOT" "$HATS_STAGING" "$BUILD_ROOT" "$TMP_ROOT" "$KP/logs"
export TMPDIR=$TMP_ROOT
exec > >(tee -a "$LOG") 2>&1
echo "start $(date -Is) host=$(hostname) nproc=$(nproc)"
echo "STAGE=$STAGE HATS_STAGING=$HATS_STAGING"
echo "NOTE: temp mlao only — NO promote to /arc/projects/hats"
echo "NOTE: leave y0k6nhqc n662j3fl i5qvnrr3 kue9aual alone"

ceph_bytes() {
  python3 -c 'import os,subprocess,sys
path,attr=sys.argv[1],sys.argv[2]
try: print(int(os.getxattr(path,attr)))
except OSError:
  try: print(int(subprocess.check_output(["getfattr","--only-values","-n",attr,path],text=True).strip() or 0))
  except Exception: print(0)' "$1" "$2"
}

FREE=$(( $(ceph_bytes /arc/projects/mlao ceph.quota.max_bytes) - $(ceph_bytes /arc/projects/mlao ceph.dir.rbytes) ))
echo "mlao_free_gib=$((FREE/2**30))"
# ~2 GiB csv.gz total → parquet + HATS TMP; keep generous floor
(( FREE > 400 * 2**30 )) || { echo "FAIL mlao free < 400 GiB"; exit 1; }
[[ -d "$SHARDS" ]] || { echo "FAIL missing shards $SHARDS"; exit 1; }

# Package venv on node-local /scratch; hats-import TMP stays on CephFS.
python3 -m venv --system-site-packages /scratch/hatsenv
/scratch/hatsenv/bin/python -m pip install --quiet --upgrade pip
/scratch/hatsenv/bin/python -m pip install --quiet 'hats-import==0.11.1' polars pyarrow
PY=/scratch/hatsenv/bin/python

WORKERS=${WORKERS:-4}
MEMORY_LIMIT=${MEMORY_LIMIT:-12GB}

for BAND in "${BANDS[@]}"; do
  echo "==== build band=$BAND ===="
  PLANNED=/arc/projects/hats/hsc/pdr3/forced_${BAND}
  ALIAS=hsc_pdr3_forced_${BAND}_hats
  POINTER=$KP/inputs/$ALIAS/SOURCE.json
  PARQ=$PARQ_ROOT/$BAND
  BUILD=$BUILD_ROOT/$BAND
  DST=$HATS_STAGING/forced_${BAND}
  TMP=$TMP_ROOT/hsc_pdr3_${BAND}_tmp

  if [[ -d "$DST" && -f "$DST/properties" ]]; then
    echo "note: $DST already built; skip hats-import"
    continue
  fi
  [[ -e "$DST" ]] && { echo "FAIL: $DST exists without properties"; exit 1; }

  mkdir -p "$PARQ" "$BUILD" "$TMP" "$KP/inputs/$ALIAS"
  "$PY" "$CODE/hsc_nb_csv_to_parquet.py" --shards "$SHARDS" --out "$PARQ" --band "$BAND"
  ROWS=$("$PY" -c 'import pathlib,pyarrow.parquet as pq
n=0
for p in pathlib.Path("'"$PARQ"'").glob("*.parquet"):
  n+=pq.read_metadata(p).num_rows
print(n)')
  echo "parquet_rows=$ROWS band=$BAND"
  (( ROWS > 0 )) || { echo "FAIL zero parquet rows for $BAND"; exit 1; }

  "$PY" -c 'import pathlib,pyarrow.parquet as pq
p=next(pathlib.Path("'"$PARQ"'").glob("*.parquet"))
names=set(pq.read_schema(p).names)
need={"ra","dec","object_id"}
missing=need-names
if missing: raise SystemExit(f"FAIL missing {missing} have={sorted(names)}")
print("schema_ok", p.name, "ncols", len(names))'

  rm -rf "$BUILD"
  mkdir -p "$BUILD"
  "$PY" "$CODE/hats_import_run.py" \
    --inputs "$PARQ"/*.parquet --reader parquet \
    --name "forced_${BAND}" --out "$BUILD" --tmp "$TMP" \
    --ra ra --dec dec --sort object_id \
    --pixel-threshold 1000000 \
    --workers "$WORKERS" --memory-limit "$MEMORY_LIMIT"

  N=$("$PY" -c "from pathlib import Path
p=next(q for q in (Path('$BUILD/forced_${BAND}/hats.properties'), Path('$BUILD/forced_${BAND}/properties')) if q.exists())
print(next(int(l.split('=',1)[1]) for l in p.read_text().splitlines() if l.startswith('hats_nrows=')))")
  [[ "$N" == "$ROWS" ]] || { echo "FAIL hats_nrows $N != $ROWS for $BAND"; exit 1; }
  mv "$BUILD/forced_${BAND}" "$DST"
  rmdir "$BUILD" 2>/dev/null || true
  rm -rf "$TMP"

  PARTS=$("$PY" -c "import csv; print(sum(1 for r in csv.DictReader(open('$DST/partition_info.csv')) if r.get('Npix')))")
  BYTES=$(du -sb "$DST" | cut -f1)
  "$PY" - "$DST" "$POINTER" "$BYTES" "$ROWS" "$PARTS" "$PLANNED" "$BAND" "$ALIAS" <<'PY'
import datetime, json, pathlib, sys
dst, pointer, nbytes, rows, parts, planned, band, alias = sys.argv[1:]
now = datetime.datetime.now(datetime.timezone.utc).isoformat()
prev = json.loads(pathlib.Path(pointer).read_text()) if pathlib.Path(pointer).exists() else {}
prov = {
  "survey": "hsc",
  "release": f"pdr3_forced_{band}",
  "band": band,
  "source": "NAOJ pdr3_dud_rev.summary catalog_jobs csv.gz",
  "nrows": int(rows),
  "partitions": int(parts),
  "bytes": int(nbytes),
  "temporary_host": True,
  "planned_hats_path": planned,
  "built_at_utc": now,
  "ra_column": "ra",
  "dec_column": "dec",
  "host": "/arc/projects/mlao",
  "note": "Temp HATS only — promote to hats after deep QA + headroom; no ACL here",
}
pathlib.Path(dst, "PENSUS_BUILD.json").write_text(json.dumps(prov, indent=2, sort_keys=True) + "\n")
out = {
  **prev,
  "alias": alias,
  "band": band,
  "hats_path": dst,
  "temporary_hats_path": dst,
  "planned_hats_path": planned,
  "official_mirror": False,
  "exists": True,
  "hats_ready": True,
  "nrows": int(rows),
  "build": prov,
  "probed_at_utc": now,
}
pathlib.Path(pointer).parent.mkdir(parents=True, exist_ok=True)
pathlib.Path(pointer).write_text(json.dumps(out, indent=2, sort_keys=True) + "\n")
print("wrote", pointer)
PY

  if [[ -f "$CODE/verify_hats_tree.py" ]]; then
    export PYTHONPATH="$CODE/vendor/xmatcher/src:$CODE${PYTHONPATH:+:$PYTHONPATH}"
    "$PY" -m pip install --quiet astropy cdshealpix pyyaml || true
    "$PY" "$CODE/verify_hats_tree.py" "$DST" || echo "WARN verify_hats_tree soft-fail $BAND"
  fi
  echo "band_done $BAND DST=$DST rows=$ROWS parts=$PARTS planned=$PLANNED"
done

echo "done $(date -Is) LOG=$LOG"
echo "NOTE: temp trees under $HATS_STAGING/forced_nb* — do NOT promote yet"
