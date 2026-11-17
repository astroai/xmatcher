#!/bin/bash
set -euo pipefail
# Stage SDSS DR17 stargal-primary sweeps onto skyai (PhotoPrimary-style imaging).
KP=/arc/projects/k-pop/pensus
CODE=$KP/code
HOST=/arc/projects/skyai
STAGE=$HOST/pensus/staging/sdss_dr17_sweeps
RAW=$STAGE/raw
PARQ=$STAGE/parquet
LOG=$KP/logs/sdss_dr17_stage_$(date -u +%Y%m%dT%H%M%SZ).log
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

MMAX=$(ceph_bytes /arc/projects/skyai ceph.quota.max_bytes)
MUSED=$(ceph_bytes /arc/projects/skyai ceph.dir.rbytes)
FREE=$((MMAX-MUSED))
echo "skyai free_gib=$((FREE/2**30))"
# raw ~256 GiB + parquet ~150 + margin → require ≥1200 GiB
(( FREE > 1200 * 2**30 )) || { echo FAIL skyai free for SDSS stage; exit 1; }

mkdir -p "$RAW" "$PARQ" "$STAGE" "$KP/logs"
python3 -m venv --system-site-packages /scratch/sdssenv
/scratch/sdssenv/bin/python -m pip install --quiet --upgrade pip
/scratch/sdssenv/bin/python -m pip install --quiet astropy pyarrow numpy
PY=/scratch/sdssenv/bin/python

"$PY" "$CODE/stage_sdss_dr17_sweeps.py" \
  --out "$RAW" \
  --manifest "$STAGE/MANIFEST.json"

NRAW=$(find "$RAW" -maxdepth 1 -name 'calibObj-*-stargal-primary.fits.gz' ! -name '*wise*' | wc -l)
echo "raw_files=$NRAW"
(( NRAW >= 3700 )) || { echo FAIL too few raw sweeps; exit 1; }

"$PY" "$CODE/sdss_sweeps_convert.py" \
  --raw "$RAW" \
  --out "$PARQ" \
  --manifest "$STAGE/convert.jsonl" \
  --delete-raw

NPARQ=$(find "$PARQ" -maxdepth 1 -name '*.parquet' | wc -l)
ROWS=$("$PY" -c 'import pathlib,pyarrow.parquet as pq
n=0
for p in pathlib.Path("'"$PARQ"'").glob("*.parquet"):
  n+=pq.read_metadata(p).num_rows
print(n)')
echo "parquet_files=$NPARQ rows=$ROWS"
(( NPARQ >= 3700 )) || { echo FAIL too few parquet shards; exit 1; }
(( ROWS > 100000000 )) || { echo FAIL unexpectedly few rows; exit 1; }

"$PY" - <<PY
import datetime, json, pathlib
stage = pathlib.Path("$STAGE")
pointer = pathlib.Path("/arc/projects/k-pop/pensus/inputs/sdss_dr17_sweeps_hats/SOURCE.json")
pointer.parent.mkdir(parents=True, exist_ok=True)
now = datetime.datetime.now(datetime.timezone.utc).isoformat()
doc = {
  "alias": "sdss_dr17_sweeps_hats",
  "survey": "sdss",
  "release": "dr17_sweeps",
  "source": "https://dr17.sdss.org/sas/dr17/eboss/sweeps/dr13_final/301/",
  "product": "calibObj-*-stargal-primary (PhotoPrimary-style; not spectra)",
  "nrows_staged": int("$ROWS"),
  "parquet_files": int("$NPARQ"),
  "staging_path": str(stage),
  "planned_hats_path": "/arc/projects/hats/sdss/dr17_sweeps",
  "temporary_host": "/arc/projects/skyai",
  "probed_at_utc": now,
}
pointer.write_text(json.dumps(doc, indent=2, sort_keys=True) + "\n")
(stage / "STAGE.json").write_text(json.dumps(doc, indent=2, sort_keys=True) + "\n")
print("wrote", pointer)
PY
echo "done stage $(date -Is) LOG=$LOG"
