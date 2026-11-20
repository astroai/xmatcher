#!/bin/bash
set -euo pipefail
# Stage SDSS DR16 stargal-primary sweeps onto skyai (PhotoPrimary-style imaging).
# Prefer SAS dump while CDS vizcat is down. Do NOT kill SkyServer contingency nxs5ai9m.
# No hats-import / promote in this job — stage parquet only; pin later to hats/sdss/dr16.
# Do not disturb y0k6nhqc (HSC), n662j3fl (ATLAS), i5qvnrr3 (Hubble).
KP=/arc/projects/k-pop/pensus
CODE=$KP/code
HOST=/arc/projects/skyai
STAGE=$HOST/pensus/staging/sdss_dr16_sweeps
RAW=$STAGE/raw
PARQ=$STAGE/parquet
TMP=$STAGE/tmp
POINTER=$KP/inputs/sdss_dr16_hats/SOURCE.json
PLANNED=/arc/projects/hats/sdss/dr16
# data.sdss.org still TLS-timeout from this fleet; dr16.sdss.org serves the same tree.
SAS_BASE=${PENSUS_SDSS_SAS_BASE:-https://dr16.sdss.org/sas/dr16/eboss/sweeps/dr13_final}
LOG=$KP/logs/sdss_dr16_sweeps_stage_$(date -u +%Y%m%dT%H%M%SZ).log

mkdir -p "$RAW" "$PARQ" "$TMP" "$STAGE" "$KP/logs" "$KP/inputs/sdss_dr16_hats"
export TMPDIR=$TMP
export PENSUS_PYDEPS=${PENSUS_PYDEPS:-$TMP/pydeps}
mkdir -p "$PENSUS_PYDEPS"

exec > >(tee -a "$LOG") 2>&1
echo "start $(date -Is) host=$(hostname) nproc=$(nproc)"
echo "STAGE=$STAGE POINTER=$POINTER PLANNED=$PLANNED SAS_BASE=$SAS_BASE"
echo "NOTE: leave y0k6nhqc n662j3fl i5qvnrr3 nxs5ai9m alone; stage-only no promote"

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
# raw ~256 GiB + parquet ~150–220 + margin → require ≥900 GiB
(( FREE > 900 * 2**30 )) || { echo FAIL skyai free for SDSS DR16 SAS stage; exit 1; }

export PYTHONPATH="$PENSUS_PYDEPS${PYTHONPATH:+:$PYTHONPATH}"
python3 -m pip install --quiet --upgrade --target "$PENSUS_PYDEPS" pip
python3 -m pip install --quiet --target "$PENSUS_PYDEPS" astropy pyarrow numpy

PENSUS_SDSS_SAS_BASE="$SAS_BASE" python3 "$CODE/stage_sdss_dr16_sweeps.py" \
  --out "$RAW" \
  --manifest "$STAGE/MANIFEST.json" \
  --base "$SAS_BASE"

NRAW=$(find "$RAW" -maxdepth 1 -name 'calibObj-*-stargal-primary.fits.gz' ! -name '*wise*' | wc -l)
echo "raw_files=$NRAW"
(( NRAW >= 3700 )) || { echo FAIL too few raw sweeps; exit 1; }

python3 "$CODE/sdss_sweeps_convert.py" \
  --raw "$RAW" \
  --out "$PARQ" \
  --manifest "$STAGE/convert.jsonl" \
  --delete-raw

NPARQ=$(find "$PARQ" -maxdepth 1 -name '*.parquet' | wc -l)
ROWS=$(python3 -c 'import pathlib,pyarrow.parquet as pq
n=0
for p in pathlib.Path("'"$PARQ"'").glob("*.parquet"):
  n+=pq.read_metadata(p).num_rows
print(n)')
echo "parquet_files=$NPARQ rows=$ROWS"
(( NPARQ >= 3700 )) || { echo FAIL too few parquet shards; exit 1; }
(( ROWS > 100000000 )) || { echo FAIL unexpectedly few rows; exit 1; }

python3 - <<PY
import datetime, json, pathlib
stage = pathlib.Path("$STAGE")
pointer = pathlib.Path("$POINTER")
pointer.parent.mkdir(parents=True, exist_ok=True)
now = datetime.datetime.now(datetime.timezone.utc).isoformat()
doc = {
  "alias": "sdss_dr16_hats",
  "survey": "sdss",
  "release": "dr16",
  "source": "$SAS_BASE/301/",
  "product": "calibObj-*-stargal-primary (PhotoPrimary-style; not spectra)",
  "nrows_staged": int("$ROWS"),
  "parquet_files": int("$NPARQ"),
  "staging_path": str(stage),
  "planned_hats_path": "$PLANNED",
  "temporary_host": "$HOST",
  "probed_at_utc": now,
  "note": "SAS stage while CDS vizcat down; SkyServer nxs5ai9m left running as contingency",
}
pointer.write_text(json.dumps(doc, indent=2, sort_keys=True) + "\n")
(stage / "STAGE.json").write_text(json.dumps(doc, indent=2, sort_keys=True) + "\n")
print("wrote", pointer)
PY
echo "done stage $(date -Is) LOG=$LOG"
echo "NOTE: hats-import + promote to $PLANNED + ACL only after deep QA / headroom"
