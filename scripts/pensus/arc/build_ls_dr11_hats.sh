#!/bin/bash
set -euo pipefail
# Legacy Surveys Tractor DR11 stage-1 Parquet (k-pop) -> HATS.
#
# Default final destination is /arc/projects/hats/legacysurvey/dr11. While hats
# free is short, set TEMP_HOST to stage under another project, e.g.:
#   TEMP_HOST=/arc/projects/skyai bash build_ls_dr11_hats.sh
# which writes:
#   DST=$TEMP_HOST/pensus/hats_staging/legacysurvey/dr11
#   TMP=$TEMP_HOST/pensus/hats_build/ls_dr11_tmp
# and records planned_hats_path=/arc/projects/hats/legacysurvey/dr11 in SOURCE.json.
#
# Fail-closed: stage1 totals, exact row conservation, verify_hats_tree, deep QA.
# Deletes k-pop stage-1 Parquet only after deep QA. hats-import resume=True.
# TMP must be on CephFS (not /scratch) to avoid 300 Gi ephemeral eviction.
KP=/arc/projects/k-pop/pensus
CODE=$KP/code
LOG=$KP/logs/ls_dr11_build_$(date -u +%Y%m%dT%H%M%SZ).log
exec > >(tee -a "$LOG") 2>&1
echo "start $(date -Is) host=$(hostname) nproc=$(nproc) TEMP_HOST=${TEMP_HOST:-} WORKERS=${WORKERS:-} MEMORY_LIMIT=${MEMORY_LIMIT:-}"

ROWS=3967618454
SWEEPS=2126
ROOT=$KP/staging/legacysurvey_dr11
STAGE=$ROOT/stage1
ALIAS=legacysurvey_dr11_hats
PLANNED_HATS=/arc/projects/hats/legacysurvey/dr11
VENV=/scratch/hatsenv
WORKERS=${WORKERS:-$(( $(nproc) - 1 ))}
MEMORY_LIMIT=${MEMORY_LIMIT:-12GB}

ceph_bytes() {
  # $1=path $2=attr -> integer bytes (0 if unset)
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

if [[ -n "${TEMP_HOST:-}" ]]; then
  BUILD=$TEMP_HOST/pensus/hats_staging/legacysurvey/_build
  DST=$TEMP_HOST/pensus/hats_staging/legacysurvey/dr11
  TMP=${TMP:-$TEMP_HOST/pensus/hats_build/ls_dr11_tmp}
  SPACE_ROOT=$TEMP_HOST
  TEMPORARY=true
else
  BUILD=/arc/projects/hats/legacysurvey/_build
  DST=$PLANNED_HATS
  TMP=${TMP:-$KP/hats_build/ls_dr11_tmp}
  # Output gated on hats; TMP gated on the project that holds TMP (default k-pop).
  SPACE_ROOT=/arc/projects/hats
  # TMP default is under k-pop; derive /arc/projects/<name> from TMP path.
  TMP_SPACE_ROOT=$(echo "$TMP" | cut -d/ -f1-4)
  TEMPORARY=false
fi
TMP_SPACE_ROOT=${TMP_SPACE_ROOT:-$SPACE_ROOT}

[[ -e "$DST" ]] && { echo "FAIL: $DST exists; refuse overwrite"; exit 1; }

# Stage-1 must already be complete (2126 markers). Do not import ls_stage here:
# base:latest lacks torchfits, and re-staging is a separate campaign.
python3 - "$STAGE" "$ROOT/manifest.json" "$SWEEPS" "$ROWS" <<'PY'
import json, pathlib, sys
stage, manifest_path = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2])
sweeps, rows = int(sys.argv[3]), int(sys.argv[4])
manifest = json.loads(manifest_path.read_text())
recs = [json.loads(p.read_text()) for p in (stage / "_done").glob("*.json")]
parts = list(stage.glob("*/*.parquet"))
partial = list(stage.glob("*/*.partial")) + list(stage.glob("*/*.parquet.partial"))
got = sum(r["rows"] for r in recs)
print(f"markers {len(recs)} parquet {len(parts)} partial {len(partial)} rows {got} manifest {len(manifest)}")
if not (
    len(manifest) == sweeps
    and len(recs) == len(parts) == sweeps
    and not partial
    and got == rows
    and sum(m["nrows"] for m in manifest) == rows
):
    sys.exit("FAIL: stage 1 does not match the published LS DR11 sweep totals")
PY

STAGED=$(du -sb --exclude=_done "$STAGE" | cut -f1)
OUT_MAX=$(ceph_bytes "$SPACE_ROOT" ceph.quota.max_bytes)
OUT_USED=$(ceph_bytes "$SPACE_ROOT" ceph.dir.rbytes)
TMP_MAX=$(ceph_bytes "$TMP_SPACE_ROOT" ceph.quota.max_bytes)
TMP_USED=$(ceph_bytes "$TMP_SPACE_ROOT" ceph.dir.rbytes)
echo "staged=$STAGED out_root=$SPACE_ROOT max=$OUT_MAX used=$OUT_USED tmp_root=$TMP_SPACE_ROOT tmp=$TMP dst=$DST temporary=$TEMPORARY workers=$WORKERS memory_limit=$MEMORY_LIMIT"
NEED=$(( STAGED * 110 / 100 ))
TMP_NEED=$(( STAGED * 130 / 100 + 300 * 2**30 ))
# Same-volume peak: output + tmp + 300 GiB floor.
if [[ "$SPACE_ROOT" == "$TMP_SPACE_ROOT" ]]; then
  PEAK=$(( NEED + STAGED * 130 / 100 + 300 * 2**30 ))
  if [[ "$OUT_MAX" != 0 ]] && (( OUT_MAX - OUT_USED < PEAK )); then
    echo "FAIL: $SPACE_ROOT free $(( OUT_MAX - OUT_USED )) < peak need $PEAK (out+tmp+floor)"
    exit 1
  fi
else
  if [[ "$OUT_MAX" != 0 ]] && (( OUT_MAX - OUT_USED < NEED )); then
    echo "FAIL: $SPACE_ROOT free $(( OUT_MAX - OUT_USED )) < need $NEED"; exit 1
  fi
  if [[ "$TMP_MAX" != 0 ]] && (( TMP_MAX - TMP_USED < TMP_NEED )); then
    echo "FAIL: $TMP_SPACE_ROOT free $(( TMP_MAX - TMP_USED )) < tmp need $TMP_NEED"; exit 1
  fi
fi

mkdir -p "$BUILD" "$TMP"
python3 -m venv --system-site-packages "$VENV"
"$VENV/bin/python" -m pip install --quiet --upgrade pip
"$VENV/bin/python" -m pip install --quiet 'hats-import==0.11.1' polars
PY="$VENV/bin/python"

"$PY" "$CODE/hats_import_run.py" \
  --inputs "$STAGE/S/*.parquet" "$STAGE/N/*.parquet" --reader parquet \
  --name dr11 --out "$BUILD" --tmp "$TMP" --ra RA --dec DEC --sort BRICKID \
  --workers "$WORKERS" --memory-limit "$MEMORY_LIMIT"

"$PY" "$CODE/verify_ls_dr11_hats.py" "$BUILD/dr11" "$ROWS" "$STAGE/*/*.parquet" \
  || { echo "FAIL: row conservation; build left at $BUILD/dr11"; exit 1; }

mv "$BUILD/dr11" "$DST"
rmdir "$BUILD" 2>/dev/null || true
rm -rf "$TMP"
BYTES=$(du -sb "$DST" | cut -f1)
PARTS=$("$PY" -c "import csv,sys; print(sum(1 for r in csv.DictReader(open(sys.argv[1])) if r.get('Npix')))" "$DST/partition_info.csv")
HI_VER=$("$PY" -c 'import importlib.metadata as m; print(m.version("hats-import"))')
echo "built $DST bytes=$BYTES partitions=$PARTS"

mkdir -p "$KP/inputs/$ALIAS"
"$PY" - "$DST" "$KP/inputs/$ALIAS/SOURCE.json" "$HI_VER" "$BYTES" "$ROWS" "$SWEEPS" "$PLANNED_HATS" "$TEMPORARY" <<'PY'
import datetime, json, pathlib, sys
dst, pointer, hi_ver, nbytes, rows, sweeps, planned, temporary = sys.argv[1:]
now = datetime.datetime.now(datetime.timezone.utc).isoformat()
props = next(q for q in (pathlib.Path(dst, "hats.properties"), pathlib.Path(dst, "properties")) if q.exists())
nrows = next(int(l.split("=", 1)[1]) for l in props.read_text().splitlines() if l.startswith("hats_nrows="))
assert nrows == int(rows), (nrows, rows)
temp = temporary.lower() == "true"
prov = {
    "survey": "legacysurvey",
    "release": "dr11",
    "source": "https://portal.nersc.gov/cfs/cosmo/data/legacysurvey/dr11/",
    "reference": "Dey et al. 2019, AJ 157, 168; Tractor sweeps DR11",
    "input": f"{sweeps} south/north sweep FITS -> Parquet (PHOTSYS N/S, north _I nulls)",
    "nrows": nrows,
    "ra_column": "RA",
    "dec_column": "DEC",
    "sort_columns": "BRICKID",
    "integrity_key": "RELEASE*1e12+BRICKID*1e6+OBJID, PHOTSYS low bit",
    "hats_import_version": hi_ver,
    "built_at_utc": now,
    "bytes": int(nbytes),
    "temporary_host": temp,
    "planned_hats_path": planned,
}
pathlib.Path(dst, "PENSUS_BUILD.json").write_text(json.dumps(prov, indent=2, sort_keys=True) + "\n")
pathlib.Path(pointer).write_text(json.dumps({
    "survey": "legacysurvey_dr11",
    "alias": pathlib.Path(pointer).parent.name,
    "hats_path": dst,
    "temporary_hats_path": dst if temp else None,
    "promoted_hats_path": planned if temp else dst,
    "planned_hats_path": planned,
    "official_mirror": not temp,
    "hats_layout": "survey/release",
    "exists": True,
    "nrows": nrows,
    "build": prov,
    "properties_head": props.read_text().splitlines()[:16],
    "probed_at_utc": now,
}, indent=2, sort_keys=True) + "\n")
print("wrote", pointer, "nrows", nrows, "temporary", temp, "planned", planned)
PY

"$VENV/bin/python" -m pip install --quiet astropy cdshealpix pyyaml
export PYTHONPATH="$CODE/vendor/xmatcher/src:$CODE"
"$PY" "$CODE/verify_hats_tree.py" "$DST" || { echo "FAIL verify_hats_tree"; exit 1; }
"$PY" "$CODE/qa_hats_release.py" "$DST" "$ALIAS" "$ROWS" "$PARTS" \
  || { echo "FAIL deep QA; k-pop stage-1 Parquet kept at $STAGE"; exit 1; }

# Delete stage-1 Parquet only after deep QA (same policy as CatWISE2020).
find "$STAGE" -name '*.parquet' -delete
find "$STAGE" -mindepth 1 -type d -empty -delete
"$PY" - "$KP/inputs/$ALIAS/SOURCE.json" "$STAGE" "$STAGED" <<'PY'
import datetime, json, pathlib, sys
p = pathlib.Path(sys.argv[1]); d = json.loads(p.read_text())
d["kpop_removed"] = [f"{sys.argv[2]}/*/*.parquet"]; d["kpop_removed_bytes"] = int(sys.argv[3])
d["kpop_removed_at_utc"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
p.write_text(json.dumps(d, indent=2, sort_keys=True) + "\n")
PY
echo "DELETED stage-1 Parquet under $STAGE bytes=$STAGED"
echo "done $(date -Is)"
