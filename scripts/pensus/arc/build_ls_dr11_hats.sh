#!/bin/bash
set -euo pipefail
# Legacy Surveys Tractor DR11 stage-1 Parquet (k-pop) -> HATS at /arc/projects/hats/legacysurvey/dr11.
# Builds straight into hats (no k-pop HATS copy), checks exact row conservation and deep QA, then
# repoints inputs/legacysurvey_dr11_hats, and deletes the k-pop stage-1 Parquet only after deep QA.
# Do not launch until hats free >= ~1.05x staged (~1 TB today) or ceph.quota.max_bytes >= 25e12.
# hats-import resume=True; TMP must be on CephFS (not /scratch) to avoid 300 Gi ephemeral eviction.
KP=/arc/projects/k-pop/pensus
CODE=$KP/code
LOG=$KP/logs/ls_dr11_build_$(date -u +%Y%m%dT%H%M%SZ).log
exec > >(tee -a "$LOG") 2>&1
echo "start $(date -Is) host=$(hostname) nproc=$(nproc)"

ROWS=3967618454
SWEEPS=2126
ROOT=$KP/staging/legacysurvey_dr11
STAGE=$ROOT/stage1
BUILD=/arc/projects/hats/legacysurvey/_build
DST=/arc/projects/hats/legacysurvey/dr11
ALIAS=legacysurvey_dr11_hats
# hats-import intermediates must NOT live on node-local /scratch: CANFAR evicts the
# pod when ephemeral local storage hits 300Gi (seen on CatWISE ktnf1y2f mid-Splitting).
TMP=$KP/hats_build/ls_dr11_tmp
VENV=/scratch/hatsenv
WORKERS=${WORKERS:-$(( $(nproc) - 1 ))}

[[ -e "$DST" ]] && { echo "FAIL: $DST exists; refuse overwrite"; exit 1; }

# Leftover / incomplete sweeps only; no-op when all 2126 markers are present.
python3 "$CODE/ls_stage.py" 0 1 "$WORKERS" || { echo "FAIL: stage 1 incomplete"; exit 1; }
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
HATS_MAX=$(getfattr --only-values -n ceph.quota.max_bytes /arc/projects/hats)
HATS_USED=$(getfattr --only-values -n ceph.dir.rbytes /arc/projects/hats)
KPOP_MAX=$(getfattr --only-values -n ceph.quota.max_bytes /arc/projects/k-pop)
KPOP_USED=$(getfattr --only-values -n ceph.dir.rbytes /arc/projects/k-pop)
echo "staged=$STAGED hats max_bytes=$HATS_MAX rbytes=$HATS_USED k-pop free=$(( KPOP_MAX - KPOP_USED )) tmp=$TMP"
# HATS output ~1.0x stage-1 bytes with headroom; require 10% above staged on hats.
NEED=$(( STAGED * 110 / 100 ))
if [[ "$HATS_MAX" != 0 ]] && (( HATS_MAX - HATS_USED < NEED )); then
  echo "FAIL: hats free $(( HATS_MAX - HATS_USED )) < need $NEED (wait for >=1TB free or max_bytes>=25e12)"
  exit 1
fi
# Intermediates peak ~1.0–1.3× staged; keep 300 GiB k-pop floor after that.
(( KPOP_MAX - KPOP_USED > STAGED * 130 / 100 + 300 * 2**30 )) \
  || { echo "FAIL: k-pop free $(( KPOP_MAX - KPOP_USED )) too small for tmp on CephFS"; exit 1; }

mkdir -p "$BUILD" "$TMP"
python3 -m venv --system-site-packages "$VENV"
"$VENV/bin/python" -m pip install --quiet --upgrade pip
"$VENV/bin/python" -m pip install --quiet 'hats-import==0.11.1' polars
PY="$VENV/bin/python"

"$PY" "$CODE/hats_import_run.py" \
  --inputs "$STAGE/S/*.parquet" "$STAGE/N/*.parquet" --reader parquet \
  --name dr11 --out "$BUILD" --tmp "$TMP" --ra RA --dec DEC --sort BRICKID --workers "$WORKERS"

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
"$PY" - "$DST" "$KP/inputs/$ALIAS/SOURCE.json" "$HI_VER" "$BYTES" "$ROWS" "$SWEEPS" <<'PY'
import datetime, json, pathlib, sys
dst, pointer, hi_ver, nbytes, rows, sweeps = sys.argv[1:]
now = datetime.datetime.now(datetime.timezone.utc).isoformat()
props = next(q for q in (pathlib.Path(dst, "hats.properties"), pathlib.Path(dst, "properties")) if q.exists())
nrows = next(int(l.split("=", 1)[1]) for l in props.read_text().splitlines() if l.startswith("hats_nrows="))
assert nrows == int(rows), (nrows, rows)
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
}
pathlib.Path(dst, "PENSUS_BUILD.json").write_text(json.dumps(prov, indent=2, sort_keys=True) + "\n")
pathlib.Path(pointer).write_text(json.dumps({
    "survey": "legacysurvey_dr11",
    "alias": pathlib.Path(pointer).parent.name,
    "hats_path": dst,
    "promoted_hats_path": dst,
    "official_mirror": True,
    "hats_layout": "survey/release",
    "exists": True,
    "nrows": nrows,
    "build": prov,
    "properties_head": props.read_text().splitlines()[:16],
    "probed_at_utc": now,
}, indent=2, sort_keys=True) + "\n")
print("wrote", pointer, "nrows", nrows)
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
