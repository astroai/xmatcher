#!/bin/bash
set -euo pipefail
# Stage MAST Hubble Source Catalog v4 (unique summary) via CasJobs → mlao.
# No public HATS. Do not touch HSC-SSP promote or ATLAS jobs.
# Host: mlao (skyai busy with HSC cache; ots has ATLAS; unions reserved for VISTA-class).
# CephFS TMP only. Public ACL only after later hats promote.

KP=/arc/projects/k-pop/pensus
CODE=$KP/code
HOST=/arc/projects/mlao
STAGE=$HOST/pensus/staging/hubble_hsc_v4
TMP=$STAGE/tmp
POINTER=$KP/inputs/hubble_hsc_v4_hats/SOURCE.json
PLANNED=/arc/projects/hats/hubble/hsc_v4
LOG=$KP/logs/hubble_hsc_v4_stage_$(date -u +%Y%m%dT%H%M%SZ).log
MODE=${PENSUS_HSCV4_MODE:-prep}   # prep | stage

mkdir -p "$TMP" "$STAGE/shards" "$KP/inputs/hubble_hsc_v4_hats" "$KP/logs" \
  "$(dirname "$PLANNED")"
export TMPDIR=$TMP
export PENSUS_PYDEPS=${PENSUS_PYDEPS:-$TMP/pydeps}
mkdir -p "$PENSUS_PYDEPS"

exec > >(tee -a "$LOG") 2>&1
echo "start $(date -Is) mode=$MODE host=$(hostname) nproc=$(nproc)"
echo "STAGE=$STAGE POINTER=$POINTER PLANNED=$PLANNED TMP=$TMP"

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

MMAX=$(ceph_bytes /arc/projects/mlao ceph.quota.max_bytes)
MUSED=$(ceph_bytes /arc/projects/mlao ceph.dir.rbytes)
FREE=$((MMAX-MUSED))
echo "mlao_free_gib=$((FREE/2**30))"
# ~139M unique rows; allow ≥800 GiB free for parquet shards + TMP headroom
(( FREE > 800 * 2**30 )) || { echo "FAIL mlao free $((FREE/2**30)) GiB < 800"; exit 1; }

# Mirrors-first reminder (already prechecked): no LSDB/TAP HATS for HSCv4.
echo "mirrors: HSCv4 is CasJobs-only; dest pin $PLANNED"

# Prefer system python; install mastcasjobs/pyarrow into CephFS pydeps when staging.
export PYTHONPATH="$PENSUS_PYDEPS${PYTHONPATH:+:$PYTHONPATH}"
if [[ "$MODE" == "stage" ]]; then
  python3 -m pip install --quiet --upgrade --target "$PENSUS_PYDEPS" pip
  python3 -m pip install --quiet --target "$PENSUS_PYDEPS" mastcasjobs pyarrow astropy requests
  if [[ -z "${CASJOBS_USERID:-}" || -z "${CASJOBS_PW:-}" ]]; then
    echo "BLOCKED: CASJOBS_USERID/CASJOBS_PW not in environment"
    python3 "$CODE/stage_hubble_hsc_v4_casjobs.py" \
      --stage "$STAGE" --pointer "$POINTER" --planned-hats "$PLANNED" --prep-only || true
    exit 2
  fi
  python3 "$CODE/stage_hubble_hsc_v4_casjobs.py" \
    --stage "$STAGE" \
    --pointer "$POINTER" \
    --planned-hats "$PLANNED" \
    --ra-step "${PENSUS_HSCV4_RA_STEP:-5.0}"
else
  python3 "$CODE/stage_hubble_hsc_v4_casjobs.py" \
    --stage "$STAGE" \
    --pointer "$POINTER" \
    --planned-hats "$PLANNED" \
    --prep-only
fi

echo "done $(date -Is) MODE=$MODE STAGE=$STAGE POINTER=$POINTER LOG=$LOG"
echo "NOTE: public ACL only after promote to $PLANNED via apply_hats_public_acl.sh"
echo "NOTE: do not disturb y0k6nhqc (HSC-SSP) or n662j3fl (ATLAS)"
