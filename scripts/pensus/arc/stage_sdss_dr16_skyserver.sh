#!/bin/bash
set -euo pipefail
# Stage SDSS DR16 PhotoPrimary via SciServer/CasJobs → mlao (CDS preferred when up).
# Do not disturb y0k6nhqc (HSC), n662j3fl (ATLAS), h8rfta1s (Hubble).
KP=/arc/projects/k-pop/pensus
CODE=$KP/code
HOST=/arc/projects/mlao
STAGE=$HOST/pensus/staging/sdss_dr16_skyserver
TMP=$STAGE/tmp
POINTER=$KP/inputs/sdss_dr16_hats/SOURCE.json
PLANNED=/arc/projects/hats/sdss/dr16
LOG=$KP/logs/sdss_dr16_skyserver_$(date -u +%Y%m%dT%H%M%SZ).log

mkdir -p "$STAGE/shards" "$TMP" "$KP/inputs/sdss_dr16_hats" "$KP/logs" "$(dirname "$PLANNED")"
export TMPDIR=$TMP
export PENSUS_PYDEPS=${PENSUS_PYDEPS:-$TMP/pydeps}
mkdir -p "$PENSUS_PYDEPS"

# Load SKYSERVER_* past interactive bashrc guard — never echo secrets.
if [[ -f "$HOME/.bashrc" ]]; then
  # shellcheck disable=SC1090
  eval "$(grep -E '^[[:space:]]*export[[:space:]]+SKYSERVER_(USERID|PW)=' "$HOME/.bashrc" || true)"
fi
if [[ -n "${SKYSERVER_USERID:-}" ]]; then
  echo "SKYSERVER_USERID_set=yes SKYSERVER_USERID_len=${#SKYSERVER_USERID}"
else
  echo "SKYSERVER_USERID_set=no SKYSERVER_USERID_len=0"
fi
if [[ -n "${SKYSERVER_PW:-}" ]]; then
  echo "SKYSERVER_PW_set=yes SKYSERVER_PW_len=${#SKYSERVER_PW}"
else
  echo "SKYSERVER_PW_set=no SKYSERVER_PW_len=0"
fi

exec > >(tee -a "$LOG") 2>&1
echo "start $(date -Is) host=$(hostname) nproc=$(nproc)"
echo "STAGE=$STAGE POINTER=$POINTER PLANNED=$PLANNED"
echo "NOTE: leave y0k6nhqc n662j3fl h8rfta1s alone"

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
(( FREE > 800 * 2**30 )) || { echo "FAIL mlao free < 800 GiB"; exit 1; }

export PYTHONPATH="$PENSUS_PYDEPS${PYTHONPATH:+:$PYTHONPATH}"
# SciServer is not on PyPI — stage_sdss_dr16_skyserver.py talks Keystone/CasJobs via urllib.
python3 -m pip install --quiet --upgrade --target "$PENSUS_PYDEPS" pip
python3 -m pip install --quiet --target "$PENSUS_PYDEPS" pandas pyarrow

if [[ -z "${SKYSERVER_USERID:-}" || -z "${SKYSERVER_PW:-}" ]]; then
  echo "BLOCKED: SKYSERVER_* missing"
  python3 "$CODE/stage_sdss_dr16_skyserver.py" \
    --stage "$STAGE" --pointer "$POINTER" --planned-hats "$PLANNED" --prep-only || true
  exit 2
fi

# Default 1° strips (2° OK in sparse RA; dense strips bisect to 0.05°).
python3 "$CODE/stage_sdss_dr16_skyserver.py" \
  --stage "$STAGE" \
  --pointer "$POINTER" \
  --planned-hats "$PLANNED" \
  --ra-step "${PENSUS_SDSS_RA_STEP:-1.0}" \
  --ra-start "${PENSUS_SDSS_RA_START:-0.0}" \
  --ra-stop "${PENSUS_SDSS_RA_STOP:-360.0}"

echo "done $(date -Is) LOG=$LOG"
echo "NOTE: hats-import + promote to $PLANNED + ACL after staging completes"
