#!/bin/bash
set -euo pipefail
# Stage HSC-SSP PDR3 narrow bands NB0387/0816/0921/1010 via NAOJ catalog_jobs.
# Auth: HSC_USERID/HSC_PW from bashrc (past interactive guard).
# Do not disturb y0k6nhqc (broad promote), n662j3fl, h8rfta1s.
KP=/arc/projects/k-pop/pensus
CODE=$KP/code
HOST=/arc/projects/mlao
STAGE=$HOST/pensus/staging/hsc_pdr3_nb
TMP=$STAGE/tmp
POINTER_ROOT=$KP/inputs
LOG=$KP/logs/hsc_pdr3_nb_stage_$(date -u +%Y%m%dT%H%M%SZ).log

mkdir -p "$STAGE/shards" "$TMP" "$KP/logs" "$POINTER_ROOT"
export TMPDIR=$TMP

if [[ -f "$HOME/.bashrc" ]]; then
  # shellcheck disable=SC1090
  eval "$(grep -E '^[[:space:]]*export[[:space:]]+HSC_(USERID|PW)=' "$HOME/.bashrc" || true)"
fi
if [[ -n "${HSC_USERID:-}" ]]; then
  echo "HSC_USERID_set=yes HSC_USERID_len=${#HSC_USERID}"
else
  echo "HSC_USERID_set=no HSC_USERID_len=0"
fi
if [[ -n "${HSC_PW:-}" ]]; then
  echo "HSC_PW_set=yes HSC_PW_len=${#HSC_PW}"
else
  echo "HSC_PW_set=no HSC_PW_len=0"
fi

exec > >(tee -a "$LOG") 2>&1
echo "start $(date -Is) host=$(hostname) nproc=$(nproc)"
echo "STAGE=$STAGE POINTER_ROOT=$POINTER_ROOT"
echo "NOTE: leave y0k6nhqc n662j3fl h8rfta1s alone; planned hats/hsc/pdr3/forced_nb*"

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
(( FREE > 200 * 2**30 )) || { echo "FAIL mlao free < 200 GiB"; exit 1; }

if [[ -z "${HSC_USERID:-}" || -z "${HSC_PW:-}" ]]; then
  echo "BLOCKED: HSC_* missing"
  python3 "$CODE/stage_hsc_pdr3_nb_naoj.py" --stage "$STAGE" --pointer-root "$POINTER_ROOT" --prep-only || true
  exit 2
fi

python3 "$CODE/stage_hsc_pdr3_nb_naoj.py" \
  --stage "$STAGE" \
  --pointer-root "$POINTER_ROOT"

echo "done $(date -Is) LOG=$LOG"
echo "NOTE: hats-import + promote to hats/hsc/pdr3/forced_nb* after stage"
