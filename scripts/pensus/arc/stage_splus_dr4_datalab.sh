#!/bin/bash
set -euo pipefail
# S-PLUS DR4 public dual photometry from Data Lab TAP → skyai staging.
KP=/arc/projects/k-pop/pensus
CODE=$KP/code
HOST=/arc/projects/skyai
STAGE=$HOST/pensus/staging/splus_dr4
POINTER=$KP/inputs/splus_dr4_hats/SOURCE.json
PLANNED=/arc/projects/hats/splus/dr4
LOG=$KP/logs/splus_dr4_stage_$(date -u +%Y%m%dT%H%M%SZ).log
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
# ~80M rows / ~tens of GiB CSV+parquet; TMP not needed for stage; keep ≥1 TiB floor
(( FREE > 1000 * 2**30 )) || { echo FAIL skyai free; exit 1; }

mkdir -p "$STAGE/shards" "$KP/inputs/splus_dr4_hats" "$KP/logs"
# Prefer system pyarrow if present; else venv
if python3 -c 'import pyarrow' 2>/dev/null; then
  PY=python3
else
  python3 -m venv --system-site-packages /scratch/splusenv
  /scratch/splusenv/bin/python -m pip install --quiet --upgrade pip
  /scratch/splusenv/bin/python -m pip install --quiet pyarrow
  PY=/scratch/splusenv/bin/python
fi

"$PY" "$CODE/stage_splus_dr4_datalab.py" \
  --stage "$STAGE" \
  --pointer "$POINTER" \
  --planned-hats "$PLANNED" \
  --ra-step 2.0

echo "done $(date -Is) STAGE=$STAGE POINTER=$POINTER LOG=$LOG"
echo "NOTE: public DR4 only; no iDR5; public ACL only after later hats promote"
