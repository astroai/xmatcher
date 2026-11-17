#!/bin/bash
set -euo pipefail
# UKIDSS DR11PLUS GCS seamless staging on skyai (after LAS; leave mlao/unions for UHS/VHS).
KP=/arc/projects/k-pop/pensus
CODE=$KP/code
HOST=/arc/projects/skyai
STAGE=$HOST/pensus/staging/ukidss_dr11_gcs
LOG=$KP/logs/ukidss_dr11_gcs_stage_$(date -u +%Y%m%dT%H%M%SZ).log
exec > >(tee -a "$LOG") 2>&1
echo "start $(date -Is) host=$(hostname)"

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
echo "skyai free=$FREE max=$MMAX used=$MUSED"
(( FREE > 2000 * 2**30 )) || { echo FAIL skyai free; exit 1; }

mkdir -p "$STAGE" "$KP/inputs/ukidss_dr11_gcs_hats"
python3 "$CODE/stage_ukidss_dr11_gcs_wsa.py" \
  --stage "$STAGE" \
  --pointer "$KP/inputs/ukidss_dr11_gcs_hats/SOURCE.json" \
  --planned-hats /arc/projects/hats/ukidss/dr11_gcs \
  --host-label skyai \
  --ra-step 2.0 \
  --timeout 3600
echo "done $(date -Is)"
