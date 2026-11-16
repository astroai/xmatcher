#!/bin/bash
set -euo pipefail
KP=/arc/projects/k-pop/pensus
CODE=$KP/code
HOST=/arc/projects/ots
STAGE=$HOST/pensus/staging/atlas_dr5
LOG=$KP/logs/atlas_dr5_stage_$(date -u +%Y%m%dT%H%M%SZ).log
exec > >(tee -a "$LOG") 2>&1
echo "start $(date -Is) host=$(hostname)"
mkdir -p "$STAGE" "$KP/inputs/atlas_dr5_hats" "$HOST/pensus"
# Ensure write ok
: > "$HOST/pensus/.pensus_write_ok" || true
OMAX=$(getfattr --only-values -n ceph.quota.max_bytes /arc/projects/ots)
OUSED=$(getfattr --only-values -n ceph.dir.rbytes /arc/projects/ots)
echo "ots free_gib=$(( (OMAX-OUSED)/2**30 ))"
(( OMAX-OUSED > 800*2**30 )) || { echo FAIL ots free; exit 1; }
python3 "$CODE/stage_atlas_dr5_osa.py" \
  --stage "$STAGE" \
  --pointer "$KP/inputs/atlas_dr5_hats/SOURCE.json" \
  --planned-hats /arc/projects/hats/atlas/dr5 \
  --ra-step 0.25 \
  --timeout 600
echo "done $(date -Is) LOG=$LOG"
