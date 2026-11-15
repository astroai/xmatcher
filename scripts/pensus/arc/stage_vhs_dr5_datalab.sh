#!/bin/bash
set -euo pipefail
KP=/arc/projects/k-pop/pensus
CODE=$KP/code
HOST=/arc/projects/unions
STAGE=$HOST/pensus/staging/vhs_dr5
LOG=$KP/logs/vhs_dr5_stage_$(date -u +%Y%m%dT%H%M%SZ).log
exec > >(tee -a "$LOG") 2>&1
echo "start $(date -Is) host=$(hostname)"
mkdir -p "$STAGE" "$KP/inputs/vhs_dr5_hats" "$HOST/pensus"
# space gate: expect ~0.5–1.5 TiB staged CSV.gz; need 1.1x + headroom; unions ~43 TiB free
UMAX=$(getfattr --only-values -n ceph.quota.max_bytes /arc/projects/unions)
UUSED=$(getfattr --only-values -n ceph.dir.rbytes /arc/projects/unions)
FREE=$((UMAX-UUSED))
echo "unions free=$FREE"
(( FREE > 3000 * 2**30 )) || { echo FAIL unions free; exit 1; }
# Do not touch skyai / LS.
python3 "$CODE/stage_vhs_dr5_datalab.py" \
  --stage "$STAGE" \
  --pointer "$KP/inputs/vhs_dr5_hats/SOURCE.json" \
  --planned-hats /arc/projects/hats/vhs/dr5 \
  --ra-step 0.25
echo "done $(date -Is)"
