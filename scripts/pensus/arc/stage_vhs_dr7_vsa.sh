#!/bin/bash
set -euo pipefail
KP=/arc/projects/k-pop/pensus
CODE=$KP/code
HOST=/arc/projects/unions
STAGE=$HOST/pensus/staging/vhs_dr7
LOG=$KP/logs/vhs_dr7_stage_$(date -u +%Y%m%dT%H%M%SZ).log
exec > >(tee -a "$LOG") 2>&1
echo "start $(date -Is) host=$(hostname) pin=VHSDR7/VHSv202407131"
mkdir -p "$STAGE" "$KP/inputs/vhs_dr7_hats" "$HOST/pensus"
UMAX=$(getfattr --only-values -n ceph.quota.max_bytes /arc/projects/unions)
UUSED=$(getfattr --only-values -n ceph.dir.rbytes /arc/projects/unions)
FREE=$((UMAX-UUSED))
echo "unions free=$FREE"
# ~1–2 TiB staged + later hats; require comfortable headroom
(( FREE > 3000 * 2**30 )) || { echo FAIL unions free; exit 1; }
# Drop abandoned Data Lab DR5 partials if present (keep disk clean)
if [[ -d "$HOST/pensus/staging/vhs_dr5" ]]; then
  echo "deleting abandoned vhs_dr5 staging"
  rm -rf "$HOST/pensus/staging/vhs_dr5"
fi
rm -f "$KP/inputs/vhs_dr5_hats/SOURCE.json" 2>/dev/null || true
# Do not touch skyai / LS.
python3 "$CODE/stage_vhs_dr7_vsa.py" \
  --stage "$STAGE" \
  --pointer "$KP/inputs/vhs_dr7_hats/SOURCE.json" \
  --planned-hats /arc/projects/hats/vhs/dr7 \
  --ra-step 0.25 \
  --timeout 3600
echo "done $(date -Is)"
