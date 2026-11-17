#!/bin/bash
# Stage KiDS DR5 multi-band catalogue tiles (anonymous ESO ObsCore + dataportal) on skyai.
set -euo pipefail
KP=/arc/projects/k-pop/pensus
CODE=$KP/code
HOST=/arc/projects/skyai
STAGE=$HOST/pensus/staging/kids_dr5
LOG=$KP/logs/kids_dr5_stage_$(date -u +%Y%m%dT%H%M%SZ).log
exec > >(tee -a "$LOG") 2>&1
echo "start $(date -Is) host=$(hostname)"
mkdir -p "$STAGE/tiles" "$KP/inputs/kids_dr5_hats" "$HOST/pensus"
: > "$HOST/pensus/.pensus_write_ok" || true
Q=$(getfattr --only-values -n ceph.quota.max_bytes /arc/projects/skyai)
U=$(getfattr --only-values -n ceph.dir.rbytes /arc/projects/skyai)
echo "skyai free_gib=$(( (Q-U)/2**30 ))"
# ~215 GiB catalog + margin; require >= 500 GiB free
(( Q-U > 500*2**30 )) || { echo FAIL skyai free; exit 1; }
python3 "$CODE/stage_kids_dr5_eso.py" \
  --stage "$STAGE" \
  --pointer "$KP/inputs/kids_dr5_hats/SOURCE.json" \
  --planned-hats /arc/projects/hats/kids/dr5 \
  --timeout 600
echo "done $(date -Is) LOG=$LOG"
