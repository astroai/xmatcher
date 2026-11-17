#!/bin/bash
set -euo pipefail
KP=/arc/projects/k-pop/pensus
CODE=$KP/code
HOST=/arc/projects/mlao
STAGE=$HOST/pensus/staging/jplus_dr3
LOG=$KP/logs/jplus_dr3_stage_$(date -u +%Y%m%dT%H%M%SZ).log
exec > >(tee -a "$LOG") 2>&1
echo "start $(date -Is) host=$(hostname)"
mkdir -p "$STAGE" "$KP/inputs/jplus_dr3_hats"
MMAX=$(getfattr --only-values -n ceph.quota.max_bytes /arc/projects/mlao)
MUSED=$(getfattr --only-values -n ceph.dir.rbytes /arc/projects/mlao)
echo "mlao free_gib=$(( (MMAX-MUSED)/2**30 ))"
(( MMAX-MUSED > 500*2**30 )) || { echo FAIL mlao free; exit 1; }
python3 "$CODE/stage_jplus_dr3_cefca.py" \
  --stage "$STAGE" \
  --pointer "$KP/inputs/jplus_dr3_hats/SOURCE.json" \
  --planned-hats /arc/projects/hats/jplus/dr3
echo "done $(date -Is) LOG=$LOG"
