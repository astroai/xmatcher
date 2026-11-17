#!/bin/bash
set -euo pipefail
KP=/arc/projects/k-pop/pensus
CODE=$KP/code
HOST=/arc/projects/mlao
STAGE=$HOST/pensus/staging/jpas_e2dr
LOG=$KP/logs/jpas_e2dr_stage_$(date -u +%Y%m%dT%H%M%SZ).log
exec > >(tee -a "$LOG") 2>&1
echo "start $(date -Is) host=$(hostname)"
mkdir -p "$STAGE" "$KP/inputs/jpas_e2dr_hats"
MMAX=$(getfattr --only-values -n ceph.quota.max_bytes /arc/projects/mlao)
MUSED=$(getfattr --only-values -n ceph.dir.rbytes /arc/projects/mlao)
echo "mlao free_gib=$(( (MMAX-MUSED)/2**30 ))"
(( MMAX-MUSED > 400*2**30 )) || { echo FAIL mlao free; exit 1; }
python3 "$CODE/stage_jpas_e2dr_cefca.py" \
  --stage "$STAGE" \
  --pointer "$KP/inputs/jpas_e2dr_hats/SOURCE.json" \
  --planned-hats /arc/projects/hats/jpas/e2dr
echo "done $(date -Is) LOG=$LOG"
