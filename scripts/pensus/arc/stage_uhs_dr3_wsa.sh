#!/bin/bash
set -euo pipefail
KP=/arc/projects/k-pop/pensus
CODE=$KP/code
HOST=/arc/projects/mlao
STAGE=$HOST/pensus/staging/uhs_dr3
LOG=$KP/logs/uhs_dr3_stage_$(date -u +%Y%m%dT%H%M%SZ).log
exec > >(tee -a "$LOG") 2>&1
echo "start $(date -Is) host=$(hostname)"
mkdir -p "$STAGE" "$KP/inputs/uhs_dr3_hats"
MMAX=$(getfattr --only-values -n ceph.quota.max_bytes /arc/projects/mlao)
MUSED=$(getfattr --only-values -n ceph.dir.rbytes /arc/projects/mlao)
FREE=$((MMAX-MUSED))
echo "mlao free=$FREE"
# Need headroom for ~50–200 GiB staged + later hats (~0.15–0.5 TiB) while XSC runs (~few GiB)
(( FREE > 800 * 2**30 )) || { echo FAIL mlao free; exit 1; }
# Leave skyai alone; XSC may share mlao — both small vs free.
python3 "$CODE/stage_uhs_dr3_wsa.py" \
  --stage "$STAGE" \
  --pointer "$KP/inputs/uhs_dr3_hats/SOURCE.json" \
  --planned-hats /arc/projects/hats/uhs/dr3 \
  --ra-step 2.0 \
  --timeout 3600
echo "done $(date -Is)"
