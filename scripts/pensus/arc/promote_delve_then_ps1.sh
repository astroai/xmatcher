#!/bin/bash
set -euo pipefail
# Serialize large cache promotes: DELVE DR3 gold (~3.05 TiB) then PS1 OTMO (~2.78 TiB).
# Both are already HATS trees under k-pop pensus/inputs/_cache — no hats-import needed.
KP=/arc/projects/k-pop/pensus
CODE=$KP/code
LOG=$KP/logs/promote_delve_then_ps1_$(date -u +%Y%m%dT%H%M%SZ).log
exec > >(tee -a "$LOG") 2>&1
echo "start $(date -Is) serialize DELVE then PS1"

bash "$CODE/promote_delve_dr3_gold.sh"
echo "DELVE promote finished $(date -Is)"

bash "$CODE/promote_ps1_otmo.sh"
echo "PS1 promote finished $(date -Is)"
echo "all done $(date -Is) LOG=$LOG"
