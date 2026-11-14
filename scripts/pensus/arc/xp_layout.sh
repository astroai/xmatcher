#!/bin/bash
set -uo pipefail
# Usage: xp_layout.sh run <shard> <nshards> <workers> | xp_layout.sh finalize
GAIA=/arc/projects/hats/gaia/dr3
XP=/arc/projects/k-pop/pensus/staging/gaia_dr3_xp_continuous
CMD=$1; shift
LOG=/arc/projects/k-pop/pensus/logs/xp_layout_${CMD}_${1:-all}_$(date -u +%Y%m%dT%H%M%SZ).log
exec > >(tee -a "$LOG") 2>&1
echo "start $(date -Is) host=$(hostname) nproc=$(nproc)"
test -f "$GAIA/properties" || { echo "FAIL: Gaia not in hats yet"; exit 1; }
python3 -m venv --system-site-packages /scratch/xpenv && /scratch/xpenv/bin/python -m pip install --quiet cdshealpix
if [[ $CMD == run ]]; then
  # Stage 1 is still producing files: keep laying out new ones until all files are done.
  TOTAL=$(grep -c . "$XP/_MD5SUM.txt")
  while :; do
    STAGED=$(find "$XP/stage1/_done" -name '*.json' | wc -l)
    /scratch/xpenv/bin/python /arc/projects/k-pop/pensus/code/xp_layout.py run "$GAIA" "$XP/stage1" "$XP/hats" "$@"
    RC=$?
    DONE=$(find "$XP/hats/_done" -name '*.json' | wc -l)
    echo "laid out $DONE/$TOTAL (staged $STAGED) $(date -Is)"
    [[ $DONE -ge $TOTAL ]] && break
    [[ $RC -ne 0 && $STAGED -ge $TOTAL ]] && { echo "FAIL: stage 1 complete but layout failures remain"; exit 1; }
    sleep 600
  done
else
  /scratch/xpenv/bin/python /arc/projects/k-pop/pensus/code/xp_layout.py finalize "$GAIA" "$XP/stage1" "$XP/hats" 219197643
fi
echo "exit $? $(date -Is)"
