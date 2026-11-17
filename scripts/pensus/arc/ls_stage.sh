#!/bin/bash
set -uo pipefail
SHARD=$1 NSHARDS=$2 WORKERS=${3:-6}
LOG=/arc/projects/k-pop/pensus/logs/ls_dr11_stage1_s${SHARD}of${NSHARDS}_$(date -u +%Y%m%dT%H%M%SZ).log
exec > >(tee -a "$LOG") 2>&1
echo "start $(date -Is) host=$(hostname) nproc=$(nproc)"
VENV=/scratch/lsenv
python3 -m venv --system-site-packages "$VENV"
"$VENV/bin/python" -m pip install --quiet 'torchfits==1.3.2' || { echo "FAIL: torchfits install"; exit 1; }
"$VENV/bin/python" -c "import pyarrow, torchfits; print('pyarrow', pyarrow.__version__, 'torchfits', torchfits.__version__)"
df -h /scratch | tail -1
"$VENV/bin/python" /arc/projects/k-pop/pensus/code/ls_stage.py "$SHARD" "$NSHARDS" "$WORKERS"
echo "exit $? $(date -Is)"
