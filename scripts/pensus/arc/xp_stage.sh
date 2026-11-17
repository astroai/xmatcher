#!/bin/bash
set -uo pipefail
SHARD=$1 NSHARDS=$2 WORKERS=${3:-6}
LOG=/arc/projects/k-pop/pensus/logs/xp_stage1_s${SHARD}of${NSHARDS}_$(date -u +%Y%m%dT%H%M%SZ).log
exec > >(tee -a "$LOG") 2>&1
echo "start $(date -Is) host=$(hostname) nproc=$(nproc)"
python3 -c 'import pyarrow; print("pyarrow", pyarrow.__version__)'
python3 /arc/projects/k-pop/pensus/code/xp_stage.py "$SHARD" "$NSHARDS" "$WORKERS"
echo "exit $? $(date -Is)"
