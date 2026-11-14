#!/bin/bash
set -uo pipefail
SHARD=$1 NSHARDS=$2 WORKERS=${3:-6} ORDER=${4:-}
LOG=/arc/projects/k-pop/pensus/logs/catwise_stage1_s${SHARD}of${NSHARDS}${ORDER:+_$ORDER}_$(date -u +%Y%m%dT%H%M%SZ).log
exec > >(tee -a "$LOG") 2>&1
echo "start $(date -Is) host=$(hostname) nproc=$(nproc)"
python3 -c "import pyarrow; print(\"pyarrow\", pyarrow.__version__)"; df -h /scratch | tail -1
python3 /arc/projects/k-pop/pensus/code/catwise_stage.py "$SHARD" "$NSHARDS" "$WORKERS" $ORDER
echo "exit $? $(date -Is)"
