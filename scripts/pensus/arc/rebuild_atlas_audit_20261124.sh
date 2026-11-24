#!/usr/bin/env bash
set -euo pipefail

# Run in a CANFAR headless session. The audited mirror is read-only here.
RUN=${1:?pass a new repair run directory under /arc/projects/astroai/pensus}
INPUT=${2:-/arc/projects/skyai/pensus/staging/atlas_dr5_eso/parquet}
CODE=$(cd "$(dirname "$0")" && pwd)
case "$RUN" in /arc/projects/astroai/pensus/*) ;; *) exit 2 ;; esac
mkdir -p "$(dirname "$RUN")"
mkdir "$RUN"
exec > >(tee -a "$RUN/rebuild.log") 2>&1
trap 'rc=$?; printf "%s\n" "$rc" > "$RUN/rebuild.exit"' EXIT

MIRROR=/arc/projects/hats/atlas/dr5
PARKED=/arc/projects/skyai/pensus/hats_staging/_promoting/atlas/dr5
if [[ -f "$PARKED/partition_info.csv" ]]; then
    printf '%s\n' "$PARKED" > "$RUN/source.path"
    exit
fi

export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1
python3 -m venv --system-site-packages /scratch/atlas-audit-env
PY=/scratch/atlas-audit-env/bin/python
"$PY" -m pip install --quiet 'hats-import==0.11.1' 'hats==0.11.2'
NINPUT=$(find "$INPUT" -maxdepth 1 -type f -name '*.parquet' | wc -l)
(( NINPUT >= 5000 ))
printf '%s\n' "Input shards $NINPUT"
df -h /scratch "$RUN"
mkdir -p /scratch/atlas-import "$RUN/candidate"
"$PY" "$CODE/hats_import_run.py" --inputs "$INPUT/*.parquet" --reader parquet \
    --out "$RUN/candidate" --name dr5 --tmp /scratch/atlas-import --ra ra --dec dec \
    --sort id --pixel-threshold 1000000 --workers "${WORKERS:-6}" --memory-limit "${WORKER_MEMORY:-6GB}"

"$PY" - "$RUN/candidate/dr5" "$MIRROR" <<'PY'
import csv
import sys
from pathlib import Path

import pyarrow.parquet as pq


def layout(root):
    with (root / "partition_info.csv").open() as stream:
        return {(int(r["Norder"]), int(r["Npix"])) for r in csv.DictReader(stream)}


candidate, live = map(Path, sys.argv[1:])
assert layout(candidate) == layout(live), "rebuild changed the partition tree"
rows = 0
for part in sorted(layout(candidate)):
    order, pix = part
    path = candidate / f"dataset/Norder={order}/Dir={pix // 10000 * 10000}/Npix={pix}.parquet"
    rows += pq.read_metadata(path).num_rows
assert rows == 150198243, f"expected original row count, rebuilt {rows}"
PY
printf '%s\n' "$RUN/candidate/dr5" > "$RUN/source.path"
