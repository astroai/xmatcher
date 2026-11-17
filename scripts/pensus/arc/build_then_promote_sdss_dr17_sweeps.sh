#!/bin/bash
set -euo pipefail
# Stage SDSS DR17 stargal-primary on skyai, build HATS, promote to hats with public ACL.
KP=/arc/projects/k-pop/pensus
CODE=$KP/code
LOG=$KP/logs/sdss_dr17_build_promote_$(date -u +%Y%m%dT%H%M%SZ).log
exec > >(tee -a "$LOG") 2>&1
echo "start $(date -Is) stage+build+promote SDSS DR17 sweeps (stargal-primary)"

export WORKERS=${WORKERS:-8}
export MEMORY_LIMIT=${MEMORY_LIMIT:-14GB}

bash "$CODE/stage_sdss_dr17_sweeps.sh"
echo "stage finished $(date -Is)"

bash "$CODE/build_sdss_dr17_sweeps_hats.sh"
echo "build finished $(date -Is)"

SRC=/arc/projects/skyai/pensus/hats_staging/sdss/dr17_sweeps
ROWS=$(python3 -c 'import json; print(json.load(open("/arc/projects/k-pop/pensus/inputs/sdss_dr17_sweeps_hats/SOURCE.json"))["nrows"])')
PARTS=$(python3 -c "import csv; print(sum(1 for r in csv.DictReader(open('$SRC/partition_info.csv')) if r.get('Npix')))")
export ROWS PARTS
bash "$CODE/promote_sdss_dr17_sweeps.sh"
echo "all done $(date -Is) LOG=$LOG"
