#!/bin/bash
set -euo pipefail
# Build KiDS DR5 HATS on skyai (local to staging), then promote to hats with public ACL.
KP=/arc/projects/k-pop/pensus
CODE=$KP/code
LOG=$KP/logs/kids_dr5_build_promote_$(date -u +%Y%m%dT%H%M%SZ).log
exec > >(tee -a "$LOG") 2>&1
echo "start $(date -Is) build+promote KiDS DR5"

bash "$CODE/build_kids_dr5_hats.sh"
echo "build finished $(date -Is)"

SRC=/arc/projects/skyai/pensus/hats_staging/kids/dr5
ROWS=$(python3 -c 'import json; print(json.load(open("/arc/projects/k-pop/pensus/inputs/kids_dr5_hats/SOURCE.json"))["nrows"])')
PARTS=$(python3 -c "import csv; print(sum(1 for r in csv.DictReader(open('$SRC/partition_info.csv')) if r.get('Npix')))")
export ROWS PARTS
bash "$CODE/promote_kids_dr5.sh"
echo "all done $(date -Is) LOG=$LOG"
