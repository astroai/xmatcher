#!/bin/bash
set -euo pipefail
# Build S-PLUS DR4 HATS on skyai, then promote to hats with public ACL.
KP=/arc/projects/k-pop/pensus
CODE=$KP/code
LOG=$KP/logs/splus_dr4_build_promote_$(date -u +%Y%m%dT%H%M%SZ).log
exec > >(tee -a "$LOG") 2>&1
echo "start $(date -Is) build+promote S-PLUS DR4"

bash "$CODE/build_splus_dr4_hats.sh"
echo "build finished $(date -Is)"

SRC=/arc/projects/skyai/pensus/hats_staging/splus/dr4
ROWS=$(python3 -c 'import json; print(json.load(open("/arc/projects/k-pop/pensus/inputs/splus_dr4_hats/SOURCE.json"))["nrows"])')
PARTS=$(python3 -c "import csv; print(sum(1 for r in csv.DictReader(open('$SRC/partition_info.csv')) if r.get('Npix')))")
export ROWS PARTS
bash "$CODE/promote_splus_dr4.sh"
echo "all done $(date -Is) LOG=$LOG"
