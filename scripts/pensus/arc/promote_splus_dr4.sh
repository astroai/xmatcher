#!/bin/bash
set -euo pipefail
# Promote verified temp S-PLUS DR4 HATS to official hats + public ACL.
KP=/arc/projects/k-pop/pensus
CODE=$KP/code
SRC=/arc/projects/skyai/pensus/hats_staging/splus/dr4
DST=/arc/projects/hats/splus/dr4
ALIAS=splus_dr4_hats
# rows/parts filled from pointer + partition_info at promote time if wrapper used;
# default from build claim.
ROWS=${ROWS:-80155428}
PARTS=${PARTS:-0}
if [[ "$PARTS" -eq 0 ]]; then
  PARTS=$(python3 -c "import csv; print(sum(1 for r in csv.DictReader(open('$SRC/partition_info.csv')) if r.get('Npix')))")
fi
exec bash "$CODE/promote_hats_tree.sh" "$SRC" "$DST" "$ALIAS" "$ROWS" "$PARTS"
