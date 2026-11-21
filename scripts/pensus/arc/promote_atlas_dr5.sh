#!/bin/bash
set -euo pipefail
# Promote verified temp ATLAS DR5/ER5 HATS to official hats + public ACL.
KP=/arc/projects/k-pop/pensus
CODE=$KP/code
SRC=/arc/projects/skyai/pensus/hats_staging/atlas/dr5
DST=/arc/projects/hats/atlas/dr5
ALIAS=atlas_dr5_hats
ROWS=${ROWS:-0}
PARTS=${PARTS:-0}
if [[ "$ROWS" -eq 0 ]]; then
  ROWS=$(python3 -c 'import json; print(json.load(open("/arc/projects/k-pop/pensus/inputs/atlas_dr5_hats/SOURCE.json"))["nrows"])')
fi
if [[ "$PARTS" -eq 0 ]]; then
  PARTS=$(python3 -c "import csv; print(sum(1 for r in csv.DictReader(open('$SRC/partition_info.csv')) if r.get('Npix')))")
fi
exec bash "$CODE/promote_hats_tree.sh" "$SRC" "$DST" "$ALIAS" "$ROWS" "$PARTS"
