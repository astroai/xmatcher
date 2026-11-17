#!/bin/bash
set -euo pipefail
# Promote verified temp SDSS DR17 sweeps HATS to official hats + public ACL.
KP=/arc/projects/k-pop/pensus
CODE=$KP/code
SRC=/arc/projects/skyai/pensus/hats_staging/sdss/dr17_sweeps
DST=/arc/projects/hats/sdss/dr17_sweeps
ALIAS=sdss_dr17_sweeps_hats
ROWS=${ROWS:-0}
PARTS=${PARTS:-0}
if [[ "$ROWS" -eq 0 ]]; then
  ROWS=$(python3 -c 'import json; print(json.load(open("/arc/projects/k-pop/pensus/inputs/sdss_dr17_sweeps_hats/SOURCE.json"))["nrows"])')
fi
if [[ "$PARTS" -eq 0 ]]; then
  PARTS=$(python3 -c "import csv; print(sum(1 for r in csv.DictReader(open('$SRC/partition_info.csv')) if r.get('Npix')))")
fi
exec bash "$CODE/promote_hats_tree.sh" "$SRC" "$DST" "$ALIAS" "$ROWS" "$PARTS"
