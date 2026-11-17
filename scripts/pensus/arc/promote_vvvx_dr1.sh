#!/bin/bash
set -euo pipefail
# Promote verified temp VVVX DR1 HATS to official hats + public ACL.
KP=/arc/projects/k-pop/pensus
CODE=$KP/code
SRC=/arc/projects/unions/pensus/hats_staging/vvvx/dr1
DST=/arc/projects/hats/vvvx/dr1
ALIAS=vvvx_dr1_hats
ROWS=886574093
PARTS=1663
exec bash "$CODE/promote_hats_tree.sh" "$SRC" "$DST" "$ALIAS" "$ROWS" "$PARTS"
