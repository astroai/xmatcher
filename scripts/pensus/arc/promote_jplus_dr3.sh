#!/bin/bash
set -euo pipefail
# Promote verified temp J-PLUS DR3 HATS to official hats + public ACL.
KP=/arc/projects/k-pop/pensus
CODE=$KP/code
SRC=/arc/projects/mlao/pensus/hats_staging/jplus/dr3
DST=/arc/projects/hats/jplus/dr3
ALIAS=jplus_dr3_hats
ROWS=16419372
PARTS=41
exec bash "$CODE/promote_hats_tree.sh" "$SRC" "$DST" "$ALIAS" "$ROWS" "$PARTS"
