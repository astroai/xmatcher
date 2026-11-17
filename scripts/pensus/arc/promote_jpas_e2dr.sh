#!/bin/bash
set -euo pipefail
# Promote verified temp J-PAS E2DR HATS to official hats + public ACL.
KP=/arc/projects/k-pop/pensus
CODE=$KP/code
SRC=/arc/projects/mlao/pensus/hats_staging/jpas/e2dr
DST=/arc/projects/hats/jpas/e2dr
ALIAS=jpas_e2dr_hats
ROWS=4804561
PARTS=11
exec bash "$CODE/promote_hats_tree.sh" "$SRC" "$DST" "$ALIAS" "$ROWS" "$PARTS"
