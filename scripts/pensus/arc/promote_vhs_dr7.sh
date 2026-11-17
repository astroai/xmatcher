#!/bin/bash
set -euo pipefail
# Promote verified temp VHS DR7 HATS to official hats + public ACL.
KP=/arc/projects/k-pop/pensus
CODE=$KP/code
SRC=/arc/projects/unions/pensus/hats_staging/vhs/dr7
DST=/arc/projects/hats/vhs/dr7
ALIAS=vhs_dr7_hats
ROWS=1496104720
PARTS=3274
# Large tree: ask promote for headroom check via optional 6th arg if supported; else rely on free space.
exec bash "$CODE/promote_hats_tree.sh" "$SRC" "$DST" "$ALIAS" "$ROWS" "$PARTS"
