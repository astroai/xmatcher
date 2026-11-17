#!/bin/bash
set -euo pipefail
# Promote synced LSDB DELVE DR3 gold HATS from k-pop cache → official hats + public ACL.
KP=/arc/projects/k-pop/pensus
CODE=$KP/code
SRC=/arc/projects/k-pop/pensus/inputs/_cache/delve_dr3_gold/hats-http-42c2d5b251a6
DST=/arc/projects/hats/delve/dr3_gold
ALIAS=delve_dr3_gold_hats
ROWS=2588267795
PARTS=6477

# Margins: ~3049 GiB out ×1.1 ≈ 3354 GiB; hats free must clear that + 300 GiB floor.
python3 - <<'PY'
import os, subprocess
def bytes_attr(path, attr):
  try:
    return int(os.getxattr(path, attr.encode() if isinstance(attr,str) else attr))
  except Exception:
    return int(subprocess.check_output(["getfattr","--only-values","-n",attr,path], text=True).strip())
free = bytes_attr("/arc/projects/hats", "ceph.quota.max_bytes") - bytes_attr("/arc/projects/hats", "ceph.dir.rbytes")
need = int(3049 * 1.1 * 2**30) + 300 * 2**30
print(f"hats_free_gib={free/2**30:.0f} need_gib={need/2**30:.0f}")
if free < need:
  raise SystemExit("FAIL hats free margin for DELVE DR3 gold")
PY

exec bash "$CODE/promote_hats_tree.sh" "$SRC" "$DST" "$ALIAS" "$ROWS" "$PARTS"
