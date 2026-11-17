#!/bin/bash
set -euo pipefail
# Promote synced LIneA/MAST PS1 OTMO HATS from k-pop cache → official hats + public ACL.
KP=/arc/projects/k-pop/pensus
CODE=$KP/code
SRC=/arc/projects/k-pop/pensus/inputs/_cache/otmo/hats-http-cbef92db81e2
DST=/arc/projects/hats/ps1/otmo
ALIAS=ps1_otmo_hats
ROWS=10560724292
PARTS=27161

python3 - <<'PY'
import os, subprocess
def bytes_attr(path, attr):
  try:
    return int(os.getxattr(path, attr.encode() if isinstance(attr,str) else attr))
  except Exception:
    return int(subprocess.check_output(["getfattr","--only-values","-n",attr,path], text=True).strip())
free = bytes_attr("/arc/projects/hats", "ceph.quota.max_bytes") - bytes_attr("/arc/projects/hats", "ceph.dir.rbytes")
need = int(2782 * 1.1 * 2**30) + 300 * 2**30
print(f"hats_free_gib={free/2**30:.0f} need_gib={need/2**30:.0f}")
if free < need:
  raise SystemExit("FAIL hats free margin for PS1 OTMO")
PY

exec bash "$CODE/promote_hats_tree.sh" "$SRC" "$DST" "$ALIAS" "$ROWS" "$PARTS"
