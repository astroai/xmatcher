#!/bin/bash
set -euo pipefail
KP=/arc/projects/k-pop/pensus
CODE=$KP/code
SRC=/arc/projects/mlao/pensus/hats_staging/uhs/dr3
DST=/arc/projects/hats/uhs/dr3
ALIAS=uhs_dr3_hats
ROWS=514336270
PARTS=1111
LOG=$KP/logs/promote_uhs_dr3_$(date -u +%Y%m%dT%H%M%SZ).log
exec > >(tee -a "$LOG") 2>&1
echo "start $(date -Is) promote UHS DR3 $SRC -> $DST (accepted soft-fail ra=360)"

python3 - <<'PY'
import os, subprocess
def xb(path, attr):
  try:
    return int(os.getxattr(path, attr))
  except OSError:
    return int(subprocess.check_output(["getfattr","--only-values","-n",attr,path], text=True).strip())
q=xb(b"/arc/projects/hats", b"ceph.quota.max_bytes")
u=xb(b"/arc/projects/hats", b"ceph.dir.rbytes")
free=q-u
import subprocess as sp
staged=int(sp.check_output(["du","-sb","/arc/projects/mlao/pensus/hats_staging/uhs/dr3"]).split()[0])
need=int(1.1*staged)+300*(2**30)
print(f"hats free={free} staged={staged} need>={need} quota={q}")
if free < need:
  raise SystemExit(f"FAIL hats free {free} < need {need}")
print("margin_ok")
PY

mkdir -p "$(dirname "$DST")"
bash "$CODE/promote_hats_tree.sh" "$SRC" "$DST" "$ALIAS" "$ROWS" "$PARTS"
bash "$CODE/apply_hats_public_acl.sh" "$(dirname "$DST")" "$DST"
echo "promote UHS done $(date -Is)"
