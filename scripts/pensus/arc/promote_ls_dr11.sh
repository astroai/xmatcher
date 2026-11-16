#!/bin/bash
set -euo pipefail
KP=/arc/projects/k-pop/pensus
CODE=$KP/code
SRC=/arc/projects/skyai/pensus/hats_staging/legacysurvey/dr11
DST=/arc/projects/hats/legacysurvey/dr11
ALIAS=legacysurvey_dr11_hats
ROWS=3967618454
PARTS=9222
LOG=$KP/logs/promote_legacysurvey_dr11_$(date -u +%Y%m%dT%H%M%SZ).log
exec > >(tee -a "$LOG") 2>&1
echo "start $(date -Is) promote LS DR11 $SRC -> $DST"

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
need=int(1.1 * 898063500021) + 300*(2**30)  # 1.1x tree + 300 GiB floor
print(f"hats free={free} need>={need} quota={q}")
if free < need:
  raise SystemExit(f"FAIL hats free {free} < need {need}")
print("margin_ok")
PY

# Do not touch unions VHS/VVVX jobs; this only copies skyai -> hats.
bash "$CODE/promote_hats_tree.sh" "$SRC" "$DST" "$ALIAS" "$ROWS" "$PARTS"
echo "promote wrapper done $(date -Is)"
