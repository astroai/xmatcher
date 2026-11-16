#!/bin/bash
set -euo pipefail
KP=/arc/projects/k-pop/pensus
CODE=$KP/code
SRC=/arc/projects/k-pop/pensus/staging/gaia_dr3_xp_continuous/hats
DST=/arc/projects/hats/gaia/dr3_xp_continuous
ALIAS=gaia_dr3_xp_continuous_hats
ROWS=219197643
PARTS=2016
LOG=$KP/logs/promote_gaia_xp_$(date -u +%Y%m%dT%H%M%SZ).log
exec > >(tee -a "$LOG") 2>&1
echo "start $(date -Is) promote Gaia XP-full $SRC -> $DST"

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
staged=int(sp.check_output(["du","-sb","/arc/projects/k-pop/pensus/staging/gaia_dr3_xp_continuous/hats"]).split()[0])
need=int(1.1*staged)+300*(2**30)
print(f"hats free={free} staged={staged} need>={need} quota={q}")
if free < need:
  raise SystemExit(f"FAIL hats free {free} < need {need}")
print("margin_ok")
PY

mkdir -p "$(dirname "$DST")"
bash "$CODE/promote_hats_tree.sh" "$SRC" "$DST" "$ALIAS" "$ROWS" "$PARTS"
bash "$CODE/apply_hats_public_acl.sh" "$(dirname "$DST")" "$DST"
echo "Gaia XP promote done $(date -Is)"
