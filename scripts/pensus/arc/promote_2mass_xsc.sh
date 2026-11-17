#!/bin/bash
set -euo pipefail
KP=/arc/projects/k-pop/pensus
CODE=$KP/code
SRC=/arc/projects/mlao/pensus/hats_staging/2mass/xsc
DST=/arc/projects/hats/2mass/xsc
ALIAS=twomass_xsc_hats
ROWS=1647599
LOG=$KP/logs/promote_2mass_xsc_$(date -u +%Y%m%dT%H%M%SZ).log
exec > >(tee -a "$LOG") 2>&1
echo "start $(date -Is) promote 2MASS XSC $SRC -> $DST"
PARTS=$(python3 -c "import csv; print(sum(1 for r in csv.DictReader(open('$SRC/partition_info.csv')) if r.get('Npix')))")
echo "PARTS=$PARTS"
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
# staged ~80 MiB; use du
import subprocess as sp
staged=int(sp.check_output(["du","-sb","/arc/projects/mlao/pensus/hats_staging/2mass/xsc"]).split()[0])
need=int(1.1*staged)+300*(2**30)
print(f"hats free={free} staged={staged} need>={need} quota={q}")
if free < need:
  raise SystemExit(f"FAIL hats free {free} < need {need}")
print("margin_ok")
PY
bash "$CODE/promote_hats_tree.sh" "$SRC" "$DST" "$ALIAS" "$ROWS" "$PARTS"
bash "$CODE/apply_hats_public_acl.sh" "$DST"
echo "promote wrapper done $(date -Is)"
