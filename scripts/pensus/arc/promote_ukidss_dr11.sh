#!/bin/bash
# Promote ready UKIDSS DR11 programmes (LAS/GCS/DXS/UDS) from skyai staging to hats.
set -euo pipefail
KP=/arc/projects/k-pop/pensus
CODE=$KP/code
LOG=$KP/logs/promote_ukidss_dr11_$(date -u +%Y%m%dT%H%M%SZ).log
exec > >(tee -a "$LOG") 2>&1
echo "start $(date -Is) UKIDSS DR11 promote batch"

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
# ~8 GiB staged total + margin + 300 GiB floor
need=int(1.1 * 9 * (2**30)) + 300*(2**30)
print(f"hats free={free} need>={need} quota={q}")
if free < need:
  raise SystemExit(f"FAIL hats free {free} < need {need}")
print("margin_ok")
PY

promote_one() {
  local prog=$1 rows=$2 parts=$3
  local src=/arc/projects/skyai/pensus/hats_staging/ukidss/dr11_${prog}
  local dst=/arc/projects/hats/ukidss/dr11_${prog}
  local alias=ukidss_dr11_${prog}_hats
  if [[ -e "$dst/properties" || -e "$dst/hats.properties" ]]; then
    echo "SKIP already promoted $dst"
    bash "$CODE/apply_hats_public_acl.sh" "$dst" || true
    return 0
  fi
  echo "PROMOTE $src -> $dst rows=$rows parts=$parts"
  bash "$CODE/promote_hats_tree.sh" "$src" "$dst" "$alias" "$rows" "$parts"
  bash "$CODE/apply_hats_public_acl.sh" "$dst"
}

# Parent survey ACL once
mkdir -p /arc/projects/hats/ukidss
bash "$CODE/apply_hats_public_acl.sh" /arc/projects/hats/ukidss || true

promote_one las 77685267 188
promote_one gcs 64835919 151
promote_one dxs 2992865 4
promote_one uds 296026 1

echo "UKIDSS batch done $(date -Is) LOG=$LOG"
