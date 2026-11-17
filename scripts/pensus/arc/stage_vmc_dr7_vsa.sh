#!/bin/bash
set -euo pipefail
# VMC DR7 on unions alongside VHS DR7 (network-bound strip staging; leave VHS alone).
KP=/arc/projects/k-pop/pensus
CODE=$KP/code
HOST=/arc/projects/unions
STAGE=$HOST/pensus/staging/vmc_dr7
LOG=$KP/logs/vmc_dr7_stage_$(date -u +%Y%m%dT%H%M%SZ).log
exec > >(tee -a "$LOG") 2>&1
echo "start $(date -Is) host=$(hostname) pin=VMCDR7/VMCv20250130 programmeID=130"

free_bytes="$(python3 - <<'PY'
import os, subprocess
path = b"/arc/projects/unions"
try:
    q = int(os.getxattr(path, b"ceph.quota.max_bytes"))
    u = int(os.getxattr(path, b"ceph.dir.rbytes"))
except OSError:
    def gf(attr):
        return int(subprocess.check_output(
            ["getfattr", "--only-values", "-n", attr, "/arc/projects/unions"], text=True
        ).strip())
    q, u = gf("ceph.quota.max_bytes"), gf("ceph.dir.rbytes")
print(q - u)
PY
)"
echo "unions free=$free_bytes"
# Keep multi-TiB headroom for concurrent VHS DR7 staging
(( free_bytes > 5000 * 2**30 )) || { echo FAIL unions free; exit 1; }

mkdir -p "$STAGE" "$KP/inputs/vmc_dr7_hats"
python3 "$CODE/stage_vmc_dr7_vsa.py" \
  --stage "$STAGE" \
  --pointer "$KP/inputs/vmc_dr7_hats/SOURCE.json" \
  --planned-hats /arc/projects/hats/vmc/dr7 \
  --ra-step 1.0 \
  --timeout 3600
echo "done $(date -Is)"
