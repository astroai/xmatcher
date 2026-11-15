#!/bin/bash
set -euo pipefail
# VIKING DR5 on unions alongside VHS DR7 (network-bound strip staging; leave VHS alone).
KP=/arc/projects/k-pop/pensus
CODE=$KP/code
HOST=/arc/projects/unions
STAGE=$HOST/pensus/staging/viking_dr5
LOG=$KP/logs/viking_dr5_stage_$(date -u +%Y%m%dT%H%M%SZ).log
exec > >(tee -a "$LOG") 2>&1
echo "start $(date -Is) host=$(hostname) pin=VIKINGDR5/VIKINGv20181012 programmeID=140"

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

mkdir -p "$STAGE" "$KP/inputs/viking_dr5_hats"
python3 "$CODE/stage_viking_dr5_vsa.py" \
  --stage "$STAGE" \
  --pointer "$KP/inputs/viking_dr5_hats/SOURCE.json" \
  --planned-hats /arc/projects/hats/viking/dr5 \
  --ra-step 1.0 \
  --timeout 3600
echo "done $(date -Is)"
