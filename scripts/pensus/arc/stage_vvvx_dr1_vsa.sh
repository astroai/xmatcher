#!/bin/bash
set -euo pipefail
# VVVX DR1 on unions alongside VHS DR7 (network-bound strip staging; leave VHS alone).
# VIRAC2 proper-motion catalog is ESO-only — this stages VSA VVVX photometry only.
KP=/arc/projects/k-pop/pensus
CODE=$KP/code
HOST=/arc/projects/unions
STAGE=$HOST/pensus/staging/vvvx_dr1
LOG=$KP/logs/vvvx_dr1_stage_$(date -u +%Y%m%dT%H%M%SZ).log
exec > >(tee -a "$LOG") 2>&1
echo "start $(date -Is) host=$(hostname) pin=VVVXDR1/vvvxSource programmeID=170"

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

mkdir -p "$STAGE" "$KP/inputs/vvvx_dr1_hats"
python3 "$CODE/stage_vvvx_dr1_vsa.py" \
  --stage "$STAGE" \
  --pointer "$KP/inputs/vvvx_dr1_hats/SOURCE.json" \
  --planned-hats /arc/projects/hats/vvvx/dr1 \
  --ra-step 0.5 \
  --timeout 3600
echo "done $(date -Is)"
