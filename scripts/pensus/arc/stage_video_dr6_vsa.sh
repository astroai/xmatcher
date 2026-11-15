#!/bin/bash
set -euo pipefail
# VIDEO DR6 on unions alongside VHS DR7 (network-bound strip staging; leave VHS alone).
KP=/arc/projects/k-pop/pensus
CODE=$KP/code
HOST=/arc/projects/unions
STAGE=$HOST/pensus/staging/video_dr6
LOG=$KP/logs/video_dr6_stage_$(date -u +%Y%m%dT%H%M%SZ).log
exec > >(tee -a "$LOG") 2>&1
echo "start $(date -Is) host=$(hostname) pin=VIDEODR6/VIDEOv20211014 programmeID=150"

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

mkdir -p "$STAGE" "$KP/inputs/video_dr6_hats"
python3 "$CODE/stage_video_dr6_vsa.py" \
  --stage "$STAGE" \
  --pointer "$KP/inputs/video_dr6_hats/SOURCE.json" \
  --planned-hats /arc/projects/hats/video/dr6 \
  --ra-step 1.0 \
  --timeout 3600
echo "done $(date -Is)"
