#!/bin/bash
set -euo pipefail
KP=/arc/projects/k-pop/pensus
CODE=$KP/code
HOST=/arc/projects/unions
STAGE=$HOST/pensus/staging/vhs_dr7
LOG=$KP/logs/vhs_dr7_stage_$(date -u +%Y%m%dT%H%M%SZ).log
exec > >(tee -a "$LOG") 2>&1
echo "start $(date -Is) host=$(hostname) pin=VHSDR7/VHSv202407131"
mkdir -p "$STAGE" "$KP/inputs/vhs_dr7_hats" "$HOST/pensus"

free_bytes="$(python3 - <<'PY'
import os
path = b"/arc/projects/unions"
try:
    q = int(os.getxattr(path, b"ceph.quota.max_bytes"))
    u = int(os.getxattr(path, b"ceph.dir.rbytes"))
except OSError:
    # fallback getfattr
    import subprocess
    def gf(attr):
        out = subprocess.check_output(
            ["getfattr", "--only-values", "-n", attr, "/arc/projects/unions"],
            text=True,
        ).strip()
        return int(out)
    q, u = gf("ceph.quota.max_bytes"), gf("ceph.dir.rbytes")
print(q - u)
PY
)"
echo "unions free=$free_bytes"
(( free_bytes > 3000 * 2**30 )) || { echo FAIL unions free; exit 1; }

if [[ -d "$HOST/pensus/staging/vhs_dr5" ]]; then
  echo "deleting abandoned vhs_dr5 staging"
  rm -rf "$HOST/pensus/staging/vhs_dr5"
fi
rm -f "$KP/inputs/vhs_dr5_hats/SOURCE.json" 2>/dev/null || true

python3 "$CODE/stage_vhs_dr7_vsa.py" \
  --stage "$STAGE" \
  --pointer "$KP/inputs/vhs_dr7_hats/SOURCE.json" \
  --planned-hats /arc/projects/hats/vhs/dr7 \
  --ra-step 0.25 \
  --timeout 3600
echo "done $(date -Is)"
