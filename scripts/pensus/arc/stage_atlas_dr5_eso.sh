#!/bin/bash
# Stage VST ATLAS DR5/ER5 multi-band catalogue tiles (anonymous ESO ObsCore + dataportal) on skyai.
set -euo pipefail
KP=/arc/projects/k-pop/pensus
CODE=$KP/code
HOST=/arc/projects/skyai
STAGE=$HOST/pensus/staging/atlas_dr5_eso
LOG=$KP/logs/atlas_dr5_eso_stage_$(date -u +%Y%m%dT%H%M%SZ).log
exec > >(tee -a "$LOG") 2>&1
echo "start $(date -Is) host=$(hostname)"
mkdir -p "$STAGE/tiles" "$KP/inputs/atlas_dr5_hats" "$HOST/pensus"
: > "$HOST/pensus/.pensus_write_ok" || true
ceph_bytes() {
  python3 -c 'import os,subprocess,sys
path,attr=sys.argv[1],sys.argv[2]
try:
  print(int(os.getxattr(path,attr)))
except OSError:
  try:
    out=subprocess.check_output(["getfattr","--only-values","-n",attr,path],text=True).strip()
    print(int(out) if out else 0)
  except Exception:
    print(0)' "$1" "$2"
}
Q=$(ceph_bytes /arc/projects/skyai ceph.quota.max_bytes)
U=$(ceph_bytes /arc/projects/skyai ceph.dir.rbytes)
echo "skyai free_gib=$(( (Q-U)/2**30 ))"
# ~118 GiB catalog FITS + margin; require >= 800 GiB free for stage alone
(( Q-U > 800*2**30 )) || { echo FAIL skyai free; exit 1; }
python3 "$CODE/stage_atlas_dr5_eso.py" \
  --stage "$STAGE" \
  --pointer "$KP/inputs/atlas_dr5_hats/SOURCE.json" \
  --planned-hats /arc/projects/hats/atlas/dr5 \
  --timeout 600
echo "done $(date -Is) LOG=$LOG"
