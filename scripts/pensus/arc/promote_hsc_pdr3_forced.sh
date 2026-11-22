#!/bin/bash
set -euo pipefail
# Promote already-synced LSDB HSC-SSP PDR3 forced G/R/I/Z/Y from skyai cache
# → /arc/projects/hats/hsc/pdr3/forced_{g,r,i,z,y} + public ACL.
# No re-sync. No Ray. No NB. User override: promote even if hats free < 25 TiB,
# but stop cleanly if a single-band margin fails mid-run.
# ATLAS / other jobs are untouched.

KP=/arc/projects/k-pop/pensus
CODE=$KP/code
DST_ROOT=/arc/projects/hats/hsc/pdr3
TMP=${PENSUS_TMPDIR:-/arc/projects/skyai/pensus/tmp/hsc_pdr3_promote}
LOG=$KP/logs/hsc_pdr3_promote_$(date -u +%Y%m%dT%H%M%SZ).log
mkdir -p "$TMP" "$DST_ROOT" "$KP/logs"
export TMPDIR=$TMP
# Keep QA deps on CephFS when possible (promote_hats_tree still uses /scratch for tiny manifests).
export PENSUS_PYDEPS=${PENSUS_PYDEPS:-$TMP/pydeps}

exec > >(tee -a "$LOG") 2>&1
echo "start $(date -Is) HSC PDR3 forced promote-from-cache (user override)"
echo "DST_ROOT=$DST_ROOT TMP=$TMP LOG=$LOG"

hats_free_gib() {
  python3 - <<'PY'
import os, subprocess
def ceph_bytes(path, attr):
    try:
        return int(os.getxattr(path, attr))
    except Exception:
        return int(subprocess.check_output(
            ["getfattr", "--only-values", "-n", attr, path], text=True
        ).strip() or 0)
print(f"{(ceph_bytes('/arc/projects/hats','ceph.quota.max_bytes')-ceph_bytes('/arc/projects/hats','ceph.dir.rbytes'))/2**30:.1f}")
PY
}

# Band: SRC rows parts est_gib
promote_one() {
  local band=$1 src=$2 rows=$3 parts=$4 est_gib=$5
  local band_l dst alias free need
  band_l=$(echo "$band" | tr '[:upper:]' '[:lower:]')
  dst="$DST_ROOT/forced_${band_l}"
  alias="hsc_pdr3_forced_${band_l}_hats"
  echo "==== promote band=$band src=$src dst=$dst est_gib=$est_gib $(date -Is) ===="

  if [[ -f "$dst/properties" || -f "$dst/hats.properties" ]]; then
    echo "note: $dst already live; verifying"
    python3 "$CODE/verify_hats_tree.py" "$dst" && {
      echo "skip band=$band (already promoted)"
      return 0
    }
    echo "WARN existing dest failed verify; will re-promote"
  fi

  test -f "$src/properties" -o -f "$src/hats.properties" || {
    echo "FAIL no HATS root at $src"
    return 1
  }

  free=$(hats_free_gib)
  # 1.1× band + 300 GiB floor for this band
  need=$(python3 -c "print(f'{float('$est_gib')*1.1 + 300:.1f}')")
  echo "hats_free_gib=$free need_band_gib=$need"
  if python3 -c "import sys; sys.exit(0 if float('$free') >= float('$need') else 1)"; then
    :
  else
    echo "STOP: insufficient hats free for band=$band (free=$free need=$need). Earlier bands left as-is."
    return 2
  fi

  bash "$CODE/promote_hats_tree.sh" "$src" "$dst" "$alias" "$rows" "$parts"
  echo "promoted band=$band -> $dst $(date -Is) hats_free_now=$(hats_free_gib)"
}

# Known cache trees from successful sync qpnvl9kg
promote_one G /arc/projects/skyai/pensus/inputs/_cache/hsc_pdr3-forced-G/hats-http-f4642f61bab9 783961389 2574 1246
promote_one R /arc/projects/skyai/pensus/inputs/_cache/hsc_pdr3-forced-R/hats-http-486cf831a898 779192787 2124 1241
promote_one I /arc/projects/skyai/pensus/inputs/_cache/hsc_pdr3-forced-I/hats-http-67301b0e6763 757603782 2081 1218
promote_one Z /arc/projects/skyai/pensus/inputs/_cache/hsc_pdr3-forced-Z/hats-http-e25d9dad0572 771110298 2113 1227
promote_one Y /arc/projects/skyai/pensus/inputs/_cache/hsc_pdr3-forced-Y/hats-http-a9395f2d88e6 731099731 2010 1153

echo "all done $(date -Is) DST_ROOT=$DST_ROOT LOG=$LOG"
ls -la "$DST_ROOT" || true
echo "final_hats_free_gib=$(hats_free_gib)"
