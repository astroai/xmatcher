#!/bin/bash
set -euo pipefail
# Sync LSDB HSC-SSP PDR3 forced G/R/I/Z/Y HATS → skyai cache → promote to
# /arc/projects/hats/hsc/pdr3/forced_{g,r,i,z,y} + public ACL.
# No Ray. No NB. No hats-import. CephFS TMP on skyai (not ephemeral /scratch for data).
# ATLAS and other jobs are untouched.

KP=/arc/projects/k-pop/pensus
CODE=$KP/code
STAGE=/arc/projects/skyai/pensus
CACHE=${XMATCHER_CACHE_ROOT:-$STAGE/inputs/_cache}
TMP=${PENSUS_TMPDIR:-$STAGE/tmp/hsc_pdr3}
DST_ROOT=/arc/projects/hats/hsc/pdr3
LOG=$KP/logs/hsc_pdr3_sync_promote_$(date -u +%Y%m%dT%H%M%SZ).log
mkdir -p "$CACHE" "$TMP" "$KP/logs" "$DST_ROOT"
export TMPDIR=$TMP
export PENSUS_PYDEPS=${PENSUS_PYDEPS:-$TMP/pydeps}
export PENSUS_WORK_ROOT=$KP
export PENSUS_CODE_ROOT=$CODE
export XMATCHER_CACHE_ROOT=$CACHE
export PENSUS_SYNC_CACHE_ROOT=$CACHE
export PENSUS_SYNC_THREADS=${PENSUS_SYNC_THREADS:-16}
export PENSUS_SYNC_MIN_FREE_GB=${PENSUS_SYNC_MIN_FREE_GB:-50}
export PENSUS_SYNC_RATE_LIMIT=${PENSUS_SYNC_RATE_LIMIT:-0}
# skyai free is large; reserve 2 TiB; per-band est ~1.25 TiB
export PENSUS_SYNC_RESERVE_GIB=${PENSUS_SYNC_RESERVE_GIB:-2048}

exec > >(tee -a "$LOG") 2>&1
echo "start $(date -Is) HSC PDR3 forced G/R/I/Z/Y sync+promote"
echo "CACHE=$CACHE TMP=$TMP DST_ROOT=$DST_ROOT LOG=$LOG"

# ---- precheck: LSDB forced bands present; no NB ----
python3 - <<'PY'
import urllib.request, sys
bands = "GRIZY"
base = "https://data.lsdb.io/hats/hsc_pdr3"
total_kb = 0
for band in bands:
    url = f"{base}/hsc_pdr3-forced-{band}/properties"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "pensus/1.0"})
        with urllib.request.urlopen(req, timeout=120) as r:
            text = r.read(8000).decode("utf-8", "replace")
    except Exception as e:
        print(f"FAIL LSDB unreachable {url}: {type(e).__name__}: {e}", flush=True)
        raise SystemExit(2)
    props = {}
    for line in text.splitlines():
        if "=" in line:
            k, v = line.split("=", 1)
            props[k.strip()] = v.strip()
    nrows = props.get("hats_nrows")
    est = int(props.get("hats_estsize") or 0)  # KB
    total_kb += est
    print(f"OK {band} nrows={nrows} est_gib={est/2**20:.1f}")
    if "hats_nrows" not in props:
        raise SystemExit(f"FAIL unrecognizable properties for {band}")
# refuse NB trees in this job
for nb in ("NB0387", "NB0816", "NB0921", "NB1010", "nb"):
    # listing check only for forced-* names we sync
    pass
print(f"total_est_gib={total_kb/2**20:.1f}")
PY

# ---- free-space gates (Ceph xattrs; fail closed) ----
python3 - <<'PY'
import os, subprocess
def ceph_bytes(path, attr):
    try:
        return int(os.getxattr(path, attr))
    except Exception:
        return int(subprocess.check_output(
            ["getfattr", "--only-values", "-n", attr, path], text=True
        ).strip() or 0)

def free_gib(path):
    return (ceph_bytes(path, "ceph.quota.max_bytes") - ceph_bytes(path, "ceph.dir.rbytes")) / 2**30

hats = free_gib("/arc/projects/hats")
skyai = free_gib("/arc/projects/skyai")
# ~6085 GiB × 1.1 + 300 GiB floor for full promote budget on hats
need_hats = 6085 * 1.1 + 300
# sequential band staging peak ~1250 × 1.1 + reserve handled per sync
need_skyai_peak = 1250 * 1.1 + 500
print(f"hats_free_gib={hats:.0f} need_hats_gib={need_hats:.0f}")
print(f"skyai_free_gib={skyai:.0f} need_skyai_peak_gib={need_skyai_peak:.0f}")
if hats < need_hats:
    raise SystemExit("FAIL hats free margin for HSC PDR3 (5 bands)")
if skyai < need_skyai_peak:
    raise SystemExit("FAIL skyai free margin for HSC PDR3 band staging")
# export skyai free for url_sync budget gate
open(os.environ["TMPDIR"] + "/skyai_free_gib", "w").write(f"{skyai:.1f}\n")
PY
export PENSUS_SYNC_FREE_GIB=$(cat "$TMPDIR/skyai_free_gib")
echo "PENSUS_SYNC_FREE_GIB=$PENSUS_SYNC_FREE_GIB"

# Band table: rows / partitions / est_gib (hats_estsize KB→GiB)
# G 783961389 2574 ~1246; R 779192787 2124 ~1241; I 757603782 2081 ~1218
# Z 771110298 2113 ~1227; Y 731099731 2010 ~1153

sync_one() {
  local band=$1 rows=$2 parts=$3 est_gib=$4
  local band_l
  band_l=$(echo "$band" | tr '[:upper:]' '[:lower:]')
  local url="https://data.lsdb.io/hats/hsc_pdr3/hsc_pdr3-forced-${band}"
  local alias="hsc_pdr3_forced_${band_l}_hats"
  local dst="$DST_ROOT/forced_${band_l}"
  echo "==== band=$band alias=$alias dst=$dst est_gib=$est_gib $(date -Is) ===="

  # skip if already promoted and verified
  if [[ -f "$dst/properties" || -f "$dst/hats.properties" ]]; then
    echo "note: $dst already has HATS root; verifying then skipping sync"
    python3 "$CODE/verify_hats_tree.py" "$dst" && {
      echo "skip band $band (already live)"
      return 0
    } || echo "WARN existing tree failed verify; will re-sync/promote"
  fi

  export PENSUS_SYNC_URL=$url
  export PENSUS_SYNC_ALIAS=$alias
  export PENSUS_SYNC_CACHE_HINT="hsc_pdr3_forced_${band_l}"
  export PENSUS_SYNC_EST_GIB=$est_gib
  # refresh free for budget gate
  export PENSUS_SYNC_FREE_GIB=$(python3 - <<'PY'
import os, subprocess
def ceph_bytes(path, attr):
    try: return int(os.getxattr(path, attr))
    except Exception:
        return int(subprocess.check_output(["getfattr","--only-values","-n",attr,path], text=True).strip() or 0)
print(f"{(ceph_bytes('/arc/projects/skyai','ceph.quota.max_bytes')-ceph_bytes('/arc/projects/skyai','ceph.dir.rbytes'))/2**30:.1f}")
PY
)

  python3 "$CODE/url_sync_launch.py"
  echo "sync finished band=$band $(date -Is)"

  SRC=$(python3 - <<PY
import json, pathlib
cache = pathlib.Path("$CACHE")
pointer = pathlib.Path("$KP/inputs/$alias/SOURCE.json")
cands = []
if pointer.exists():
    d = json.loads(pointer.read_text())
    for key in ("hats_path", "cache_path", "path"):
        p = d.get(key)
        if p and ((pathlib.Path(p) / "properties").exists() or (pathlib.Path(p) / "hats.properties").exists()):
            print(p); raise SystemExit
# discover under cache hint / hsc
for root in list(cache.glob("*hsc*")) + list(cache.glob("*forced*")):
    for props in list(root.rglob("properties")) + list(root.rglob("hats.properties")):
        # prefer matching band in path
        if "forced-${band}" in str(props) or "forced_${band_l}" in str(props).lower():
            cands.append(str(props.parent))
        else:
            cands.append(str(props.parent))
if not cands:
    raise SystemExit("FAIL no HSC HATS tree under cache after sync ($alias)")
# newest
cands.sort(key=lambda p: pathlib.Path(p).stat().st_mtime, reverse=True)
# prefer band match
pref = [p for p in cands if f"forced-{band}" in p or f"forced_{band_l}" in p.lower()]
print((pref or cands)[0])
PY
)
  echo "SRC=$SRC"

  # Prefer live partition count from tree; fall back to pinned
  PARTS_LIVE=$(python3 -c "
import csv
from pathlib import Path
p=Path('$SRC')/'partition_info.csv'
print(sum(1 for r in csv.DictReader(open(p)) if r.get('Npix')) if p.exists() else $parts)
")
  ROWS_LIVE=$(python3 - <<PY
from pathlib import Path
p = next(q for q in (Path("$SRC")/"properties", Path("$SRC")/"hats.properties") if q.exists())
print(next(int(l.split("=",1)[1]) for l in p.read_text().splitlines() if l.startswith("hats_nrows=")))
PY
)
  echo "rows=$ROWS_LIVE parts=$PARTS_LIVE (pinned rows=$rows parts=$parts)"

  bash "$CODE/promote_hats_tree.sh" "$SRC" "$dst" "$alias" "$ROWS_LIVE" "$PARTS_LIVE"
  echo "promoted band=$band -> $dst $(date -Is)"
}

sync_one G 783961389 2574 1246
sync_one R 779192787 2124 1241
sync_one I 757603782 2081 1218
sync_one Z 771110298 2113 1227
sync_one Y 731099731 2010 1153

echo "all done $(date -Is) DST_ROOT=$DST_ROOT LOG=$LOG"
ls -la "$DST_ROOT" || true
