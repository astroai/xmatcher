#!/bin/bash
set -euo pipefail
# Sync LSDB HSC-SSP PDR3 forced G/R/I/Z/Y HATS → skyai cache, then promote to
# /arc/projects/hats/hsc/pdr3/forced_{g,r,i,z,y} + public ACL when hats free allows.
# Default: do NOT promote until hats free ≥ PENSUS_HATS_PROMOTE_MIN_GIB (25000).
# No Ray. No NB. No hats-import. CephFS TMP on skyai.
# ATLAS and other jobs are untouched.

KP=/arc/projects/k-pop/pensus
CODE=$KP/code
STAGE=/arc/projects/skyai/pensus
CACHE=${XMATCHER_CACHE_ROOT:-$STAGE/inputs/_cache}
TMP=${PENSUS_TMPDIR:-$STAGE/tmp/hsc_pdr3}
DST_ROOT=/arc/projects/hats/hsc/pdr3
PROMOTE_MIN_GIB=${PENSUS_HATS_PROMOTE_MIN_GIB:-25000}
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
export PENSUS_SYNC_RESERVE_GIB=${PENSUS_SYNC_RESERVE_GIB:-2048}

exec > >(tee -a "$LOG") 2>&1
echo "start $(date -Is) HSC PDR3 forced G/R/I/Z/Y sync (+promote if hats free≥${PROMOTE_MIN_GIB} GiB)"
echo "CACHE=$CACHE TMP=$TMP DST_ROOT=$DST_ROOT PROMOTE_MIN_GIB=$PROMOTE_MIN_GIB LOG=$LOG"

# ---- precheck: LSDB forced bands present ----
python3 - <<'PY'
import urllib.request
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
    est = int(props.get("hats_estsize") or 0)  # KB
    total_kb += est
    print(f"OK {band} nrows={props.get('hats_nrows')} est_gib={est/2**20:.1f}")
    if "hats_nrows" not in props:
        raise SystemExit(f"FAIL unrecognizable properties for {band}")
print(f"total_est_gib={total_kb/2**20:.1f}")
PY

# ---- free-space: skyai required for sync; hats only gates promote ----
python3 - <<PY
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
need_skyai_peak = 1250 * 1.1 + 500
promote_min = float("$PROMOTE_MIN_GIB")
print(f"hats_free_gib={hats:.0f} promote_min_gib={promote_min:.0f} promote={'YES' if hats >= promote_min else 'NO (sync-only)'}")
print(f"skyai_free_gib={skyai:.0f} need_skyai_peak_gib={need_skyai_peak:.0f}")
if skyai < need_skyai_peak:
    raise SystemExit("FAIL skyai free margin for HSC PDR3 band staging")
open(os.environ["TMPDIR"] + "/skyai_free_gib", "w").write(f"{skyai:.1f}\n")
open(os.environ["TMPDIR"] + "/hats_free_gib", "w").write(f"{hats:.1f}\n")
PY
export PENSUS_SYNC_FREE_GIB=$(cat "$TMPDIR/skyai_free_gib")
HATS_FREE_GIB=$(cat "$TMPDIR/hats_free_gib")
echo "PENSUS_SYNC_FREE_GIB=$PENSUS_SYNC_FREE_GIB HATS_FREE_GIB=$HATS_FREE_GIB"

find_src() {
  local band=$1 band_l=$2 alias=$3
  python3 - "$CACHE" "$KP" "$alias" "$band" "$band_l" <<'PY'
import json, pathlib, sys
cache = pathlib.Path(sys.argv[1])
kp = pathlib.Path(sys.argv[2])
alias, band, band_l = sys.argv[3], sys.argv[4], sys.argv[5]
pointer = kp / "inputs" / alias / "SOURCE.json"
# known sync layout
direct = cache / f"hsc_pdr3-forced-{band}"
for cand in sorted(direct.glob("hats-http-*")) if direct.is_dir() else []:
    if (cand / "properties").exists() or (cand / "hats.properties").exists():
        print(cand)
        raise SystemExit
if pointer.exists():
    d = json.loads(pointer.read_text())
    for key in ("hats_path", "cache_path", "path"):
        p = d.get(key)
        if p and ((pathlib.Path(p) / "properties").exists() or (pathlib.Path(p) / "hats.properties").exists()):
            print(p)
            raise SystemExit
cands = []
needle = f"forced-{band}"
for root in list(cache.glob("*hsc*")) + list(cache.glob("*forced*")):
    for props in list(root.rglob("properties")) + list(root.rglob("hats.properties")):
        cands.append(str(props.parent))
if not cands:
    raise SystemExit(f"FAIL no HSC HATS tree under cache for {alias}")
cands.sort(key=lambda p: pathlib.Path(p).stat().st_mtime, reverse=True)
pref = [p for p in cands if needle in p or f"forced_{band_l}" in p.lower()]
print((pref or cands)[0])
PY
}

sync_one() {
  local band=$1 rows=$2 parts=$3 est_gib=$4
  local band_l
  band_l=$(echo "$band" | tr '[:upper:]' '[:lower:]')
  local url="https://data.lsdb.io/hats/hsc_pdr3/hsc_pdr3-forced-${band}"
  local alias="hsc_pdr3_forced_${band_l}_hats"
  local dst="$DST_ROOT/forced_${band_l}"
  echo "==== band=$band alias=$alias dst=$dst est_gib=$est_gib $(date -Is) ===="

  if [[ -f "$dst/properties" || -f "$dst/hats.properties" ]]; then
    echo "note: $dst already live; skipping"
    return 0
  fi

  # Skip re-sync if cache tree already looks complete (≥95% of est)
  local skip_sync=0
  if SRC_EXISTING=$(find_src "$band" "$band_l" "$alias" 2>/dev/null); then
    local have_gib
    have_gib=$(python3 -c "import os; print(sum(os.path.getsize(os.path.join(r,f)) for r,ds,fs in os.walk('$SRC_EXISTING') for f in fs)/2**30)")
    echo "cache hit SRC=$SRC_EXISTING have_gib=$have_gib"
    python3 -c "import sys; sys.exit(0 if float('$have_gib') >= 0.95*float('$est_gib') else 1)" && skip_sync=1 || true
  fi

  if [[ "$skip_sync" -eq 0 ]]; then
    export PENSUS_SYNC_URL=$url
    export PENSUS_SYNC_ALIAS=$alias
    export PENSUS_SYNC_CACHE_HINT="hsc_pdr3_forced_${band_l}"
    export PENSUS_SYNC_EST_GIB=$est_gib
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
  else
    echo "skip sync band=$band (cache ≥95% of est)"
  fi

  SRC=$(find_src "$band" "$band_l" "$alias")
  echo "SRC=$SRC"

  # Record pointer path for resume
  python3 - "$KP/inputs/$alias/SOURCE.json" "$SRC" "$alias" "$url" <<'PY'
import datetime, json, pathlib, sys
p, src, alias, url = pathlib.Path(sys.argv[1]), sys.argv[2], sys.argv[3], sys.argv[4]
d = json.loads(p.read_text()) if p.is_file() else {"alias": alias}
d.update(
    hats_path=src, cache_path=src, url=url, ok=True,
    probed_at_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),
)
p.parent.mkdir(parents=True, exist_ok=True)
p.write_text(json.dumps(d, indent=2, sort_keys=True) + "\n")
print("updated", p)
PY

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

  HATS_NOW=$(python3 - <<'PY'
import os, subprocess
def ceph_bytes(path, attr):
    try: return int(os.getxattr(path, attr))
    except Exception:
        return int(subprocess.check_output(["getfattr","--only-values","-n",attr,path], text=True).strip() or 0)
print(f"{(ceph_bytes('/arc/projects/hats','ceph.quota.max_bytes')-ceph_bytes('/arc/projects/hats','ceph.dir.rbytes'))/2**30:.1f}")
PY
)
  if python3 -c "import sys; sys.exit(0 if float('$HATS_NOW') >= float('$PROMOTE_MIN_GIB') else 1)"; then
    bash "$CODE/promote_hats_tree.sh" "$SRC" "$dst" "$alias" "$ROWS_LIVE" "$PARTS_LIVE"
    echo "promoted band=$band -> $dst $(date -Is)"
  else
    echo "DEFER promote band=$band: hats_free_gib=$HATS_NOW < promote_min_gib=$PROMOTE_MIN_GIB (tree kept at $SRC)"
  fi
}

sync_one G 783961389 2574 1246
sync_one R 779192787 2124 1241
sync_one I 757603782 2081 1218
sync_one Z 771110298 2113 1227
sync_one Y 731099731 2010 1153

echo "all done $(date -Is) DST_ROOT=$DST_ROOT LOG=$LOG"
ls -la "$DST_ROOT" || true
ls -la "$CACHE" || true
