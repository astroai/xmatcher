#!/bin/bash
set -euo pipefail
# Sync CDS SDSS DR16 imaging HATS → k-pop cache → promote to hats/sdss/dr16 + public ACL.
# Do NOT hats-import from SAS. Prefer existing public HATS (xmatcher alias sdss_dr16_hats).
KP=/arc/projects/k-pop/pensus
CODE=$KP/code
CACHE_ROOT=${XMATCHER_CACHE_ROOT:-$KP/inputs/_cache}
DST=/arc/projects/hats/sdss/dr16
ALIAS=sdss_dr16_hats
LOG=$KP/logs/sdss_dr16_sync_promote_$(date -u +%Y%m%dT%H%M%SZ).log
exec > >(tee -a "$LOG") 2>&1
echo "start $(date -Is) sync+promote SDSS DR16 from CDS HATS"

# Preflight: CDS must answer properties (mirrors-first rule exception: only public imaging HATS is CDS).
python3 - <<'PY'
import urllib.request, sys
url = "https://vizcat.cds.unistra.fr/hats/sdss_dr16/main/properties"
try:
    req = urllib.request.Request(url, headers={"User-Agent": "pensus/1.0"})
    with urllib.request.urlopen(req, timeout=120) as r:
        text = r.read(4000).decode("utf-8", "replace")
    print(text[:800])
    if "hats_nrows" not in text and "obs_collection" not in text:
        raise SystemExit("FAIL CDS properties unrecognizable")
except Exception as e:
    print(f"FAIL CDS unreachable for SDSS DR16 HATS: {type(e).__name__}: {e}", flush=True)
    raise SystemExit(2)
PY

export PENSUS_SYNC_ALIASES=sdss_dr16_hats
export PENSUS_SYNC_THREADS=${PENSUS_SYNC_THREADS:-8}
export PENSUS_SYNC_RATE_LIMIT=${PENSUS_SYNC_RATE_LIMIT:-0}
export PENSUS_SYNC_MIN_FREE_GB=${PENSUS_SYNC_MIN_FREE_GB:-200}
export XMATCHER_CACHE_ROOT="$CACHE_ROOT"
export PENSUS_PYDEPS=${PENSUS_PYDEPS:-/scratch/pensus-pydeps}
export PENSUS_WORK_ROOT=$KP
export PENSUS_CODE_ROOT=$CODE

python3 "$CODE/sync_launch.py"
echo "sync finished $(date -Is)"

# Resolve synced tree root under _cache
SRC=$(python3 - <<'PY'
import json, pathlib
cache = pathlib.Path("/arc/projects/k-pop/pensus/inputs/_cache")
pointer = pathlib.Path("/arc/projects/k-pop/pensus/inputs/sdss_dr16_hats/SOURCE.json")
cands = []
if pointer.exists():
    d = json.loads(pointer.read_text())
    for key in ("hats_path", "cache_path", "path"):
        p = d.get(key)
        if p and (pathlib.Path(p) / "properties").exists():
            print(p); raise SystemExit
        if p and (pathlib.Path(p) / "hats.properties").exists():
            print(p); raise SystemExit
# discover hats-http-* under sdss*
for root in cache.glob("*sdss*"):
    for props in list(root.rglob("properties")) + list(root.rglob("hats.properties")):
        cands.append(str(props.parent))
if not cands:
    raise SystemExit("FAIL no SDSS HATS tree found under _cache after sync")
# prefer deepest / most recently modified
cands.sort(key=lambda p: pathlib.Path(p).stat().st_mtime, reverse=True)
print(cands[0])
PY
)
echo "SRC=$SRC"

ROWS=$(python3 - <<PY
from pathlib import Path
p = next(q for q in (Path("$SRC")/"properties", Path("$SRC")/"hats.properties") if q.exists())
print(next(int(l.split("=",1)[1]) for l in p.read_text().splitlines() if l.startswith("hats_nrows=")))
PY
)
PARTS=$(python3 -c "import csv; print(sum(1 for r in csv.DictReader(open('$SRC/partition_info.csv')) if r.get('Npix')))")
echo "rows=$ROWS parts=$PARTS"

# Margin gate: estsize if present else rows-based ~0.5–0.8 TiB fallback
python3 - <<PY
import os, subprocess
from pathlib import Path
def ceph_bytes(path, attr):
  try:
    return int(os.getxattr(path, attr))
  except Exception:
    return int(subprocess.check_output(["getfattr","--only-values","-n",attr,path], text=True).strip() or 0)
free = ceph_bytes("/arc/projects/hats","ceph.quota.max_bytes") - ceph_bytes("/arc/projects/hats","ceph.dir.rbytes")
props = next(q for q in (Path("$SRC")/"properties", Path("$SRC")/"hats.properties") if q.exists())
est = None
for line in props.read_text().splitlines():
  if line.startswith("hats_estsize="):
    # hats_estsize is KB
    est = int(line.split("=",1)[1]) * 1024
    break
size = est if est else int(0.6 * 2**40)  # ~0.6 TiB fallback for 1.23B-row imaging
need = int(size * 1.1) + 300 * 2**30
print(f"hats_free_gib={free/2**30:.0f} need_gib={need/2**30:.0f} src_est_gib={(size or 0)/2**30:.0f}")
if free < need:
  raise SystemExit("FAIL hats free margin for SDSS DR16")
PY

export ROWS PARTS
bash "$CODE/promote_hats_tree.sh" "$SRC" "$DST" "$ALIAS" "$ROWS" "$PARTS"
echo "all done $(date -Is) DST=$DST LOG=$LOG"
