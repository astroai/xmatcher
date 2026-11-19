#!/bin/bash
set -euo pipefail
# Sync LSDB eROSITA-DE DR2 eRASS:3 Main v1.3 HATS → mlao cache → promote
# to /arc/projects/hats/erosita/erass3 + public ACL.
# Skip obsolete eRASS1 / LSDB hats/erosita/. No hats-import. No Ray.
# Do not disturb y0k6nhqc (HSC), n662j3fl (ATLAS), dzos94x0 (Hubble).

KP=/arc/projects/k-pop/pensus
CODE=$KP/code
HOST=/arc/projects/mlao
CACHE=${XMATCHER_CACHE_ROOT:-$HOST/pensus/inputs/_cache}
TMP=${PENSUS_TMPDIR:-$HOST/pensus/tmp/erosita_erass3}
DST=/arc/projects/hats/erosita/erass3
ALIAS=erosita_erass3_hats
URL=https://data.lsdb.io/hats/erosita_dr2_erass3/erass3_main
EXPECTED_ROWS=1975540
EXPECTED_PARTS=44
EST_GIB=1.13
LOG=$KP/logs/erosita_erass3_sync_promote_$(date -u +%Y%m%dT%H%M%SZ).log

mkdir -p "$CACHE" "$TMP" "$KP/logs" "$KP/inputs/$ALIAS" "$(dirname "$DST")"
export TMPDIR=$TMP
export PENSUS_PYDEPS=${PENSUS_PYDEPS:-$TMP/pydeps}
export PENSUS_WORK_ROOT=$KP
export PENSUS_CODE_ROOT=$CODE
export XMATCHER_CACHE_ROOT=$CACHE
export PENSUS_SYNC_CACHE_ROOT=$CACHE
export PENSUS_SYNC_THREADS=${PENSUS_SYNC_THREADS:-8}
export PENSUS_SYNC_MIN_FREE_GB=${PENSUS_SYNC_MIN_FREE_GB:-20}
export PENSUS_SYNC_RATE_LIMIT=${PENSUS_SYNC_RATE_LIMIT:-0}
export PENSUS_SYNC_RESERVE_GIB=${PENSUS_SYNC_RESERVE_GIB:-300}

exec > >(tee -a "$LOG") 2>&1
echo "start $(date -Is) eROSITA eRASS:3 sync+promote host=$(hostname) nproc=$(nproc)"
echo "CACHE=$CACHE TMP=$TMP DST=$DST URL=$URL LOG=$LOG"
echo "NOTE: skip eRASS1/DR1; leave y0k6nhqc n662j3fl dzos94x0 alone"

# ---- precheck LSDB primary table only ----
python3 - <<'PY'
import urllib.request
url = "https://data.lsdb.io/hats/erosita_dr2_erass3/erass3_main/properties"
req = urllib.request.Request(url, headers={"User-Agent": "pensus/1.0"})
with urllib.request.urlopen(req, timeout=120) as r:
    text = r.read(8000).decode("utf-8", "replace")
props = {}
for line in text.splitlines():
    if "=" in line:
        k, v = line.split("=", 1)
        props[k.strip()] = v.strip()
print(text[:600])
nrows = int(props.get("hats_nrows") or 0)
if nrows != 1975540:
    raise SystemExit(f"FAIL unexpected hats_nrows={nrows}")
print(f"OK erass3_main nrows={nrows} est_kb={props.get('hats_estsize')}")
# Refuse obsolete DR1 collection if accidentally requested
bad = "https://data.lsdb.io/hats/erosita/"
print(f"obsolete_skip={bad}")
PY

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

MMAX=$(ceph_bytes /arc/projects/mlao ceph.quota.max_bytes)
MUSED=$(ceph_bytes /arc/projects/mlao ceph.dir.rbytes)
FREE=$((MMAX-MUSED))
HMAX=$(ceph_bytes /arc/projects/hats ceph.quota.max_bytes)
HUSED=$(ceph_bytes /arc/projects/hats ceph.dir.rbytes)
HFREE=$((HMAX-HUSED))
echo "mlao_free_gib=$((FREE/2**30)) hats_free_gib=$((HFREE/2**30))"
# tiny catalog; require ≥400 GiB mlao and ≥50 GiB hats headroom
(( FREE > 400 * 2**30 )) || { echo "FAIL mlao free $((FREE/2**30)) GiB < 400"; exit 1; }
(( HFREE > 50 * 2**30 )) || { echo "FAIL hats free $((HFREE/2**30)) GiB < 50"; exit 1; }

if [[ -f "$DST/properties" || -f "$DST/hats.properties" ]]; then
  ROWS_LIVE=$(python3 - <<PY
from pathlib import Path
p = next(q for q in (Path("$DST")/"properties", Path("$DST")/"hats.properties") if q.exists())
print(next(int(l.split("=",1)[1]) for l in p.read_text().splitlines() if l.startswith("hats_nrows=")))
PY
)
  echo "note: $DST already live nrows=$ROWS_LIVE — re-applying ACL only"
  bash "$CODE/apply_hats_public_acl.sh" "$(dirname "$DST")" "$DST"
  echo "done already-live $(date -Is)"
  exit 0
fi

find_src() {
  python3 - "$CACHE" "$KP" "$ALIAS" <<'PY'
import json, pathlib, sys
cache = pathlib.Path(sys.argv[1])
kp = pathlib.Path(sys.argv[2])
alias = sys.argv[3]
pointer = kp / "inputs" / alias / "SOURCE.json"
needles = ("erass3_main", "erosita_dr2_erass3", "erosita_erass3")

def ok(path: pathlib.Path) -> bool:
    s = str(path)
    return any(n in s for n in needles) and (
        (path / "properties").exists() or (path / "hats.properties").exists()
    )

for hint in ("erass3_main", "erosita_dr2_erass3", "erosita_erass3"):
    direct = cache / hint
    if direct.is_dir():
        cands = [c for c in sorted(direct.glob("hats-http-*")) if ok(c)]
        if cands:
            print(cands[-1])
            raise SystemExit
if pointer.exists():
    d = json.loads(pointer.read_text())
    for key in ("hats_path", "cache_path", "path"):
        p = d.get(key)
        if p and ok(pathlib.Path(p)):
            print(p)
            raise SystemExit
cands = []
for root in list(cache.glob("*erass3*")) + list(cache.glob("*erosita*")):
    for props in list(root.rglob("properties")) + list(root.rglob("hats.properties")):
        if ok(props.parent):
            cands.append(str(props.parent))
if not cands:
    raise SystemExit("FAIL no eRASS3 HATS under cache")
cands.sort(key=lambda p: pathlib.Path(p).stat().st_mtime, reverse=True)
print(cands[0])
PY
}

skip_sync=0
if SRC_EXISTING=$(find_src 2>/dev/null); then
  have_gib=$(python3 -c "import os; print(sum(os.path.getsize(os.path.join(r,f)) for r,ds,fs in os.walk('$SRC_EXISTING') for f in fs)/2**30)")
  echo "cache hit SRC=$SRC_EXISTING have_gib=$have_gib"
  if python3 -c "import sys; sys.exit(0 if float('$have_gib') >= 0.95*float('$EST_GIB') else 1)"; then
    skip_sync=1
  fi
else
  echo "no cache tree yet"
fi

if [[ "$skip_sync" -eq 0 ]]; then
  export PENSUS_SYNC_URL=$URL
  export PENSUS_SYNC_ALIAS=$ALIAS
  export PENSUS_SYNC_CACHE_HINT=erass3_main
  export PENSUS_SYNC_EST_GIB=$EST_GIB
  export PENSUS_SYNC_FREE_GIB=$(python3 - <<'PY'
import os, subprocess
def ceph_bytes(path, attr):
    try: return int(os.getxattr(path, attr))
    except Exception:
        return int(subprocess.check_output(["getfattr","--only-values","-n",attr,path], text=True).strip() or 0)
print(f"{(ceph_bytes('/arc/projects/mlao','ceph.quota.max_bytes')-ceph_bytes('/arc/projects/mlao','ceph.dir.rbytes'))/2**30:.1f}")
PY
)
  python3 "$CODE/url_sync_launch.py"
  echo "sync finished $(date -Is)"
else
  echo "skip sync (cache ≥95% of est)"
fi

SRC=$(find_src)
echo "SRC=$SRC"
ROWS_LIVE=$(python3 - <<PY
from pathlib import Path
p = next(q for q in (Path("$SRC")/"properties", Path("$SRC")/"hats.properties") if q.exists())
print(next(int(l.split("=",1)[1]) for l in p.read_text().splitlines() if l.startswith("hats_nrows=")))
PY
)
PARTS_LIVE=$(python3 -c "
import csv
from pathlib import Path
p=Path('$SRC')/'partition_info.csv'
print(sum(1 for r in csv.DictReader(open(p)) if r.get('Npix')) if p.exists() else $EXPECTED_PARTS)
")
echo "rows=$ROWS_LIVE parts=$PARTS_LIVE (expected $EXPECTED_ROWS / $EXPECTED_PARTS)"
[[ "$ROWS_LIVE" == "$EXPECTED_ROWS" ]] || { echo "FAIL nrows mismatch"; exit 1; }

python3 - "$KP/inputs/$ALIAS/SOURCE.json" "$SRC" "$ALIAS" "$URL" <<'PY'
import datetime, json, pathlib, sys
p, src, alias, url = pathlib.Path(sys.argv[1]), sys.argv[2], sys.argv[3], sys.argv[4]
d = json.loads(p.read_text()) if p.is_file() else {"alias": alias}
d.update(
    survey="erosita",
    release="erass3",
    hats_path=src,
    cache_path=src,
    url=url,
    planned_hats_path="/arc/projects/hats/erosita/erass3",
    official_mirror=True,
    source="LSDB hats/erosita_dr2_erass3/erass3_main (Main v1.3)",
    note="X-ray sidecar; skip eRASS1",
    ok=True,
    probed_at_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),
)
p.parent.mkdir(parents=True, exist_ok=True)
p.write_text(json.dumps(d, indent=2, sort_keys=True) + "\n")
print("updated", p)
PY

bash "$CODE/promote_hats_tree.sh" "$SRC" "$DST" "$ALIAS" "$ROWS_LIVE" "$PARTS_LIVE"
echo "all done $(date -Is) DST=$DST LOG=$LOG"
