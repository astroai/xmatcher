#!/bin/bash
set -euo pipefail
# Stage LoTSS DR3 v1.0 source catalogue (*.srl FITS) → hats-import on mlao →
# promote to /arc/projects/hats/lotss/dr3 + public ACL.
# Skip DR1/DR2 and Gaussian *.gaul as primary. No Ray. CephFS TMP only.
# Do not disturb y0k6nhqc (HSC), n662j3fl (ATLAS), dzos94x0 (Hubble).

KP=/arc/projects/k-pop/pensus
CODE=$KP/code
HOST=/arc/projects/mlao
STAGE=$HOST/pensus/staging/lotss_dr3
TMP=$HOST/pensus/tmp/lotss_dr3
BUILD=$HOST/pensus/hats_staging/lotss/_build
DST_TEMP=$HOST/pensus/hats_staging/lotss/dr3
PLANNED=/arc/projects/hats/lotss/dr3
ALIAS=lotss_dr3_hats
POINTER=$KP/inputs/$ALIAS/SOURCE.json
FITS_URL=https://lofar-surveys.org/public/DR3/catalogues/LoTSS_DR3_v1.0.srl.fits
FITS=$STAGE/fits/LoTSS_DR3_v1.0.srl.fits
EXPECTED_MIN_ROWS=13600000
EXPECTED_MAX_ROWS=13700000
LOG=$KP/logs/lotss_dr3_stage_promote_$(date -u +%Y%m%dT%H%M%SZ).log

mkdir -p "$STAGE/fits" "$TMP" "$BUILD" "$(dirname "$DST_TEMP")" \
  "$KP/inputs/$ALIAS" "$KP/logs" "$(dirname "$PLANNED")"
export TMPDIR=$TMP
export PENSUS_PYDEPS=${PENSUS_PYDEPS:-$TMP/pydeps}

exec > >(tee -a "$LOG") 2>&1
echo "start $(date -Is) LoTSS DR3 srl stage+build+promote host=$(hostname) nproc=$(nproc)"
echo "STAGE=$STAGE TMP=$TMP PLANNED=$PLANNED LOG=$LOG"
echo "NOTE: skip DR1/DR2 and *.gaul; leave y0k6nhqc n662j3fl dzos94x0 alone"

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
# FITS ~2.84 GiB + HATS out ~few GiB + TMP headroom — require ≥400 GiB mlao, ≥50 GiB hats
(( FREE > 400 * 2**30 )) || { echo "FAIL mlao free $((FREE/2**30)) GiB < 400"; exit 1; }
(( HFREE > 50 * 2**30 )) || { echo "FAIL hats free $((HFREE/2**30)) GiB < 50"; exit 1; }

if [[ -f "$PLANNED/properties" || -f "$PLANNED/hats.properties" ]]; then
  echo "note: $PLANNED already live — re-applying ACL only"
  bash "$CODE/apply_hats_public_acl.sh" "$(dirname "$PLANNED")" "$PLANNED"
  echo "done already-live $(date -Is)"
  exit 0
fi

# ---- download FITS (resume) ----
echo "download $FITS_URL"
# Prefer curl -C -; fall back to python urllib resume
if command -v curl >/dev/null 2>&1; then
  curl -L --fail --retry 5 --retry-delay 10 -C - -o "$FITS" "$FITS_URL"
else
  python3 - "$FITS" "$FITS_URL" <<'PY'
import pathlib, sys, urllib.request
dest, url = pathlib.Path(sys.argv[1]), sys.argv[2]
dest.parent.mkdir(parents=True, exist_ok=True)
existing = dest.stat().st_size if dest.exists() else 0
headers = {"User-Agent": "pensus-lotss/1.0"}
if existing:
    headers["Range"] = f"bytes={existing}-"
req = urllib.request.Request(url, headers=headers)
mode = "ab" if existing else "wb"
with urllib.request.urlopen(req, timeout=600) as resp, open(dest, mode) as out:
    while True:
        chunk = resp.read(8 * 1024 * 1024)
        if not chunk:
            break
        out.write(chunk)
print("wrote", dest, "bytes", dest.stat().st_size)
PY
fi
BYTES=$(stat -c%s "$FITS")
echo "fits_bytes=$BYTES"
# Expect ~2.84 GiB (allow small CDN variance)
python3 -c "import sys; b=int('$BYTES'); sys.exit(0 if 2.5e9 < b < 3.2e9 else 1)" \
  || { echo "FAIL unexpected FITS size $BYTES"; exit 1; }

# ---- hats-import ----
if [[ -e "$DST_TEMP" && -n "$(ls -A "$DST_TEMP" 2>/dev/null || true)" ]]; then
  echo "note: temp HATS $DST_TEMP exists; will promote if valid"
else
  python3 -m venv --system-site-packages /scratch/lotssenv
  /scratch/lotssenv/bin/python -m pip install --quiet --upgrade pip
  /scratch/lotssenv/bin/python -m pip install --quiet 'hats-import==0.11.1' hats astropy pyarrow polars
  PY=/scratch/lotssenv/bin/python

  # Confirm RA/DEC columns
  "$PY" - "$FITS" <<'PY'
from astropy.table import Table
import sys
t = Table.read(sys.argv[1], memmap=True)
names = set(t.colnames)
print("ncols", len(names), "nrows", len(t))
print("cols_sample", sorted(names)[:40])
for need in ("RA", "DEC"):
    if need not in names:
        # LoTSS sometimes uses RA_ICRS / DEC_ICRS
        alts = [c for c in names if c.upper().replace("_ICRS","") == need or c.upper() == f"{need}_ICRS"]
        print(f"missing {need} alts={alts}")
        if not alts:
            raise SystemExit(f"FAIL missing {need}")
print("schema_ok")
PY

  RA_COL=RA
  DEC_COL=DEC
  SORT_COL=Source_Name
  "$PY" - "$FITS" <<'PY' || true
from astropy.table import Table
import sys
names=set(Table.read(sys.argv[1], memmap=True).colnames)
open("/tmp/lotss_cols.txt","w").write("\n".join(sorted(names)))
PY
  if [[ -f /tmp/lotss_cols.txt ]]; then
    if ! grep -qx 'RA' /tmp/lotss_cols.txt; then
      if grep -qx 'RA_ICRS' /tmp/lotss_cols.txt; then RA_COL=RA_ICRS; DEC_COL=DEC_ICRS; fi
    fi
    if ! grep -qx 'Source_Name' /tmp/lotss_cols.txt; then
      if grep -qx 'SOURCE_NAME' /tmp/lotss_cols.txt; then SORT_COL=SOURCE_NAME
      elif grep -qx 'Source_id' /tmp/lotss_cols.txt; then SORT_COL=Source_id
      else SORT_COL=""; fi
    fi
  fi
  echo "ra=$RA_COL dec=$DEC_COL sort=${SORT_COL:-none}"

  WORKERS=${WORKERS:-4}
  MEMORY_LIMIT=${MEMORY_LIMIT:-12GB}
  SORT_ARGS=()
  if [[ -n "${SORT_COL}" ]]; then
    SORT_ARGS=(--sort "$SORT_COL")
  fi
  "$PY" "$CODE/hats_import_run.py" \
    --inputs "$FITS" --reader fits \
    --name dr3 --out "$BUILD" --tmp "$TMP" \
    --ra "$RA_COL" --dec "$DEC_COL" \
    "${SORT_ARGS[@]}" \
    --pixel-threshold 1000000 \
    --workers "$WORKERS" --memory-limit "$MEMORY_LIMIT"

  N=$("$PY" -c "from pathlib import Path
p=next(q for q in (Path('$BUILD/dr3/hats.properties'), Path('$BUILD/dr3/properties')) if q.exists())
print(next(int(l.split('=',1)[1]) for l in p.read_text().splitlines() if l.startswith('hats_nrows=')))")
  echo "hats_nrows=$N"
  python3 -c "import sys; n=int('$N'); sys.exit(0 if $EXPECTED_MIN_ROWS <= n <= $EXPECTED_MAX_ROWS else 1)" \
    || { echo "FAIL hats_nrows $N outside [$EXPECTED_MIN_ROWS,$EXPECTED_MAX_ROWS]"; exit 1; }
  mv "$BUILD/dr3" "$DST_TEMP"
  rmdir "$BUILD" 2>/dev/null || true
fi

ROWS_LIVE=$(python3 - <<PY
from pathlib import Path
p = next(q for q in (Path("$DST_TEMP")/"properties", Path("$DST_TEMP")/"hats.properties") if q.exists())
print(next(int(l.split("=",1)[1]) for l in p.read_text().splitlines() if l.startswith("hats_nrows=")))
PY
)
PARTS_LIVE=$(python3 -c "
import csv
from pathlib import Path
p=Path('$DST_TEMP')/'partition_info.csv'
print(sum(1 for r in csv.DictReader(open(p)) if r.get('Npix')) if p.exists() else 0)
")
BYTES_HATS=$(du -sb "$DST_TEMP" | cut -f1)
echo "temp_hats rows=$ROWS_LIVE parts=$PARTS_LIVE bytes=$BYTES_HATS"

python3 - "$POINTER" "$DST_TEMP" "$BYTES_HATS" "$ROWS_LIVE" "$PARTS_LIVE" "$PLANNED" "$FITS_URL" <<'PY'
import datetime, json, pathlib, sys
pointer, dst, nbytes, rows, parts, planned, url = sys.argv[1:]
now = datetime.datetime.now(datetime.timezone.utc).isoformat()
prev = json.loads(pathlib.Path(pointer).read_text()) if pathlib.Path(pointer).exists() else {}
out = {
  **prev,
  "alias": "lotss_dr3_hats",
  "survey": "lotss",
  "release": "dr3",
  "source": "LoTSS DR3 v1.0 source catalogue (*.srl FITS)",
  "url": url,
  "hats_path": dst,
  "cache_path": dst,
  "planned_hats_path": planned,
  "nrows": int(rows),
  "partitions": int(parts),
  "bytes": int(nbytes),
  "temporary_host": True,
  "official_mirror": False,
  "note": "Radio sidecar; skip DR1/DR2; gaul not primary",
  "built_at_utc": now,
  "ok": True,
}
pathlib.Path(pointer).parent.mkdir(parents=True, exist_ok=True)
pathlib.Path(pointer).write_text(json.dumps(out, indent=2, sort_keys=True) + "\n")
print("updated", pointer)
PY

bash "$CODE/promote_hats_tree.sh" "$DST_TEMP" "$PLANNED" "$ALIAS" "$ROWS_LIVE" "$PARTS_LIVE"
echo "all done $(date -Is) PLANNED=$PLANNED LOG=$LOG"
