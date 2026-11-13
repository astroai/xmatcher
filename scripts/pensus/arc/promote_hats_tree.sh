#!/bin/bash
set -uo pipefail
# Usage: promote_hats_tree.sh <kpop_src_root> <hats_dst_root> <alias> <expected_rows> <expected_partitions> [need_gib]
# Copy a verified k-pop HATS catalogue root to /arc/projects/hats/<survey>/<release>, deep-QA it,
# repoint inputs/<alias>/SOURCE.json, then drop the k-pop copy.
SRC=$1 DST=$2 ALIAS=$3 ROWS=$4 PARTS=$5
KP=/arc/projects/k-pop/pensus
CODE=$KP/code
LOG=$KP/logs/promote_$(basename "$(dirname "$DST")")_$(basename "$DST")_$(date -u +%Y%m%dT%H%M%SZ).log
exec > >(tee -a "$LOG") 2>&1
echo "start $(date -Is) $SRC -> $DST"

manifest() { (cd "$1" && find . -type f ! -name 'PENSUS_*.json' ! -path './sync.json' -printf '%P %s\n' | LC_ALL=C sort); }

test -f "$SRC/properties" -o -f "$SRC/hats.properties" || { echo "FAIL: no HATS root at $SRC"; exit 1; }
[[ -e "$DST" && -n "$(ls -A "$DST" 2>/dev/null)" ]] && echo "note: $DST exists; rsync resumes into it"
mkdir -p "$DST"
rsync -a --exclude sync.json "$SRC/" "$DST/" || { echo "FAIL rsync"; exit 1; }
manifest "$SRC" > /scratch/a.txt; manifest "$DST" > /scratch/b.txt
cmp -s /scratch/a.txt /scratch/b.txt || { echo "FAIL manifest"; diff /scratch/a.txt /scratch/b.txt | head; exit 1; }
BYTES=$(awk '{s+=$NF} END{print s+0}' /scratch/a.txt)
echo "copied files=$(wc -l < /scratch/a.txt) bytes=$BYTES"

VENV=/scratch/qaenv
python3 -m venv --system-site-packages "$VENV"
"$VENV/bin/python" -m pip install --quiet hats astropy pyarrow polars cdshealpix pyyaml
PY="$VENV/bin/python"
export PYTHONPATH="$CODE/vendor/xmatcher/src:$CODE"

"$PY" "$CODE/verify_hats_tree.py" "$DST" || { echo "FAIL verify"; exit 1; }

"$PY" - "$KP/inputs/$ALIAS/SOURCE.json" "$DST" <<'PY'
import datetime, json, pathlib, sys
p, dst = pathlib.Path(sys.argv[1]), sys.argv[2]
d = json.loads(p.read_text()) if p.is_file() else {"alias": p.parent.name}
for k in ("cache_alias_hint", "cache_root", "cmd", "stdout_log", "stderr_log"):
    d.pop(k, None)
d.update(promoted_hats_path=dst, hats_path=dst, official_mirror=True, hats_layout="survey/release",
         promoted_at_utc=datetime.datetime.now(datetime.timezone.utc).isoformat())
p.parent.mkdir(parents=True, exist_ok=True)
p.write_text(json.dumps(d, indent=2, sort_keys=True) + "\n")
print("updated", p)
PY

# Hide the k-pop copy from the resolver during QA so the pointer check exercises the hats path.
# The resolver globs every child of _cache/<alias>, so the hidden copy must live outside it.
HIDDEN="$KP/staging/_promoting/$ALIAS/$(basename "$SRC")"
mkdir -p "$(dirname "$HIDDEN")"
mv "$SRC" "$HIDDEN"
if ! "$PY" "$CODE/qa_hats_release.py" "$DST" "$ALIAS" "$ROWS" "$PARTS"; then
  mv "$HIDDEN" "$SRC"; echo "FAIL deep QA; k-pop copy kept"; exit 1
fi
rm -rf "$HIDDEN"
CACHE_PARENT=$(dirname "$SRC")
rmdir "$CACHE_PARENT" 2>/dev/null || true
"$PY" - "$KP/inputs/$ALIAS/SOURCE.json" "$SRC" "$BYTES" <<'PY'
import datetime, json, pathlib, sys
p = pathlib.Path(sys.argv[1]); d = json.loads(p.read_text())
d["kpop_removed"] = [sys.argv[2]]; d["kpop_removed_bytes"] = int(sys.argv[3])
d["kpop_removed_at_utc"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
p.write_text(json.dumps(d, indent=2, sort_keys=True) + "\n")
PY
echo "DELETED $SRC bytes=$BYTES"
echo "all done $(date -Is)"
