#!/bin/bash
set -uo pipefail
# Euclid DR1 wide is already copied to hats, manifest-checked, verified and repointed.
# Deep-QA the hats catalogue, then drop the k-pop copy.
KP=/arc/projects/k-pop/pensus
CODE=$KP/code
SRC=$KP/inputs/_cache/euclid_dr1/mer_catalogue_wide_survey
DST=/arc/projects/hats/euclid/dr1/mer_catalogue_wide_survey
CAT=$DST/euclid_dr1_reformatted_catalogs_mer_catalogue_wide_survey-hats
LOG=$KP/logs/qa_then_drop_euclid_wide_$(date -u +%Y%m%dT%H%M%SZ).log
exec > >(tee -a "$LOG") 2>&1
echo "start $(date -Is)"

manifest() { (cd "$1" && find . -type f ! -name 'PENSUS_*.json' ! -path './sync.json' -printf '%P %s\n' | LC_ALL=C sort); }
manifest "$SRC" > /scratch/a.txt; manifest "$DST" > /scratch/b.txt
cmp -s /scratch/a.txt /scratch/b.txt || { echo "FAIL manifest"; exit 1; }
BYTES=$(awk '{s+=$NF} END{print s+0}' /scratch/a.txt)

python3 -m venv --system-site-packages /scratch/qaenv
/scratch/qaenv/bin/python -m pip install --quiet hats astropy pyarrow polars cdshealpix pyyaml
export PYTHONPATH="$CODE/vendor/xmatcher/src:$CODE"
/scratch/qaenv/bin/python "$CODE/qa_hats_release.py" "$CAT" euclid_dr1_wide_hats 888152165 768 || { echo "FAIL deep QA; k-pop copy kept"; exit 1; }

rm -rf "$SRC"
/scratch/qaenv/bin/python - "$KP/inputs/euclid_dr1_wide_hats/SOURCE.json" "$SRC" "$BYTES" <<'PY'
import datetime, json, pathlib, sys
p = pathlib.Path(sys.argv[1]); d = json.loads(p.read_text())
d["kpop_removed"] = [sys.argv[2]]; d["kpop_removed_bytes"] = int(sys.argv[3])
d["kpop_removed_at_utc"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
p.write_text(json.dumps(d, indent=2, sort_keys=True) + "\n")
PY
echo "DELETED $SRC bytes=$BYTES"
echo "all done $(date -Is)"
