#!/bin/bash
set -euo pipefail
# CatWISE2020 stage-1 Parquet (k-pop staging) -> HATS at /arc/projects/hats/catwise/2020.
# Builds straight into hats (no k-pop copy), checks exact row conservation and deep QA, then
# repoints inputs/catwise_2020_hats, and deletes the k-pop stage-1 Parquet only after deep QA passes.
# Run only after every catwise_stage1 session has exited; this first reruns stage 1 for leftover tiles.
KP=/arc/projects/k-pop/pensus
CODE=$KP/code
LOG=$KP/logs/catwise_2020_build_$(date -u +%Y%m%dT%H%M%SZ).log
exec > >(tee -a "$LOG") 2>&1
echo "start $(date -Is) host=$(hostname) nproc=$(nproc)"

ROWS=1890715640
TILES=18240
STAGE=$KP/staging/catwise_2020/stage1
BUILD=/arc/projects/hats/catwise/_build
DST=/arc/projects/hats/catwise/2020
ALIAS=catwise_2020_hats
# hats-import intermediates must NOT live on node-local /scratch: CANFAR evicts the
# pod when ephemeral local storage hits 300 Gi (seen on ktnf1y2f mid-Splitting).
TMP=$KP/hats_build/catwise_2020_tmp
VENV=/scratch/hatsenv
WORKERS=${WORKERS:-$(( $(nproc) - 1 ))}

[[ -e "$DST" ]] && { echo "FAIL: $DST exists; refuse overwrite"; exit 1; }

python3 "$CODE/catwise_stage.py" 0 1 "$WORKERS" || { echo "FAIL: stage 1 incomplete"; exit 1; }
python3 - "$STAGE" "$TILES" "$ROWS" <<'PY'
import json, pathlib, sys
stage, tiles, rows = pathlib.Path(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3])
recs = [json.loads(p.read_text()) for p in (stage / "_done").glob("*.json")]
parts = list(stage.glob("*/*.parquet"))
partial = list(stage.glob("*/*.partial"))
got = sum(r["rows"] for r in recs)
print(f"markers {len(recs)} parquet {len(parts)} partial {len(partial)} rows {got}")
if not (len(recs) == len(parts) == tiles and not partial and got == rows):
    sys.exit("FAIL: stage 1 does not match the published CatWISE2020 totals")
PY

STAGED=$(du -sb --exclude=_done "$STAGE" | cut -f1)
HATS_MAX=$(getfattr --only-values -n ceph.quota.max_bytes /arc/projects/hats)
HATS_USED=$(getfattr --only-values -n ceph.dir.rbytes /arc/projects/hats)
KPOP_MAX=$(getfattr --only-values -n ceph.quota.max_bytes /arc/projects/k-pop)
KPOP_USED=$(getfattr --only-values -n ceph.dir.rbytes /arc/projects/k-pop)
echo "staged=$STAGED hats max_bytes=$HATS_MAX rbytes=$HATS_USED k-pop free=$(( KPOP_MAX - KPOP_USED )) tmp=$TMP"
# HATS output measured at 0.96x the stage-1 bytes (zstd-15 + byte_stream_split vs zstd-3); 10% headroom.
NEED=$(( STAGED * 110 / 100 ))
if [[ "$HATS_MAX" != 0 ]] && (( HATS_MAX - HATS_USED < NEED )); then
  echo "FAIL: hats free $(( HATS_MAX - HATS_USED )) < need $NEED"; exit 1
fi
# Intermediates peak ~1.0–1.3× staged; keep 300 GiB k-pop floor after that.
(( KPOP_MAX - KPOP_USED > STAGED * 130 / 100 + 300 * 2**30 )) \
  || { echo "FAIL: k-pop free $(( KPOP_MAX - KPOP_USED )) too small for tmp on CephFS"; exit 1; }

mkdir -p "$BUILD" "$TMP"
python3 -m venv --system-site-packages "$VENV"
"$VENV/bin/python" -m pip install --quiet --upgrade pip
"$VENV/bin/python" -m pip install --quiet 'hats-import==0.11.1' polars
PY="$VENV/bin/python"

"$PY" "$CODE/hats_import_run.py" \
  --inputs "$STAGE/*/*.parquet" --reader parquet \
  --name 2020 --out "$BUILD" --tmp "$TMP" --ra ra --dec dec --sort source_id --workers "$WORKERS"

"$PY" "$CODE/verify_catwise_hats.py" "$BUILD/2020" "$ROWS" "$STAGE/*/*.parquet" \
  || { echo "FAIL: row conservation; build left at $BUILD/2020"; exit 1; }

mv "$BUILD/2020" "$DST"
rmdir "$BUILD"
rm -rf "$TMP"
BYTES=$(du -sb "$DST" | cut -f1)
PARTS=$("$PY" -c "import csv,sys; print(sum(1 for r in csv.DictReader(open(sys.argv[1])) if r.get('Npix')))" "$DST/partition_info.csv")
HI_VER=$("$PY" -c 'import importlib.metadata as m; print(m.version("hats-import"))')
echo "built $DST bytes=$BYTES partitions=$PARTS"

mkdir -p "$KP/inputs/$ALIAS"
"$PY" - "$DST" "$KP/inputs/$ALIAS/SOURCE.json" "$HI_VER" "$BYTES" "$ROWS" "$TILES" <<'PY'
import datetime, json, pathlib, sys
dst, pointer, hi_ver, nbytes, rows, tiles = sys.argv[1:]
now = datetime.datetime.now(datetime.timezone.utc).isoformat()
props = next(q for q in (pathlib.Path(dst, "hats.properties"), pathlib.Path(dst, "properties")) if q.exists())
nrows = next(int(l.split("=", 1)[1]) for l in props.read_text().splitlines() if l.startswith("hats_nrows="))
assert nrows == int(rows), (nrows, rows)
prov = {
    "survey": "catwise",
    "release": "2020",
    "source": "https://portal.nersc.gov/project/cosmo/data/CatWISE/2020/",
    "reference": "Marocco et al. 2021, ApJS 253, 8",
    "input": f"{tiles} *_cat_b0.tbl.gz IPAC tiles, row counts checked against each tile's \\Nsrc header",
    "nrows": nrows,
    "ra_column": "ra",
    "dec_column": "dec",
    "sort_columns": "source_id",
    "hats_import_version": hi_ver,
    "built_at_utc": now,
    "bytes": int(nbytes),
}
pathlib.Path(dst, "PENSUS_BUILD.json").write_text(json.dumps(prov, indent=2, sort_keys=True) + "\n")
pathlib.Path(pointer).write_text(json.dumps({
    "survey": "catwise_2020",
    "alias": pathlib.Path(pointer).parent.name,
    "hats_path": dst,
    "promoted_hats_path": dst,
    "official_mirror": True,
    "hats_layout": "survey/release",
    "exists": True,
    "nrows": nrows,
    "build": prov,
    "properties_head": props.read_text().splitlines()[:16],
    "probed_at_utc": now,
}, indent=2, sort_keys=True) + "\n")
print("wrote", pointer, "nrows", nrows)
PY

"$VENV/bin/python" -m pip install --quiet astropy cdshealpix pyyaml
export PYTHONPATH="$CODE/vendor/xmatcher/src:$CODE"
"$PY" "$CODE/verify_hats_tree.py" "$DST" || { echo "FAIL verify_hats_tree"; exit 1; }
"$PY" "$CODE/qa_hats_release.py" "$DST" "$ALIAS" "$ROWS" "$PARTS" \
  || { echo "FAIL deep QA; k-pop stage-1 Parquet kept at $STAGE"; exit 1; }

# Deletion of the stage-1 Parquet approved 2026-11-13 once deep QA passes; markers and manifest stay.
find "$STAGE" -name '*.parquet' -delete
find "$STAGE" -mindepth 1 -type d -empty -delete
"$PY" - "$KP/inputs/$ALIAS/SOURCE.json" "$STAGE" "$STAGED" <<'PY'
import datetime, json, pathlib, sys
p = pathlib.Path(sys.argv[1]); d = json.loads(p.read_text())
d["kpop_removed"] = [f"{sys.argv[2]}/*/*.parquet"]; d["kpop_removed_bytes"] = int(sys.argv[3])
d["kpop_removed_at_utc"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
p.write_text(json.dumps(d, indent=2, sort_keys=True) + "\n")
PY
echo "DELETED stage-1 Parquet under $STAGE bytes=$STAGED"
echo "done $(date -Is)"
