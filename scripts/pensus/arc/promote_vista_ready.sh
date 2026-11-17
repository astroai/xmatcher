#!/bin/bash
# Promote ready VISTA HATS (VMC/VIDEO/VIKING) from unions staging → hats.
# Keeps unions staging trees (do not delete until explicit post-QA policy).
set -euo pipefail
KP=/arc/projects/k-pop/pensus
CODE=$KP/code
LOG=$KP/logs/promote_vista_ready_$(date -u +%Y%m%dT%H%M%SZ).log
exec > >(tee -a "$LOG") 2>&1
echo "start $(date -Is) VISTA ready promote (keep staging)"

python3 - <<'PY'
import os, subprocess
def xb(path, attr):
  try:
    return int(os.getxattr(path, attr))
  except OSError:
    return int(subprocess.check_output(["getfattr","--only-values","-n",attr,path], text=True).strip())
q=xb(b"/arc/projects/hats", b"ceph.quota.max_bytes")
u=xb(b"/arc/projects/hats", b"ceph.dir.rbytes")
free=q-u
# ~8.4 GiB staged + margin + 300 GiB floor
need=int(1.1 * 9 * (2**30)) + 300*(2**30)
print(f"hats free={free} need>={need} quota={q}")
if free < need:
  raise SystemExit(f"FAIL hats free {free} < need {need}")
print("margin_ok")
PY

promote_keep_staging() {
  local src=$1 dst=$2 alias=$3 rows=$4 parts=$5
  echo "PROMOTE(keep-staging) $src -> $dst"
  if [[ -e "$dst/properties" || -e "$dst/hats.properties" ]]; then
    echo "note: $dst exists; rsync resumes"
  fi
  mkdir -p "$dst"
  bash "$CODE/apply_hats_public_acl.sh" "$(dirname "$dst")" "$dst" || echo "WARN pre-ACL"
  rsync -a --exclude sync.json "$src/" "$dst/"
  bash "$CODE/apply_hats_public_acl.sh" "$dst"

  VENV=/scratch/qaenv-vista
  python3 -m venv --system-site-packages "$VENV"
  "$VENV/bin/python" -m pip install --quiet hats astropy pyarrow polars cdshealpix pyyaml
  local PY=$VENV/bin/python
  export PYTHONPATH="$CODE/vendor/xmatcher/src:$CODE"
  "$PY" "$CODE/verify_hats_tree.py" "$dst"
  "$PY" - "$KP/inputs/$alias/SOURCE.json" "$dst" <<'PY'
import datetime, json, pathlib, sys
p, dst = pathlib.Path(sys.argv[1]), sys.argv[2]
d = json.loads(p.read_text()) if p.is_file() else {"alias": p.parent.name}
for k in ("cache_alias_hint", "cache_root", "cmd", "stdout_log", "stderr_log"):
    d.pop(k, None)
d.update(
  promoted_hats_path=dst,
  hats_path=dst,
  official_mirror=True,
  hats_layout="survey/release",
  staging_kept=True,
  staging_keep_reason="leave unions staging until post-promote QA policy",
  promoted_at_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),
)
p.parent.mkdir(parents=True, exist_ok=True)
p.write_text(json.dumps(d, indent=2, sort_keys=True) + "\n")
print("updated", p)
PY
  # Hide staging only for pointer QA, then restore (do not delete).
  local hidden=$KP/staging/_promoting_vista/$alias/$(basename "$src")
  mkdir -p "$(dirname "$hidden")"
  mv "$src" "$hidden"
  if ! "$PY" "$CODE/qa_hats_release.py" "$dst" "$alias" "$rows" "$parts"; then
    mv "$hidden" "$src"
    echo "FAIL deep QA; staging restored at $src"
    exit 1
  fi
  mv "$hidden" "$src"
  echo "KEEP staging $src (policy)"
  bash "$CODE/apply_hats_public_acl.sh" "$dst"
  echo "done $alias"
}

promote_keep_staging /arc/projects/unions/pensus/hats_staging/vmc/dr7 \
  /arc/projects/hats/vmc/dr7 vmc_dr7_hats 64359516 149
promote_keep_staging /arc/projects/unions/pensus/hats_staging/video/dr6 \
  /arc/projects/hats/video/dr6 video_dr6_hats 3384907 10
promote_keep_staging /arc/projects/unions/pensus/hats_staging/viking/dr5 \
  /arc/projects/hats/viking/dr5 viking_dr5_hats 82063585 159

echo "VISTA batch done $(date -Is) LOG=$LOG"
