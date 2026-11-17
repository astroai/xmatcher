#!/bin/bash
set +e
LOG=/arc/projects/k-pop/pensus/logs/keepalive_$(date -u +%Y%m%dT%H%M%SZ).log
exec > >(tee -a "$LOG") 2>&1
echo "start $(date -Is)"
if [[ -f "$HOME/.config/splus/token" ]]; then echo TOKEN_EXISTS=yes; else echo TOKEN_EXISTS=no; fi
for h in skyai mlao unions ots hats k-pop; do
  root=/arc/projects/$h
  mx=$(getfattr --only-values -n ceph.quota.max_bytes "$root" 2>/dev/null)
  us=$(getfattr --only-values -n ceph.dir.rbytes "$root" 2>/dev/null)
  echo "FREE_$h=$(( (mx-us)/1024/1024/1024 )) GiB"
done
python3 - <<'PY'
import json, os
for cand in [
  "/arc/projects/hats/gaia/dr3_xp_continuous",
  "/arc/projects/hats/gaia/xp_continuous_mean_spectrum",
  "/arc/projects/hats/gaia/dr3_xp",
]:
  if os.path.isdir(cand):
    try:
      b = int(os.getxattr(cand, b"ceph.dir.rbytes"))
    except OSError:
      import subprocess
      b = int(subprocess.check_output(["du", "-sb", cand], text=True).split()[0])
    print(f"xp_dest={cand} gib={b/2**30:.1f}")
for a in ["jplus_dr3_hats", "vhs_dr7_hats", "vvvx_dr1_hats", "jpas_e2dr_hats"]:
  p = f"/arc/projects/k-pop/pensus/inputs/{a}/SOURCE.json"
  try:
    d = json.load(open(p))
    keys = [
      "alias", "tiles", "nrows", "nrows_staged", "hats_ready", "hats_path",
      "planned_hats_path", "strips", "bytes", "official_mirror",
    ]
    print(a, {k: d.get(k) for k in keys if d.get(k) is not None})
  except Exception as e:
    print(a, "ERR", e)
print("kids_tiles", len(list(__import__("pathlib").Path("/arc/projects/skyai/pensus/staging/kids_dr5/tiles").glob("*"))))
print("atlas_strips", len(list(__import__("pathlib").Path("/arc/projects/ots/pensus/staging/atlas_dr5/strips").glob("*"))))
jpas = "/arc/projects/mlao/pensus/hats_staging/jpas/e2dr"
dest = "/arc/projects/hats/jpas/e2dr"
print(
  "jpas_temp",
  os.path.exists(jpas + "/hats.properties") or os.path.exists(jpas + "/properties"),
  "hats_dest_exists",
  os.path.exists(dest),
)
PY
echo "done $(date -Is) LOG=$LOG"
