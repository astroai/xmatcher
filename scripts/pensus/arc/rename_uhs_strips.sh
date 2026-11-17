#!/bin/bash
set -euo pipefail
# Fix UHS strip names where ".csv.gz" was mangled to "pcsvpgz" (dot→p).
D=/arc/projects/mlao/pensus/staging/uhs_dr3/strips
LOG=/arc/projects/k-pop/pensus/logs/uhs_rename_strips_$(date -u +%Y%m%dT%H%M%SZ).log
exec > >(tee -a "$LOG") 2>&1
echo "start $(date -Is)"
python3 - <<'PY'
from pathlib import Path
d = Path("/arc/projects/mlao/pensus/staging/uhs_dr3/strips")
names = sorted(p.name for p in d.iterdir())
print("count", len(names))
print("sample", names[:6])
print("csv.gz", sum(1 for n in names if n.endswith(".csv.gz")))
print("pcsvpgz", sum(1 for n in names if n.endswith("pcsvpgz")))
ren = 0
for p in sorted(d.iterdir(), key=lambda x: x.name):
    n = p.name
    if n.endswith("pcsvpgz.json"):
        dest = p.with_name(n[: -len("pcsvpgz.json")] + ".csv.gz.json")
    elif n.endswith("pcsvpgz"):
        dest = p.with_name(n[: -len("pcsvpgz")] + ".csv.gz")
    else:
        continue
    if dest.exists():
        print("exists skip", dest.name)
        continue
    p.rename(dest)
    ren += 1
print("renamed", ren)
names = sorted(p.name for p in d.iterdir())
print("after csv.gz", sum(1 for n in names if n.endswith(".csv.gz")))
print("after meta", sum(1 for n in names if n.endswith(".csv.gz.json")))
print("sample", names[:6])
PY
echo "done $(date -Is)"
