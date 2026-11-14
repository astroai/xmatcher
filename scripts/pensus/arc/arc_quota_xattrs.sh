#!/bin/bash
# Exact CephFS quota xattrs on /arc/projects/{hats,k-pop} and their ancestors.
LOG=/arc/projects/k-pop/pensus/logs/arc_quota_xattrs_$(date -u +%Y%m%dT%H%M%SZ).log
exec > >(tee -a "$LOG") 2>&1
echo "probe $(date -Is) host=$(hostname) kernel=$(uname -r)"
mount | grep -E ' /arc( |/)' | sed 's/secret=[^,]*/secret=REDACTED/'
ls -ld /arc/projects/HATS 2>&1
for p in /arc/projects/hats /arc/projects/k-pop /arc/projects /arc; do
  echo "== $p"
  for a in ceph.quota.max_bytes ceph.quota.max_files ceph.quota ceph.dir.rbytes ceph.dir.rfiles ceph.dir.layout.pool; do
    printf '  getfattr %s -> ' "$a"; getfattr --absolute-names -n "$a" "$p" 2>&1 | grep -v '^# file' | tr '\n' ' '; echo
  done
  python3 - "$p" <<'EOF'
import os, sys
p = sys.argv[1]
for a in ("ceph.quota.max_bytes", "ceph.quota.max_files", "ceph.dir.rbytes"):
    try:
        print(f"  os.getxattr {a} = {os.getxattr(p, a).decode()!r}")
    except OSError as e:
        print(f"  os.getxattr {a} -> OSError errno={e.errno} {e.strerror}")
print("  listxattr:", os.listxattr(p))
st = os.statvfs(p)
print(f"  statvfs size={st.f_blocks*st.f_frsize} avail={st.f_bavail*st.f_frsize}")
EOF
done
echo done
