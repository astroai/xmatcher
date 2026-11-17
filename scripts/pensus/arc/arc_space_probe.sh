#!/bin/bash
# Measure /arc/projects/{hats,k-pop} on the mounted ARC filesystem (not the VOS listing).
LOG=/arc/projects/k-pop/pensus/logs/arc_space_probe_$(date -u +%Y%m%dT%H%M%SZ).log
exec > >(tee -a "$LOG") 2>&1
echo "probe $(date -Is) host=$(hostname)"
mount | grep -E ' /arc( |/)' | sed 's/secret=[^,]*/secret=REDACTED/'
for p in /arc/projects/hats /arc/projects/k-pop /arc; do
  echo "== $p"
  df -B1 "$p" | tail -1
  df -h "$p" | tail -1
  stat -f -c 'statfs type=%T bsize=%S blocks=%b bfree=%f bavail=%a' "$p"
  for a in ceph.quota.max_bytes ceph.dir.rbytes ceph.dir.rfiles ceph.quota.max_files; do
    printf '%s=' "$a"; getfattr --only-values -n "$a" "$p" 2>&1 || python3 -c "import os,sys;print(os.getxattr(sys.argv[1],sys.argv[2]).decode())" "$p" "$a" 2>&1
    echo
  done
done
command -v quota && quota -s 2>&1 | head
echo "== top-level hats usage (ceph.dir.rbytes)"
for d in /arc/projects/hats/*/; do printf '%s %s\n' "$(getfattr --only-values -n ceph.dir.rbytes "$d" 2>/dev/null || python3 -c "import os,sys;print(os.getxattr(sys.argv[1],'ceph.dir.rbytes').decode())" "$d" 2>/dev/null)" "$d"; done
echo done
