#!/bin/bash
# Apply public-survey ACL policy under /arc/projects/hats/...
#
# Policy (POSIX ACL on CephFS /arc):
#   - public (other): read-only  -> o::rX
#   - hats writers:   writable   -> g:hats:rwX and g:hats-rw:rwX
#   - owner / owning group keep rwX; setgid on dirs via g+s
#
# Usage: apply_hats_public_acl.sh <path> [<path> ...]
# Only pass public survey trees. Do not run on private/non-public hats children.
#
# Tolerates vanished files during concurrent rsync (ENOENT on setfacl -R).
set -euo pipefail

# Explicit mask so named-group rwX is effective (otherwise mask can clamp to r-x).
ACCESS_ACL='u::rwX,g::rwX,g:hats:rwX,g:hats-rw:rwX,m::rwX,o::rX'
DEFAULT_ACL='u::rwX,g::rwX,g:hats:rwX,g:hats-rw:rwX,m::rwX,o::rX'

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <public-hats-path> [...]" >&2
  exit 2
fi

apply_acl_resilient() {
  local path=$1
  local attempt rc=0
  for attempt in 1 2 3; do
    set +e
    setfacl -R -m "$ACCESS_ACL" "$path"
    rc=$?
    set -e
    if [[ $rc -eq 0 ]]; then
      break
    fi
    echo "WARN: setfacl access attempt $attempt rc=$rc on $path (retry if concurrent writers)"
    sleep 2
  done
  set +e
  setfacl -R -d -m "$DEFAULT_ACL" "$path"
  local drc=$?
  set -e
  if [[ $drc -ne 0 ]]; then
    echo "WARN: setfacl default rc=$drc on $path"
  fi
  set +e
  find "$path" -type d -exec chmod g+s {} +
  set -e
  # Root of tree must succeed; nested ENOENT during rsync is acceptable if root ACL is correct.
  getfacl -p "$path" | head -30
  if ! getfacl -p "$path" | grep -q 'group:hats:rwx'; then
    echo "FAIL: $path missing group:hats:rwx after apply" >&2
    return 1
  fi
  if ! getfacl -p "$path" | grep -q 'other::r-x'; then
    echo "FAIL: $path missing other::r-x after apply" >&2
    return 1
  fi
  return 0
}

apply_one() {
  local path=$1
  if [[ ! -e "$path" ]]; then
    echo "SKIP missing $path"
    return 0
  fi
  case "$path" in
    /arc/projects/hats/*) ;;
    *)
      echo "REFUSE non-hats path: $path" >&2
      return 1
      ;;
  esac

  echo "ACL public-ro + hats-rw -> $path"
  apply_acl_resilient "$path"
}

for p in "$@"; do
  apply_one "$p"
done
echo "apply_hats_public_acl done"
