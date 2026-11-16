#!/bin/bash
# Apply public-survey ACL policy under /arc/projects/hats/...
#
# Policy (POSIX ACL on CephFS /arc):
#   - public (other): read-only  -> o::rX
#   - hats writers:   writable   -> g:hats:rwX and g:hats-rw:rwX
#   - owner / owning group keep rwX; setgid on dirs preserved via g+s
#
# Usage: apply_hats_public_acl.sh <path> [<path> ...]
# Only pass public survey trees. Do not run on private/non-public hats children.
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <public-hats-path> [...]" >&2
  exit 2
fi

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
  # Access ACL on the tree (capital X: execute only where meaningful).
  setfacl -R -m u::rwX,g::rwX,g:hats:rwX,g:hats-rw:rwX,o::rX "$path"
  # Default ACL so new files/dirs inherit the same policy.
  setfacl -R -d -m u::rwX,g::rwX,g:hats:rwX,g:hats-rw:rwX,o::rX "$path"
  # Keep setgid on directories for group inheritance of owning gid.
  find "$path" -type d -exec chmod g+s {} +

  echo "--- getfacl -p $path (head) ---"
  getfacl -p "$path" | head -30
}

for p in "$@"; do
  apply_one "$p"
done
echo "apply_hats_public_acl done"
