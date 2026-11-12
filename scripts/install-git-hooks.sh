#!/usr/bin/env bash
# Point this clone at the checked-in hooks under scripts/git-hooks/.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
HOOKS_DIR="$ROOT/scripts/git-hooks"

if [[ ! -x "$HOOKS_DIR/pre-push" ]]; then
  chmod +x "$HOOKS_DIR/pre-push"
fi

git -C "$ROOT" config core.hooksPath scripts/git-hooks
echo "Configured core.hooksPath=scripts/git-hooks"
echo "pre-push will run: pixi run preflight-push"
