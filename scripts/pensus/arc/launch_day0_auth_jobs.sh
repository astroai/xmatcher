#!/bin/bash
# Shared: load bashrc export lines past interactive guard (no secret echo).
set -euo pipefail
eval "$(grep -E '^[[:space:]]*export[[:space:]]+(SKYSERVER|HSC|COSMOHUB|CASJOBS)_(USERID|PW)=' "$HOME/.bashrc" || true)"
