#!/bin/bash
set -euo pipefail
export TEMP_HOST=/arc/projects/skyai
export WORKERS=${WORKERS:-}
exec bash /arc/projects/k-pop/pensus/code/build_ls_dr11_hats.sh
