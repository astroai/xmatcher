#!/bin/bash
set -euo pipefail
# Resume LS DR11 hats-import on skyai with high per-worker RAM.
# Prior 16c/64GB thrash: dask auto-split ~4 GiB/worker at Splitting ~85%.
export TEMP_HOST=/arc/projects/skyai
export WORKERS=${WORKERS:-8}
export MEMORY_LIMIT=${MEMORY_LIMIT:-14GB}
exec bash /arc/projects/k-pop/pensus/code/build_ls_dr11_hats.sh
