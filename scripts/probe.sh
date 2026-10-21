#!/bin/bash
set -euo pipefail
echo "=== 1. Listing /arc/projects/hats/zensus ==="
ls -la /arc/projects/hats/zensus || true
echo "=== 2. Finding structure of /arc/projects/hats/zensus ==="
find /arc/projects/hats/zensus -maxdepth 3 || true
echo "=== 3. Checking properties or partition_info if present ==="
cat /arc/projects/hats/zensus/properties 2>/dev/null || true
cat /arc/projects/hats/zensus/*/properties 2>/dev/null || true
