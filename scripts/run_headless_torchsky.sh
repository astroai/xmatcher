#!/bin/bash
set -euo pipefail
export WORK=/scratch/src
mkdir -p "${WORK}/astroai"
cd "${WORK}/astroai"

echo "==> 1. Cloning xmatcher from sfabbro/xmatcher..."
if [ ! -d xmatcher ]; then
  git clone https://github.com/sfabbro/xmatcher.git
else
  cd xmatcher && git pull && cd ..
fi

echo "==> 2. Cloning torchsky (branch release/0.4.0)..."
if [ ! -d torchsky ]; then
  git clone https://github.com/astroai/torchsky.git --branch release/0.4.0
else
  cd torchsky && git pull && cd ..
fi

echo "==> 3. Installing torchsky & xmatcher editable..."
cd "${WORK}/astroai/torchsky"
pip install -e . --no-deps
cd "${WORK}/astroai/xmatcher"
pip install -e . --no-deps

echo "==> 4. Executing full Torchsky 0.4.0 crossmatch test suite against Zensus HATS..."
python scripts/canfar-torchsky-zensus-test.py
