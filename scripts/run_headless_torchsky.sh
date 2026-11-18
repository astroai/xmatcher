#!/bin/bash
# CANFAR headless run of scripts/canfar-torchsky-zensus-test.py against public
# torchsky (>=0.5.0 from PyPI, CPU PyTorch). Packages go to /scratch, never to
# $HOME/.local (shared /arc home).
set -euo pipefail

export PATH=/opt/canfar/bin:/opt/conda/bin:/opt/astroai/bin:$PATH
export WORK=/scratch/src
export PYTHONNOUSERSITE=1
unset PYTHONPATH
mkdir -p "${WORK}/astroai"
cd "${WORK}/astroai"

echo "==> 1. Unpacking xmatcher from VOSpace..."
vcp vos:sfabbro/xmatcher.tar.gz /scratch/xmatcher.tar.gz
mkdir -p "${WORK}/astroai/xmatcher"
tar -xzf /scratch/xmatcher.tar.gz -C "${WORK}/astroai/xmatcher"

echo "==> 2. Installing xmatcher[torchsky,cds] into isolated /scratch/packages..."
# CPU PyTorch: PyPI's Linux wheels pull the multi-GB CUDA toolkit. The local
# "+cpu" build sorts above the PyPI build of the same release.
mkdir -p /scratch/packages
pip install --target /scratch/packages \
    --extra-index-url https://download.pytorch.org/whl/cpu \
    "${WORK}/astroai/xmatcher[torchsky,cds]"
export PYTHONPATH=/scratch/packages
python3 -c "import torch, torchsky, torchfits, xmatcher; print('torchsky', torchsky.__version__, 'torch', torch.__version__, 'torchfits', torchfits.__version__)"

echo "==> 3. Running the torchsky crossmatch suite..."
cd "${WORK}/astroai/xmatcher"
mkdir -p /scratch/output
python3 scripts/canfar-torchsky-zensus-test.py --out-dir /scratch/output

echo "==> 4. Uploading results to VOSpace..."
vcp /scratch/output/torchsky_test_summary.json vos:sfabbro/torchsky_test_summary.json
for f in /scratch/output/*.parquet; do
    if [ -f "$f" ]; then
        vcp "$f" "vos:sfabbro/$(basename "$f")"
    fi
done

echo "==> 5. Copying results to /arc/projects/hats/tests/output/ if writable..."
if mkdir -p /arc/projects/hats/tests/output 2>/dev/null && [ -w /arc/projects/hats/tests/output ]; then
    cp -r /scratch/output/* /arc/projects/hats/tests/output/
fi

echo "==> ALL JOBS AND TESTS COMPLETED SUCCESSFULLY!"
