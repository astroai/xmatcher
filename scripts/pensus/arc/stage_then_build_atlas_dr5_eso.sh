#!/bin/bash
# ATLAS DR5/ER5: ESO FITS stage → parquet → HATS on skyai (temp). No OSA. No promote yet.
set -euo pipefail
KP=/arc/projects/k-pop/pensus
CODE=$KP/code
LOG=$KP/logs/atlas_dr5_eso_stage_build_$(date -u +%Y%m%dT%H%M%SZ).log
exec > >(tee -a "$LOG") 2>&1
echo "==== START atlas ESO stage+build $(date -Is) host=$(hostname) ===="
echo "OSA strip staging remains paused; planned pin /arc/projects/hats/atlas/dr5"
bash "$CODE/stage_atlas_dr5_eso.sh"
echo "==== stage finished $(date -Is); starting hats build ===="
bash "$CODE/build_atlas_dr5_hats.sh"
echo "==== ALL DONE atlas ESO stage+build $(date -Is) LOG=$LOG ===="
