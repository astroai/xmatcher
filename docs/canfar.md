# Run xmatcher on CANFAR

Use a session image with Python 3.13 or a checkout with Pixi. In a checkout,
source the project settings before launching a headless workload.

```bash
source scripts/canfar-env.sh
unset PYTHONPATH
pixi install
pixi run xmatcher describe gaia_lsdb_hats
```

For a pip installation, set the same variables in the headless session.

```bash
export PYTHONNOUSERSITE=1
unset PYTHONPATH
export XMATCHER_COMMUNITY_ROOTS=/arc/projects/hats
export XMATCHER_CACHE_ROOT=/arc/projects/astroai/hats
export XMATCHER_OUTPUT_ROOT=/arc/projects/astroai/xmatcher
python -m pip install 'xmatcher[hats-ray]'
xmatcher describe gaia_lsdb_hats
```

- `/arc/projects/hats` holds released HATS products. xmatcher uses verified
  copies there in place. See the [mounted survey list](hats-sources.md#community-trees-on-canfar).
- `XMATCHER_CACHE_ROOT` stores reusable mirrors and registry state. Choose a
  writable project directory with space for the catalogues you need.
- `XMATCHER_OUTPUT_ROOT` applies to bare output filenames such as
  `-o targets_gaia.parquet`. An explicit directory in `-o` selects that
  destination. Choose project storage for outputs and resumable runs.
- `/scratch` is available for per-job temporary files. For a multi-node Ray
  run, use paths visible to all workers for the inputs, cache and output.

Set the variables before sourcing the script to use another project's paths.
For a small match against Gaia over TAP, start with a target file in your
working directory.

```bash
xmatcher match targets.csv gaia --match-radius-arcsec 1 \
  --keep-rows all-from-1 -o targets_gaia.parquet
```

For a full-sky workload, see the
[distributed union guide](usage.md#distributed-full-sky-union-ray-union).
For a multi-node CANFAR job, see the options in
[`scripts/canfar-ray-job.sh`](../scripts/canfar-ray-job.sh).
