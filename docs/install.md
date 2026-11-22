# Installation

`xmatcher` needs Python 3.13 or later. Its core dependencies are installed
automatically:

| Package | Used for |
|---|---|
| [torchsky](https://github.com/astroai/torchsky) (>=0.6.0, with PyTorch) | Candidate search (exact declination-zone index), association statistics, epoch propagation, HEALPix |
| [torchfits](https://github.com/astroai/torchfits) (>=1.3.3) | FITS table reading and streaming FITS output |
| Polars, PyArrow | In-memory and streamed tables, Parquet and HATS I/O |
| Astropy, PyVO | Coordinate frames and units, TAP queries |
| NumPy, SciPy, PyYAML | Numerics, `cKDTree` searches (Friends-of-Friends, `ray-union`), catalogue registry |

An installed torchsky older than 0.6.0 is refused with an upgrade message.

## pip

```bash
pip install xmatcher
```

With every optional backend:

```bash
pip install "xmatcher[all]"
```

From a source checkout:

```bash
git clone https://github.com/astroai/xmatcher.git
cd xmatcher
pip install -e ".[all]"
```

## Optional extras

Install only the backends you use:

| Extra | Packages | Enables |
|---|---|---|
| `cds` | `astroquery` | CDS VizieR queries and the CDS XMatch service |
| `hats` | `hats`, `cdshealpix` | Writing a `.hats` match output and mirroring a TAP table to HATS (both need `cdshealpix`). Reading HATS trees and `xmatcher convert` need no extra |
| `ray` | `ray`, `cdshealpix` | The Ray engines: pixel-batch matching, `ray-union`, `ray-anchor` |
| `hats-ray` | `hats`, `ray`, `cdshealpix` | `hats` and `ray` together |
| `ml` | `scikit-learn`, `xgboost`, `lightgbm`, `joblib` | The `ml` and `xgb` matchers |
| `all` | All of the above | Everything |

Local matching (`fast`, `zone`), file conversion and remote TAP queries need no
extra.

## Developer environment (Pixi)

The repository ships a locked [Pixi](https://pixi.sh) environment with Python
3.13, conda-forge CPU PyTorch and every optional backend:

```bash
git clone https://github.com/astroai/xmatcher.git
cd xmatcher
pixi install        # resolves from the checked-in pixi.lock
pixi run ci-local   # ruff, format check, mypy, then the offline test suite
```

[CONTRIBUTING.md](../CONTRIBUTING.md) covers the git hooks, test selection and
release checks.

## Platform notes

- torchsky publishes wheels for Linux x86_64 and macOS 14 or later on Apple
  silicon. Elsewhere pip builds it from source, which needs a C++ compiler.
- In a conda or Pixi environment, take PyTorch from conda-forge (the Pixi
  environment does). The PyPI PyTorch wheel bundles its own OpenMP runtime,
  which aborts the process on macOS when another OpenMP runtime is loaded.
- On shared or NFS-mounted systems (CANFAR, for example), set
  `PYTHONNOUSERSITE=1` and unset `PYTHONPATH` so packages in your user
  site-packages do not shadow the environment's.
- `xmatcher doctor` compares your `~/.config/xmatcher/xmatcher.yaml` against
  the bundled catalogue registry and reports drift.
