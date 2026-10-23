# xmatcher

[![PyPI version](https://img.shields.io/pypi/v/xmatcher.svg?color=blue)](https://pypi.org/project/xmatcher/)
[![Python](https://img.shields.io/badge/python-3.13+-blue.svg)](https://www.python.org/downloads/)
[![CI](https://github.com/astroai/xmatcher/actions/workflows/ci.yml/badge.svg)](https://github.com/astroai/xmatcher/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Pixi](https://img.shields.io/badge/pixi-package-purple)](https://pixi.sh)

High-performance, format-agnostic astronomical catalogue crossmatching and multi-survey union toolkit. Built on Polars, PyArrow, and 3D unit-sphere KD-trees with native HATS and distributed Ray support.

---

## Highlights

- ⚡ **Fast & Memory-Bounded**: Vectorized 3D unit-sphere KD-tree indexing in C++ via SciPy `cKDTree` and Polars, with bounded disk-spilling for memory-constrained systems.
- 🔭 **Format & Survey Agnostic**: Match local Parquet, CSV, TSV, and FITS files directly against remote TAP/VizieR archives (`gaia`, `ps1`, `des`, `allwise`), or stream HATS trees from local storage, AWS S3 (`s3://`), and Hugging Face (`hf://`).
- 📐 **Rigorous Astrometric & Probabilistic Scoring**: Fixed-radius (`sky`), per-source adaptive errors (`skyerr`), 2D Gaussian error ellipses via Mahalanobis distance (`skyellipse`), Sutherland & Saunders Likelihood Ratios (`lr`), and Budavári & Szalay Bayes factors (`nway_match`).
- 🌐 **Laptop to Distributed Cluster**: Fast multi-threaded queries on a single workstation, scaling out to distributed full-sky unions and master catalogues on Ray clusters (`ray-union` and `ray-anchor`).

---

## Installation

```bash
# Core package (cKDTree, Polars, Astropy, PyVO)
pip install xmatcher

# With all features (Ray distributed, HATS, CDS, ML, Torchfits)
pip install "xmatcher[all]"
```

For developer setup with Pixi, container configurations, and granular extras, see the [Installation Guide](docs/usage.md#1-installation--environment-setup).

---

## 30-Second Quickstart

### Command-Line Interface (CLI)

Match two local catalogues using a 1.5 arcsecond radius:

```bash
xmatcher match optical.parquet infrared.csv -r 1.5 -o matched.parquet
```

Match a local target list against remote Gaia DR3 (cone fetched automatically):

```bash
xmatcher match targets.csv gaia -r 1.0 -o targets_gaia.parquet
```

> [!TIP]
> Explore remote archives, HATS streaming, and batch workflows in the [CLI Reference](docs/cli.md).

### Python API

```python
from xmatcher import CrossMatch

cm = CrossMatch()

# Match two catalogues into a Polars DataFrame
df = cm.crossmatch("optical.parquet", "infrared.csv", radius_arcsec=1.5)
print(f"Matched {len(df)} pairs.")
```

> [!TIP]
> Typed request objects (`MatchRequest`), multi-catalogue intersections, and probabilistic qualification are detailed in the [Python API Reference](docs/api.md).

---

## Documentation

Comprehensive documentation, architecture guides, and mathematical formulations are available in [`docs/`](docs/):

| Guide | Description |
|---|---|
| 📖 **[User Guide & Cookbooks](docs/usage.md)** | Practical end-to-end recipes, remote TAP querying, and memory budget controls. |
| 💻 **[CLI Reference](docs/cli.md)** | Complete command syntax, subcommands, flags, and recipes. |
| 🐍 **[Python API Reference](docs/api.md)** | Method signatures, dataclasses, join modes, and exception types. |
| 📐 **[Algorithms & Mathematics](docs/algorithms.md)** | 3D unit-sphere geometry, Mahalanobis ellipses, Likelihood Ratios, and Bayesian models. |
| 🏷️ **[Catalogue Naming](docs/naming.md)** | Registry naming conventions, canonical aliases, and remote identifiers. |
| 🌐 **[Anchor Master Catalogues](docs/ray-anchor.md)** | Distributed one-row-per-anchor catalogues on Ray clusters with HATS partitioning. |
| 🧭 **[Documentation Portal](docs/README.md)** | System architecture diagrams, data contracts, and release archive. |

---

## License

MIT License. See [LICENSE](LICENSE) for details.
