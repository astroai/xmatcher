# xmatcher

[![PyPI version](https://img.shields.io/pypi/v/xmatcher.svg?color=blue)](https://pypi.org/project/xmatcher/)
[![Python](https://img.shields.io/badge/python-3.13+-blue.svg)](https://www.python.org/downloads/)
[![CI](https://github.com/astroai/xmatcher/actions/workflows/ci.yml/badge.svg)](https://github.com/astroai/xmatcher/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Pixi](https://img.shields.io/badge/pixi-package-purple)](https://pixi.sh)

High-performance, format-agnostic astronomical catalogue crossmatching and multi-survey union toolkit. Built on Polars, PyArrow, and 3D unit-sphere KD-trees with native HATS and distributed Ray support.

---

## Key Features

- **Format & Source Agnostic**: Direct support for Parquet, CSV, TSV, and FITS. Query and mirror remote TAP/VizieR services (`gaia`, `ps1`, `des`, `allwise`) or stream partitioned HATS trees from local disk, HTTP, S3 (`s3://`), and Hugging Face (`hf://`).
- **Astrometric & Probabilistic Matchers**: Fixed-radius (`sky`), adaptive coordinate errors (`skyerr`), Gaussian error ellipses via Mahalanobis distance (`skyellipse`), proper-motion epoch propagation (`target_epoch`), Sutherland & Saunders Likelihood Ratio (`lr`), ML/GBDT classifiers, and Friends-of-Friends object clustering (`fof`).
- **Flexible Relational Joins**: Inner (`1and2`), Left (`all1`), Right (`all2`), Full Outer (`1or2`, `all`), Anti-joins, or pure identifier joins (`--id-join`).
- **Laptop to Distributed Cluster**: Fast multi-threaded local matching via SciPy cKDTree, bounded-memory disk spill for memory-constrained machines, and distributed full-sky HATS joins on Ray clusters (`engine="ray-union"` and `engine="ray-anchor"`).

---

## Installation

Requires Python $\ge 3.13$.

```bash
# Standard installation (fast cKDTree, Polars, Astropy, PyVO)
pip install xmatcher

# With all optional features (Ray distributed, HATS, CDS, ML, Torchfits)
pip install "xmatcher[all]"
```

For development and contributors:

```bash
git clone https://github.com/astroai/xmatcher.git
cd xmatcher
pixi install
```

For individual optional dependencies, see the [Installation Guide](docs/usage.md#1-installation--environment-setup).

---

## Quickstart: CLI

Match two local files using a 1.5 arcsecond radius:

```bash
xmatcher match cat1.parquet cat2.csv -r 1.5 -o matched.parquet
```

Match a local target list against remote Gaia DR3 (fetches cone automatically):

```bash
xmatcher match targets.csv gaia -r 1.0 -o targets_gaia.parquet
```

Full-outer union across multiple surveys into a partitioned HATS tree using Ray:

```bash
xmatcher match gaia allwise twomass --union --engine ray-union -o /data/master.hats
```

See the [CLI Reference](docs/cli.md) for all subcommands (`match`, `list`, `describe`, `discover`, `search`, `adopt`, `sync`, `project-hats`, `completion`, `doctor`).

---

## Quickstart: Python API

```python
from xmatcher import CrossMatch, MatchRequest, MatchSpec

cm = CrossMatch()

# Strongly-typed request interface
req = MatchRequest(
    cat1="optical.parquet",
    cat2="infrared.csv",
    spec=MatchSpec(radius_arcsec=1.5, matcher="sky", find="best"),
    engine="fast",  # SciPy cKDTree on 3D unit sphere
)
matched_df = cm.crossmatch_request(req)

# Multi-catalogue sequential intersection
result_3way = cm.crossmatch_multi(
    ["optical.parquet", "infrared.csv", "radio.fits"],
    radius_arcsec=2.0,
    join_type="1and2",
)

# Friends-of-Friends object bundles
bundles = cm.fof_match(
    ["survey_a.parquet", "survey_b.parquet", "survey_c.parquet"],
    radius_arcsec=1.5,
)
```

See the [Python API Reference](docs/api.md) for full method signatures, parameter types, and engine details.

---

## Documentation

Comprehensive documentation is available in the [`docs/`](docs/) directory:

- **[CLI Reference](docs/cli.md)** — Complete command syntax, subcommands, and flags.
- **[Python API Reference](docs/api.md)** — Public API specifications, dataclasses, and exceptions.
- **[User Guide & Cookbooks](docs/usage.md)** — Practical end-to-end workflows and memory controls.
- **[Algorithms & Mathematics](docs/algorithms.md)** — Astrometric calculations, likelihood ratios, and probabilistic models.
- **[Catalogue Naming](docs/naming.md)** — Survey registry naming conventions and identifier syntax.
- **[Documentation Index](docs/README.md)** — Full suite including HATS sources, master data pipelines, and specifications.

---

## License

MIT License. See [LICENSE](LICENSE) for details.
