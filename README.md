# xmatcher

[![PyPI version](https://img.shields.io/pypi/v/xmatcher.svg?color=blue)](https://pypi.org/project/xmatcher/)
[![Python](https://img.shields.io/badge/python-3.13+-blue.svg)](https://www.python.org/downloads/)
[![CI](https://github.com/astroai/xmatcher/actions/workflows/ci.yml/badge.svg)](https://github.com/astroai/xmatcher/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

`xmatcher` cross-matches astronomical catalogues: local files, remote archives
and HATS trees, from two catalogues on a laptop to full-sky multi-survey unions
on a Ray cluster. Candidate search uses
[torchsky](https://github.com/astroai/torchsky)'s exact declination-zone index;
tables are Polars and Arrow throughout.

- **Inputs.** Parquet, CSV/TSV, FITS and HATS trees on disk; named catalogues
  served by TAP and VizieR (`gaia`, `ps1`, `allwise`, ...); HATS trees over
  HTTPS, S3 and Hugging Face, mirrored into a local cache.
- **Matchers.** Fixed radius (`sky`), per-source errors (`skyerr`), error
  ellipses (`skyellipse`), Sutherland & Saunders likelihood ratios with
  magnitude or colour terms (`lr`), N-way Bayesian association, machine-learned
  scores (`ml`, `xgb`), and Friends-of-Friends grouping.
- **Epochs.** Positions can be propagated to a common epoch with proper
  motions before matching.
- **Outputs.** Inner, outer, left, right and anti joins, written to Parquet,
  CSV, FITS or a HATS tree.
- **Scale.** In-memory matching on one machine, disk spilling when memory is
  short, and Ray engines for full-sky unions (`ray-union`) and Gaia-anchored
  master catalogues (`ray-anchor`).
- **Conversion.** `xmatcher convert` streams catalogues between FITS,
  Parquet, HATS and CSV in any direction.

## Installation

```bash
pip install xmatcher            # core: local matching, conversion, TAP
pip install "xmatcher[all]"     # plus HATS, Ray, VizieR/CDS and ML backends
```

Python 3.13 or later. torchsky ships wheels for Linux x86_64 and macOS 14+ on
Apple silicon. See the [installation guide](docs/install.md) for the optional
extras, the Pixi developer environment and platform notes.

## Quickstart

### Command line

Match two local catalogues within 1.5″, keeping the best counterpart:

```bash
xmatcher match optical.parquet infrared.csv -r 1.5 -o matched.parquet
```

Each output row holds both sources' columns (clashing names from the second
catalogue get a `_2` suffix) and their separation in `sep_arcsec`.

Match a target list against Gaia DR3; the cone around your targets is fetched
over TAP:

```bash
xmatcher match targets.csv gaia -r 1.0 -o targets_gaia.parquet
```

List the named catalogues, and convert a result to FITS:

```bash
xmatcher list
xmatcher convert matched.parquet matched.fits
```

### Python

```python
from xmatcher import CrossMatch

cm = CrossMatch()
matches = cm.crossmatch("optical.parquet", "infrared.csv", radius_arcsec=1.5)
print(matches.select("id", "wid", "sep_arcsec"))
```

`crossmatch` returns a Polars DataFrame. The same call takes `matcher=`,
`engine=`, `join_type=`, `target_epoch=` and the other options of the command
line; see the [API reference](docs/api.md).

## Engines

| Engine | Where it runs | Use it for |
|---|---|---|
| `auto` (default) | One machine | `stilts` when STILTS is installed and supports the request, otherwise `fast` |
| `fast` | In memory, one machine | Most pairwise matches |
| `zone` | In memory, one machine | Same search as `fast` |
| `torchsky` | In memory, one machine | torchsky's `crossmatch_sky` directly (`sky` and `skyerr` only) |
| `astropy`, `stilts` | One machine | Astropy's or STILTS's own matching, for comparison |
| `ray` | Ray cluster | Pairwise matches too large for one machine |
| `ray-union` | Ray cluster, HATS inputs | Full outer join of many surveys |
| `ray-anchor` | Ray cluster, HATS inputs | One row per anchor source (normally Gaia) with each survey's best counterpart |

The [user guide](docs/usage.md) explains how to choose between them.

## Documentation

| Guide | Contents |
|---|---|
| [Installation](docs/install.md) | pip, extras, Pixi environment, platforms |
| [User guide](docs/usage.md) | Formats, remote archives, mirroring, matcher cookbook, joins, execution paths |
| [CLI reference](docs/cli.md) | Every subcommand and option |
| [Python API](docs/api.md) | `CrossMatch`, request objects, I/O helpers, exceptions |
| [Algorithms](docs/algorithms.md) | Geometry, error models, likelihood ratios, Bayesian and N-way association, Friends-of-Friends |
| [Catalogue naming](docs/naming.md) | Catalogue names, aliases and explicit identifiers |
| [HATS sources](docs/hats-sources.md) | Registry lookups, verified remote trees, mirroring |
| [Gaia-anchored masters](docs/ray-anchor.md) | The `ray-anchor` engine and its output schema |
| [PENSUS on CANFAR](scripts/pensus/README.md) | Source-preserving photometry unions from mounted HATS catalogues |
| [Documentation index](docs/README.md) | All guides, data contracts and release notes |

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for the development environment, tests
and pull-request conventions. Changes are listed in the
[changelog](CHANGELOG.md).

## License

MIT. See [LICENSE](LICENSE).
