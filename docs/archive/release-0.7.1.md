# xmatcher 0.7.1

This release adds clearer command-line options, reusable shared HATS inputs,
and fixes for distributed matching and HATS output.

## Command line and storage

- Use `--match-radius-arcsec` for angular separation, `--match-method` for
  counterpart scoring, `--matches` for counterpart selection, and
  `--keep-rows` for the rows to retain. Existing option spellings continue
  to work.
- Mirrors default to `~/.cache/xmatcher`. Bare output filenames resolve
  in the working directory by default. Configure a shared cache, read-only
  HATS roots, and a default output directory when you need them.
- CANFAR users can source `scripts/canfar-env.sh` in a checkout to enable
  the AstroAI paths. Verified survey copies under `/arc/projects/hats` are
  used in place while derivatives and cache files go to writable project
  storage. See the [CANFAR setup](https://github.com/astroai/xmatcher/blob/v0.7.1/docs/canfar.md).

## Matching and catalogue reliability

- Ray anchor runs plan reads per anchor partition, record task I/O and
  memory, and can resume partition pilots. Invalid survey positions are
  skipped and counted in the output metadata.
- HATS writers partition each row by its sky position. Outer joins retain
  the position of sources found only in later catalogues, and sequential
  HATS matching uses the accumulated match result.
- Bundled HATS metadata includes verified local copies and corrections for
  2MASS coordinates, SkyMapper error units, and AllWISE motion handling.
- Observation mappings support nJy, calibrated fluxes, magnitude systems,
  uncertainties and flags. (Source-preserving photometry union product code
  moved to `sfabbro/pensus`.)

The `zone` engine remains available in the core package. Use the
`hats` or `ray` extra to install the HEALPix dependency formerly provided
by the `zone` extra.

See the [changelog](https://github.com/astroai/xmatcher/blob/v0.7.1/CHANGELOG.md)
for the full list of changes since 0.7.0.
