# xmatcher command-line reference

The `xmatcher` CLI cross-matches astronomical catalogues held in local files,
remote TAP/VizieR services and partitioned HATS trees, and manages the local
HATS cache. Every subcommand prints its own options with
`xmatcher COMMAND --help`; this page follows that output.

---

## Command syntax

```bash
xmatcher [GLOBAL_OPTIONS] COMMAND [COMMAND_OPTIONS] [ARGUMENTS...]
```

When the first positional argument is not a subcommand name, `xmatcher` runs
`xmatcher match`:

```bash
# These two invocations are identical:
xmatcher a.parquet b.csv -r 1.5
xmatcher match a.parquet b.csv -r 1.5
```

---

## Global options

Every subcommand accepts these options, before or after the subcommand name:

| Option | Default | Description |
|---|---|---|
| `-h`, `--help` | | Show help and exit. |
| `-v`, `--verbose` | `0` | Increase log verbosity (`-v` for INFO, `-vv` for DEBUG). |
| `--config CONFIG_FILE` | bundled or `~/.config/xmatcher/xmatcher.yaml` | Path to a custom `xmatcher.yaml`. `convert` does not read the catalogue configuration. |
| `--no-color` | | Disable ANSI colour output. Colour is also off when `NO_COLOR` or `XMATCHER_NO_COLOR` is set, or when the output is not a terminal. |
| `--version` | | Print the version and exit. |

---

## Subcommands

| Command | Purpose |
|---|---|
| [`match`](#xmatcher-match) | Cross-match catalogues (files, HATS directories, configured names, or explicit identifiers). |
| [`list`](#xmatcher-list) | List configured catalogues, aliases, and registered HATS catalogues. |
| [`describe`](#xmatcher-describe) | Show columns, access information and defaults for a catalogue. |
| [`discover`](#xmatcher-discover) | Browse tables and inspect schemas on a remote TAP endpoint. |
| [`search`](#xmatcher-search) | Search every known TAP endpoint for table names matching a substring. |
| [`adopt`](#xmatcher-adopt) | Probe a remote TAP table, or draft from a HATS tree, and append an entry to `~/.config/xmatcher/xmatcher.yaml`. |
| [`sync`](#xmatcher-sync) | Mirror remote TAP or HATS catalogues into the local HATS cache. |
| [`project-hats`](#xmatcher-project-hats) | Write a narrower HATS tree retaining only specified columns. |
| [`export-hats`](#xmatcher-export-hats) | Flatten a configured, synced or local HATS catalogue to one `.parquet` or `.fits` file. |
| [`convert`](#xmatcher-convert) | Convert a local catalogue between FITS, Parquet, HATS and CSV, in any direction. |
| [`completion`](#xmatcher-completion) | Emit a shell completion script for `bash`, `zsh`, or `fish`. |
| [`doctor`](#xmatcher-doctor) | Compare the active configuration with the bundled baseline and report drift. |

---

## `xmatcher match`

Matches two or more catalogues by sky position (fixed radius, positional
errors or error ellipses), with optional proper-motion epoch propagation,
likelihood ratios, Bayesian scores or machine-learning classifiers, or by
identifier columns.

### Synopsis

```bash
xmatcher match [catalogues ...] [options]
```

### Positional arguments

- `catalogues`: two or more catalogue specifications:
  - Local files: `.parquet`, `.csv`, `.tsv`, `.tab`, `.fits`, `.fit`.
  - HATS directories: path to a local HATS tree or collection.
  - Configured catalogue names or aliases, matched case-insensitively:
    `gaia`, `allwise`, `twomass`, `des_dr2`, `ps1`, etc. (`xmatcher list --aliases`).
  - Explicit identifiers: `vizier:<id>` (e.g. `vizier:II/349/ps1`),
    `tap:<endpoint>/<table>` (e.g. `tap:noirlab/nsc_dr2.object`), or
    `hats:<url-or-registry-name>` (`https://...`, `vos:...`, `s3://...`,
    `hf://datasets/...`, or a registry name; see [HATS sources](hats-sources.md)).

### Output & memory controls

| Option | Default | Description |
|---|---|---|
| `-o FILE`, `--output FILE` | CSV on stdout | Output path (`.parquet`, `.csv`, `.fits`, `.hats`). An existing file is overwritten. |
| `--hats-threshold INT` | `100000` | Maximum rows per HEALPix pixel when writing `.hats` output or converting a TAP table to HATS. |
| `--batch-size INT` | none | Positive integer, accepted for compatibility but not read by any engine; it does not limit memory. Use `--memory-budget-bytes`. |
| `--memory-budget-bytes INT` | none | Spill pairwise local CSV/Parquet matching to disk above this memory budget. |
| `--scratch-dir PATH` | system temp | Parent directory for bounded-memory partition files. |
| `--partition-order ORDER` | `auto` | Sky-zone partition order of the spill path: an integer from 0 to 12, or `auto`. |

### Geometry & spatial match options

| Option | Default | Description |
|---|---|---|
| `-r ARCSEC`, `--radius ARCSEC` | `1.0` | Match radius in arcseconds, in (0, 648000]. For `sky` it is the search radius; for every other matcher it is a hard ceiling on the separation. |
| `--ra1 COL`, `--dec1 COL` | auto | RA / Dec column names for catalogue 1. |
| `--ra2 COL`, `--dec2 COL` | auto | RA / Dec column names for catalogue 2. |
| `--columns-1 COLS` | all | Comma-separated columns to read from catalogue 1. |
| `--columns-2 COLS` | all | Comma-separated columns to read from catalogue 2. |
| `--side1 KEY=VALUE` | none | Overrides for catalogue 1 (repeatable; comma-separated pairs allowed). |
| `--side2 KEY=VALUE` | none | Overrides for catalogue 2 (same keys). |
| `--side N KEY=VALUE` | none | Overrides for catalogue `N`, any position (same keys, e.g. `--side 3 ra_err_column=err`). |

Keys accepted by `--side1`, `--side2` and `--side N`: `ra_column`,
`dec_column`, `id_column`, `ra_err_column`, `dec_err_column`, `corr_column`,
`pos_err_units`, `default_pos_error_arcsec`, `epoch`, `epoch_column`,
`pm_ra_column`, `pm_dec_column`, `parallax_column`, `radial_velocity_column`,
`frame`, `lr_magnitude_column`. An unknown key is an error.

### Match algorithms & engines

| Option | Default | Description |
|---|---|---|
| `--matcher ALGORITHM` | `sky` | Matching algorithm: `sky`, `skyerr`, `skyellipse`, `lr`, `ml`, `xgb`, `auf`, `macauff`. |
| `--max-error FLOAT` | `3.0` | N-sigma cap for `skyerr`; Mahalanobis distance cap for `skyellipse`. |
| `--engine ENGINE` | `auto` | `auto` (default dispatch), `fast` (torchsky exact zone index, in memory), `zone` (same search as `fast`), `torchsky` (torchsky `crossmatch_sky`; `sky`/`skyerr` only), `astropy`, `stilts` (Java `tmatch2`), `ray` (distributed zone match), `ray-union` (distributed N-way outer join over mirrored HATS inputs), `ray-anchor` (one row per anchor source; see [ray-anchor](ray-anchor.md)). |
| `--fallback-policy POLICY` | `warn` | When an engine or matcher cannot honour the request: `warn` logs and substitutes a supported path; `error` aborts. Use `error` for reproducible runs. |
| `--join JOIN_TYPE` | `1and2` (`1or2` with `--union`) | `1and2` (inner), `1or2` or `all` (full outer), `all1` (left), `all2` (right), `1not2` (left anti), `2not1` (right anti). |
| `--find POLICY` | `best` | `best` keeps the best match; `all` keeps every pair within the radius. |
| `--union` | off | Build a master union catalogue (full outer join across all inputs). |
| `--fof` | off | Friends-of-Friends transitive closure across all catalogues. |

### ID joins

| Option | Default | Description |
|---|---|---|
| `--id-join` | off | Join on identifier columns instead of sky position. |
| `--id1 COL` | auto | ID column for catalogue 1. |
| `--id2 COL` | auto | ID column for catalogue 2. |

### Proper motion & epoch alignment

| Option | Default | Description |
|---|---|---|
| `--target-epoch FLOAT` | none | Julian-year epoch (e.g. `2016.0`) to which coordinates are propagated using proper motions. |
| `--pm-prior` | off | Inflate positional errors for sources lacking measured proper motions (Wilson 2023). Requires `--target-epoch`. |
| `--pm-prior-mag-col COL` | none | Magnitude column used to refine the proper-motion drift dispersion. |

### Probabilistic & Bayesian matching

| Option | Default | Description |
|---|---|---|
| `--probabilistic` | off | Compute a positional Bayesian `p_match`. Both catalogues need positional errors. |
| `--priors COLS` | empty | Comma-separated photometric prior columns (e.g. `g,r`); enables `p_match`. |

### Advanced filtering & ranking

| Option | Default | Description |
|---|---|---|
| `--filter-expr SQL` | none | Polars SQL `WHERE` clause applied after candidate selection. A rejected `best` match is not replaced by the next candidate. |
| `--extra-distance-cols SPEC` | none | `column:weight` pairs for multi-dimensional ranking (e.g. `g:0.5,bp_rp:0.3`). |

### Matcher-specific options

| Option | Default | Description |
|---|---|---|
| `--lr-magnitude-column COL` | none | Magnitude column for the likelihood-ratio matcher (required with `--matcher lr`). With three or more catalogues, one name for every secondary or a comma-separated list with one per secondary (`W1mag,Jmag`); `--matcher lr` with $N \ge 3$ runs one $N$-way association (see [API](api.md#nway_match)). |
| `--lr-color-column COL` | none | Primary-catalogue magnitude column: the LR photometric term becomes the colour secondary − primary (e.g. W1 − G). |
| `--lr-q FLOAT` | `0.8` | Prior probability $Q$ that a primary source has a counterpart (0–1). |
| `--ml-color-cols COLS` | none | Comma-separated photometric features for `ml` / `xgb`. Without them, ranking falls back to separation. |
| `--ml-model-path PATH` | none | Path to save or load a pre-trained Random Forest model (`joblib`). |
| `--xgb-model-path PATH` | none | Path to save or load a pre-trained XGBoost model (`joblib`). |
| `--macauff-flux-cols COLS` | none | Comma-separated magnitude columns for `macauff` flux likelihoods. |

### Remote region cones (TAP queries)

`--ra`, `--dec` and `--radius-deg` must be given together.

| Option | Description |
|---|---|
| `--ra FLOAT` | Region centre right ascension in degrees. |
| `--dec FLOAT` | Region centre declination in degrees. |
| `--radius-deg FLOAT` | Region radius in degrees for remote cone downloads. |
| `--endpoint NAME_OR_URL` | TAP endpoint for ad-hoc table ids: a short name (`vizier`, `noirlab`, `gaia`, `cadc`) or an endpoint URL. |

### Distributed Ray union & Ray anchor

These options apply to `--engine ray-union` and `--engine ray-anchor`, except
where the table says otherwise.

| Option | Default | Description |
|---|---|---|
| `--no-sync` | off | Do not mirror remote inputs; require existing cached HATS copies. |
| `--synclimit FLOAT` | `cache.rate_limit_rps`, else `1.0` | Requests/sec rate limit when mirroring remote catalogues. |
| `--cache-root PATH` | `$XMATCHER_CACHE_ROOT`; on AstroAI/CANFAR sessions `/arc/projects/astroai/hats`; else `~/.cache/xmatcher` | Durable cache root for mirrored HATS catalogues. |
| `--task-rows INT` | `2000000` | Target rows per Ray chunk task. |
| `--max-tuples INT` | `10000` | Maximum output tuples per hub source row (`ray-union` only). |
| `--chunk-memory-gb FLOAT` | `8.0` | Per-chunk candidate-pool memory guard in GiB. |
| `--anchor-id-name COL` | `source_id` | Output name of the anchor catalogue's ID column (`ray-anchor` only). |
| `--anchor-partitions ORDER/PIX,…` | all | Process only these anchor HATS partitions, e.g. a pilot (`ray-anchor` only); `properties` names the subset as `xmatcher_anchor_partitions`. |
| `--read-threads INT` | `8` | Partition files read concurrently within one task (`ray-anchor` only). |
| `--task-memory-scale FLOAT` | `1.0` | Reserve this multiple of each task's estimated peak memory as Ray `memory`, capped at 90% of the largest node's (`ray-anchor` only); `0` schedules by CPU only. |
| `--retries INT` | `0` | Driver-level retries after a backoff; mirror gaps refill incrementally and finished chunks are skipped (`ray-union` only). |
| `--fresh-after DAYS` | always probe | Skip the mirror change-probe for copies fully synced within this many days. |
| `--min-free-gb FLOAT` | `10.0` | Fail fast when the cache root or output filesystem has less free space than this many GiB. `XMATCHER_MIN_FREE_GB` overrides the default. |

---

## `xmatcher list`

Lists every catalogue in the active configuration. Names and aliases are
matched case-insensitively by every command, and explicit identifiers
(`vizier:<id>`, `tap:<endpoint>/<table>`, `hats:<url>`) need no configuration
entry.

```bash
xmatcher list [--aliases] [--hats]
```

### Options

| Option | Description |
|---|---|
| `--aliases` | Also print alias → catalogue mappings. |
| `--hats` | List the HATS catalogues registered in the IVOA registry (cached snapshot; see [HATS sources](hats-sources.md)). |

---

## `xmatcher describe`

Prints columns, coordinate conventions, access method and defaults for a
configured catalogue name or alias.

```bash
xmatcher describe <name>
```

### Example

```bash
xmatcher describe gaia
xmatcher describe nsc
```

---

## `xmatcher discover`

Lists the tables on a remote TAP endpoint, or dumps the schema of one table.

```bash
xmatcher discover <endpoint> [--schema TABLE]
```

### Arguments & options

- `endpoint`: short name (`vizier`, `noirlab`, `gaia`, `cadc`) or a full TAP
  endpoint URL.
- `--schema TABLE`: print column names, types and descriptions for one table,
  and a suggested `xmatcher.yaml` snippet.

---

## `xmatcher search`

Searches every known TAP endpoint for tables whose name contains a substring.

```bash
xmatcher search [pattern]
```

`pattern` defaults to `*` (all tables).

### Example

```bash
xmatcher search gaia
xmatcher search des
```

---

## `xmatcher adopt`

Probes a remote TAP table, or reads a HATS tree's metadata, and appends a
catalogue entry to `~/.config/xmatcher/xmatcher.yaml`, which is merged over
the bundled configuration.

```bash
xmatcher adopt <endpoint> <table> [--name NAME] [--alias ALIAS] [--dry-run]
xmatcher adopt hats <path-or-url> [--name NAME] [--alias ALIAS] [--dry-run]
```

- `endpoint`: short name (`vizier`, `noirlab`, `gaia`), a known public TAP
  name, or `hats`.
- `table`: table id (e.g. `II/349/ps1` or `ls_dr10.tractor`); with `hats`, a
  local tree path or an `http(s)` HATS URL.

With `hats`, the entry is drafted from the tree's `properties` (coordinate,
id and epoch columns, `hats_nrows`, `obs_collection`) and, for a local tree,
its `dataset/_common_metadata` schema (motion, parallax and positional-error
columns). A local tree with a sync record gets the upstream URL as
`access_identifier` and the path in `local_paths`; otherwise the path is the
`access_identifier`. Fields that metadata cannot confirm (units, epochs) are
printed as `# unverified:` lines. Nothing is written to the tree.

### Options

| Option | Default | Description |
|---|---|---|
| `--name NAME` | derived from the table id | Catalogue key to write. |
| `--alias ALIAS` | none | Short alias pointing at the new catalogue. |
| `--dry-run` | off | Print the YAML snippet without writing the user configuration. |

---

## `xmatcher sync`

Mirrors remote catalogues (TAP tables, or HATS trees over HTTP(S), `vos:`,
`s3://` or `hf://`) into the durable cache root as HATS. Later runs re-sync
incrementally.

```bash
xmatcher sync <catalogues...> [options]
```

`catalogues` are configured names or aliases, explicit `vizier:<id>` /
`tap:<endpoint>/<table>` identifiers, or `hats:` identifiers (URLs or registry
names). The command exits with status 1 if any file or page failed. A
catalogue served in place by a verified read-only community copy or
`local_paths` entry is reported as such and not downloaded; a cache or
replica root inside a read-only root is refused.

### Options

| Option | Default | Description |
|---|---|---|
| `--rate-limit FLOAT` | catalogue `sync.rate_limit_rps`, else `cache.rate_limit_rps`, else `1.0` | Requests/sec per remote host; `0` disables throttling. |
| `--force` | off | Refetch every file or page, ignoring the incremental manifest. |
| `--cache-root PATH` | as for `match` | Durable cache root for mirrored HATS catalogues. |
| `--threads INT` | `8` | Parallel download workers. |
| `--hats-threshold INT` | `100000` | Maximum rows per HEALPix pixel when a TAP table is converted to HATS. |
| `--fresh-after DAYS` | always probe | Skip the change-probe for copies fully synced within this many days. |
| `--min-free-gb FLOAT` | `10.0` | Fail fast when the cache root has less free space than this many GiB. `XMATCHER_MIN_FREE_GB` overrides the default. |

---

## `xmatcher project-hats`

Writes a narrower HATS tree that keeps only the requested columns. The
partition layout and row counts, including zero-row partitions, are
preserved. The output must not overlap the input.

```bash
xmatcher project-hats <source> <output> --columns <col1,col2,...> [options]
```

`source` is a configured name or alias, a HATS URL, or a local HATS directory;
`output` is the destination HATS directory.

### Options

| Option | Default | Description |
|---|---|---|
| `--columns COLS` | required | Comma-separated columns to retain. |
| `--no-sync` | off | Do not mirror a remote source; require an existing cached copy. |
| `--resume` | off | Resume an interrupted projection; requires a matching `project.state`. |
| `--cache-root PATH` | as for `match` | Durable cache root for mirrored HATS catalogues. |
| `--hats-threshold INT` | `100000` | Maximum rows per HEALPix pixel when a non-HATS source is converted. |
| `--rate-limit FLOAT` | `1.0` | Requests/sec when projecting an HTTPS HATS tree directly. |

---

## `xmatcher export-hats`

Flattens one HATS catalogue into a single Parquet or FITS file. Use it for a
catalogue known to the configuration or the cache:

- a local HATS directory;
- a configured HATS name or alias, or a `hats:` identifier. A remote tree must
  already be mirrored by `xmatcher sync` under the default cache root
  (`$XMATCHER_CACHE_ROOT` or its fallback); `export-hats` does not download
  and has no `--cache-root` option.

Parquet output is a Polars scan over the Npix partitions written by a
streaming sink. FITS output (`.fits` or `.fit`) is written in one
bounded-memory pass by the same writer as [`xmatcher convert`](#xmatcher-convert).
The `_healpix_29` index column is dropped unless `--keep-healpix29` is given
(with `--columns`, it is kept only if listed). An existing output file is
replaced. Keep survey-scale catalogues partitioned (HATS) when a single file
is not required.

```bash
xmatcher export-hats <source> <output.parquet|output.fits> [--columns col1,col2,...] [--keep-healpix29]
```

```bash
xmatcher export-hats ./gaia_core.hats gaia_core.parquet
xmatcher export-hats ./gaia_core.hats gaia_core.fits --columns Source,RA_ICRS,DE_ICRS
```

### Options

| Option | Default | Description |
|---|---|---|
| `--columns COLS` | all | Comma-separated columns to retain. |
| `--keep-healpix29` | off | Keep the HATS `_healpix_29` index column. |

---

## `xmatcher convert`

Converts a catalogue between FITS, Parquet, HATS and CSV, in any direction,
between local paths. It does not resolve configured names or `hats:`
identifiers and does not read `xmatcher.yaml`; use
[`xmatcher export-hats`](#xmatcher-export-hats) to flatten a configured or
synced HATS catalogue.

The format comes from each path: `.fits`/`.fit`, `.parquet` (or a directory
of Parquet files), `.hats` (or any HATS directory as input), and `.csv`/`.tsv`.
Rows stream in batches, so memory stays bounded whatever the catalogue size.
An existing output is refused unless `--overwrite` is given.

```bash
xmatcher convert <source> <output> [options]
```

```bash
xmatcher convert gaia_field.fits gaia_field.hats
xmatcher convert gaia_field.hats gaia_field.parquet --columns source_id,ra,dec
xmatcher convert survey.parquet survey.hats --ra-col RA_ICRS --dec-col DE_ICRS
xmatcher convert survey.hats survey.fits --overwrite
```

### Options

| Option | Default | Description |
|---|---|---|
| `--columns COLS` | all | Comma-separated columns to keep, in this order. |
| `--hdu HDU` | `1` | FITS table HDU (number or EXTNAME). |
| `--ra-col COL`, `--dec-col COL` | auto | Position columns of a HATS output. Defaults to the source's HATS position columns, else the single matching common name (`ra`, `RA_ICRS`, ...); an ambiguous or missing match is an error. |
| `--hats-max-rows INT` | `1000000` | Split a HEALPix cell holding more rows than this. |
| `--hats-max-order INT` | `10` | Deepest HEALPix order of a HATS output. |
| `--keep-healpix29` | off | Keep a HATS input's `_healpix_29` column. |
| `--batch-rows INT` | `262144` | Rows per streamed batch. |
| `--overwrite` | off | Replace an existing output. |

### Behaviour

- **HATS output.** Writing a HATS tree takes three passes over the input:
  1. count rows per HEALPix cell at `--hats-max-order`;
  2. split cells holding more than `--hats-max-rows` rows, and spill each
     batch's rows to their partition;
  3. write each partition sorted by `_healpix_29` (added as a column),
     followed by the metadata.

  The tree passes `hats` strict validation and also reads as a plain Parquet
  dataset. Every row needs a finite position.
- **Reading a HATS tree.** Partitions are read in (order, pixel) order. The
  `_healpix_29` column is dropped unless `--keep-healpix29` is given.
- **FITS output.** The output is a standard binary table, written in one
  streaming pass:
  - Unsigned and `int8` integers use `TZERO`.
  - Fixed-size vectors become repeat counts.
  - Integer nulls use `TNULL`.
  - Float nulls become NaN, because FITS has no other float null.
  - String widths come from the source FITS `TFORM`, or from one extra pass
    over the string columns.

---

## `xmatcher completion`

Emits a shell completion script for commands, flags, and catalogue names.
Catalogue names and TAP endpoints are read from the active `xmatcher.yaml`
when the script is generated; if it cannot be loaded, a short built-in list
is used.

```bash
xmatcher completion {bash,zsh,fish}
```

### Shell installation

```bash
# Bash, current shell only:
eval "$(xmatcher completion bash)"

# Bash, loaded at login:
xmatcher completion bash > ~/.local/share/bash-completion/completions/xmatcher

# Zsh (a directory on $fpath):
xmatcher completion zsh > "${ZDOTDIR:-$HOME}/.zsh/completions/_xmatcher"

# Fish:
xmatcher completion fish > ~/.config/fish/completions/xmatcher.fish
```

---

## `xmatcher doctor`

Compares the active `xmatcher.yaml` with the bundled baseline and reports
three kinds of drift: outdated fields (an entry present in both differs in a
structural field such as `ra_column`), entries missing from the user
configuration, and user-only entries.

```bash
xmatcher doctor [--strict] [--quiet] [--json]
```

Exit status: `0` when no outdated field is found, `1` on drift (any drift
with `--strict`), `2` when a configuration cannot be loaded.

### Options

| Option | Default | Description |
|---|---|---|
| `--strict` | off | Exit 1 on any drift, including missing-in-user and user-only entries. |
| `--quiet` | off | Print only a one-line summary. |
| `--json` | off | Print the drift report as JSON on stdout. |

---

## Common CLI recipes

### 1. Simple local pairwise match

```bash
xmatcher match cat1.parquet cat2.csv -r 1.5 -o matched.parquet
```

### 2. Match against remote Gaia DR3

`gaia` resolves to the CDS VizieR Gaia DR3 entry. The remote service is
queried for the local sources' sky area only; the full table is not
downloaded:

```bash
xmatcher match my_targets.csv gaia -r 1.0 -o targets_gaia.parquet
```

### 3. Astrometric error ellipse match (Mahalanobis distance)

```bash
xmatcher match xray.fits optical.parquet \
    --matcher skyellipse \
    --max-error 3.0 \
    --side1 ra_err_column=e_RA,dec_err_column=e_DE,pos_err_units=arcsec \
    --side2 ra_err_column=ra_error,dec_err_column=dec_error,pos_err_units=mas
```

### 4. Proper motion propagation & Wilson PM prior

The drift prior applies to a catalogue with a known epoch but no proper-motion
columns, and only error-aware matchers use it, so both sides declare errors:

```bash
xmatcher match historical_catalog.csv gaia_field.parquet \
    --matcher skyerr \
    --target-epoch 2016.0 \
    --pm-prior \
    -r 2.0 \
    --side1 epoch=1995.0,ra_err_column=ra_err,dec_err_column=dec_err,pos_err_units=arcsec \
    --side2 epoch=2016.0,pm_ra_column=pmra,pm_dec_column=pmdec,ra_err_column=ra_error,dec_err_column=dec_error,pos_err_units=mas \
    -o propagated_matches.parquet
```

With a remote catalogue in place of `gaia_field.parquet`, add `--ra`, `--dec`
and `--radius-deg`: `skyerr`, `skyellipse` and `--target-epoch` requests do
not derive a download cone automatically.

### 5. Distributed multi-survey outer join (Ray union)

Full-sky inputs are best given as HATS catalogues; they are mirrored on demand:

```bash
xmatcher match gaia_lsdb_hats twomass_hats wise_hats \
    --union \
    --engine ray-union \
    -r 2.0 \
    -o /data/hats/master_union.hats
```

### 6. Gaia master anchor catalogue

Keeps one row per Gaia source and attaches the closest counterpart from each
auxiliary survey (see [ray-anchor](ray-anchor.md)):

```bash
xmatcher match gaia_hats ps1_hats sdss_hats \
    --engine ray-anchor \
    -r 1.5 \
    --target-epoch 2016.0 \
    -o /data/hats/gaia_master.hats
```
