# xmatcher Command-Line Interface (CLI) Reference

The `xmatcher` CLI provides command-line tools for cross-matching astronomical catalogues across local files, remote TAP/VizieR services, and partitioned HATS trees.

---

## Command Syntax

```bash
xmatcher [GLOBAL_OPTIONS] COMMAND [COMMAND_OPTIONS] [ARGUMENTS...]
```

When called with two or more catalogue arguments without a subcommand name, `xmatcher` defaults to `xmatcher match`:

```bash
# These two invocations are identical:
xmatcher a.parquet b.csv -r 1.5
xmatcher match a.parquet b.csv -r 1.5
```

---

## Global Options

All subcommands accept these global flags:

| Option | Description |
|---|---|
| `-h`, `--help` | Show command help and exit. |
| `-v`, `--verbose` | Increase log verbosity (`-v` for INFO, `-vv` for DEBUG). |
| `--config CONFIG_FILE` | Path to a custom `xmatcher.yaml` (overrides default bundled or `~/.config/xmatcher/xmatcher.yaml`). |
| `--no-color` | Disable ANSI colour output. (Also respected via `NO_COLOR=1` or `XMATCHER_NO_COLOR=1`). |
| `--version` | Display version number and exit. |

---

## Subcommands Summary

| Command | Purpose |
|---|---|
| [`match`](#xmatcher-match) | Cross-match catalogues (files, HATS directories, configured names, or explicit identifiers). |
| [`list`](#xmatcher-list) | List configured catalogues, aliases, and known HATS collections. |
| [`describe`](#xmatcher-describe) | Show column schema, access metadata, and defaults for a catalogue. |
| [`discover`](#xmatcher-discover) | Browse tables and inspect schemas on a remote TAP endpoint. |
| [`search`](#xmatcher-search) | Search known TAP archives for table names matching a pattern. |
| [`adopt`](#xmatcher-adopt) | Probe a remote TAP table and append an entry to `~/.config/xmatcher/xmatcher.yaml`. |
| [`sync`](#xmatcher-sync) | Mirror remote TAP or cloud HATS catalogues into the durable local cache. |
| [`project-hats`](#xmatcher-project-hats) | Write a narrower HATS tree retaining only specified columns. |
| [`completion`](#xmatcher-completion) | Generate shell auto-completion scripts for `bash`, `zsh`, or `fish`. |
| [`doctor`](#xmatcher-doctor) | Validate local configuration against the bundled baseline and report drift. |

---

## `xmatcher match`

Matches two or more catalogues using spatial distance, positional error ellipses, proper-motion epoch propagation, likelihood ratios, or machine-learning classifiers.

### Synopsis

```bash
xmatcher match [catalogues ...] [options]
```

### Positional Arguments

- `catalogues`: Two or more catalogue specifications:
  - Local files: `.parquet`, `.csv`, `.tsv`, `.tab`, `.fits`.
  - HATS directories: path to local HATS directory.
  - Configured catalogue names or aliases: `gaia`, `allwise`, `twomass`, `des_dr2`, `ps1`, etc.
  - Explicit identifiers: `vizier:<id>` (e.g. `vizier:II/349/ps1`), `tap:<endpoint>/<table>` (e.g. `tap:noirlab/nsc_dr2.object`), or `hats:<url>` (`https://...`, `s3://...`, `hf://datasets/...`).

### Output & Memory Controls

| Option | Default | Description |
|---|---|---|
| `-o FILE`, `--output FILE` | stdout (CSV) | Output path (`.parquet`, `.csv`, `.fits`, `.hats`). |
| `--hats-threshold INT` | `100000` | Maximum rows per HEALPix pixel when saving `.hats` output. |
| `--batch-size INT` | `None` | HEALPix pixel groups per zone batch. |
| `--memory-budget-bytes INT` | `None` | Spill pairwise local CSV/Parquet matching above this memory budget. |
| `--scratch-dir PATH` | system temp | Parent directory for temporary partition files during bounded-memory runs. |
| `--partition-order INT` | `auto` | Sky-zone partition order (non-negative integer or `auto`). |

### Geometry & Spatial Match Options

| Option | Default | Description |
|---|---|---|
| `-r ARCSEC`, `--radius ARCSEC` | `1.0` | Match radius in arcseconds for the `sky` matcher. |
| `--ra1 COL`, `--dec1 COL` | auto | Right Ascension / Declination column names for catalogue 1. |
| `--ra2 COL`, `--dec2 COL` | auto | Right Ascension / Declination column names for catalogue 2. |
| `--columns-1 COLS` | all | Comma-separated list of columns to read from catalogue 1. |
| `--columns-2 COLS` | all | Comma-separated list of columns to read from catalogue 2. |
| `--side1 KEY=VALUE` | `None` | Overrides for catalogue 1 (repeatable). Keys: `ra_column`, `dec_column`, `id_column`, `ra_err_column`, `dec_err_column`, `corr_column`, `pos_err_units`, `default_pos_error_arcsec`, `epoch`, `epoch_column`, `pm_ra_column`, `pm_dec_column`, `parallax_column`, `radial_velocity_column`, `frame`. |
| `--side2 KEY=VALUE` | `None` | Overrides for catalogue 2 (repeatable, same keys as `--side1`). |

### Match Algorithms & Engines

| Option | Default | Description |
|---|---|---|
| `--matcher ALGORITHM` | `sky` | Matching algorithm: `sky`, `skyerr`, `skyellipse`, `lr`, `ml`, `xgb`, `auf`, `macauff`. |
| `--max-error FLOAT` | `3.0` | N-sigma cap for `skyerr`; Mahalanobis distance cutoff for `skyellipse`. |
| `--engine ENGINE` | `auto` | Execution engine: `auto`, `fast` (SciPy cKDTree), `zone` (HEALPix), `ray` (distributed zone), `ray-union` (full-sky outer join), `ray-anchor` (anchor-centric 1-to-N master join), `astropy`, `stilts`, `torchsky`. |
| `--fallback-policy POLICY` | `warn` | Policy when engine cannot honour the request: `warn` (log warning and fall back) or `error` (abort). |
| `--join JOIN_TYPE` | `1and2` | Relational join mode: `1and2` (inner), `1or2` or `all` (full outer), `all1` (left), `all2` (right), `1not2` (left anti), `2not1` (right anti). |
| `--find POLICY` | `best` | Candidate selection policy: `best` (closest/highest ranked match) or `all` (all pairs within radius). |
| `--union` | `False` | Build a multi-survey full-outer-join master catalogue. |
| `--fof` | `False` | Run Friends-of-Friends transitive closure clustering across all catalogues. |

### ID Joins

| Option | Default | Description |
|---|---|---|
| `--id-join` | `False` | Join on relational identifier columns instead of spatial coordinates. |
| `--id1 COL` | auto | ID column name for catalogue 1. |
| `--id2 COL` | auto | ID column name for catalogue 2. |

### Proper Motion & Epoch Alignment

| Option | Default | Description |
|---|---|---|
| `--target-epoch FLOAT` | `None` | Julian year (e.g. `2016.0`) to propagate coordinates via proper motion. |
| `--pm-prior` | `False` | Inflate positional errors for sources lacking measured proper motions (Wilson 2023). Requires `--target-epoch`. |
| `--pm-prior-mag-col COL` | `None` | Magnitude column to refine proper-motion drift dispersion estimates. |

### Probabilistic & Bayesian Matching

| Option | Default | Description |
|---|---|---|
| `--probabilistic` | `False` | Compute Bayesian positional posterior match probability `p_match`. Both catalogues require positional errors. |
| `--priors COLS` | `""` | Comma-separated list of photometric prior columns (e.g. `g,r`) for photometric qualification. |

### Advanced Filtering & Ranking

| Option | Default | Description |
|---|---|---|
| `--filter-expr SQL` | `None` | Polars SQL WHERE clause applied after candidate selection. |
| `--extra-distance-cols SPEC` | `None` | `Column:weight` pairs for multi-dimensional feature ranking (e.g. `g:0.5,bp_rp:0.3`). |

### Matcher-Specific Options

| Option | Description |
|---|---|
| `--lr-magnitude-column COL` | Magnitude column for Likelihood Ratio matcher (required when `--matcher lr`). |
| `--lr-q FLOAT` | Prior probability $Q$ that a primary source has a counterpart (default: `0.8`). |
| `--ml-color-cols COLS` | Comma-separated photometric features for `ml` / `xgb` matchers. |
| `--ml-model-path PATH` | Path to save or load a pre-trained Random Forest model (`joblib`). |
| `--xgb-model-path PATH` | Path to save or load a pre-trained XGBoost model (`joblib`). |
| `--macauff-flux-cols COLS` | Comma-separated magnitude columns for `macauff` flux likelihoods. |

### Remote Region Cones (TAP Queries)

| Option | Description |
|---|---|
| `--ra FLOAT` | Center Right Ascension in degrees for remote cone queries. |
| `--dec FLOAT` | Center Declination in degrees for remote cone queries. |
| `--radius-deg FLOAT` | Radius in degrees for remote cone downloads. |
| `--endpoint NAME_OR_URL` | Explicit TAP service short-name (`vizier`, `noirlab`, `gaia`, `cadc`) or endpoint URL. |

### Distributed Ray Union & Ray Anchor

| Option | Default | Description |
|---|---|---|
| `--no-sync` | `False` | Skip auto-mirroring of remote inputs; require existing cached HATS copies. |
| `--synclimit FLOAT` | `1.0` | Requests/sec rate limit when syncing remote catalogues. |
| `--cache-root PATH` | auto | Durable cache directory for mirrored HATS trees (or `$XMATCHER_CACHE_ROOT`). |
| `--task-rows INT` | `2000000` | Target rows per Ray task chunk. |
| `--max-tuples INT` | `10000` | Maximum candidate tuples per hub source row. |
| `--chunk-memory-gb FLOAT` | `8.0` | Memory guard per chunk worker in GiB. |
| `--anchor-id-name COL` | `source_id` | Name for the anchor catalogue's primary key column in the output. |
| `--retries INT` | `0` | Driver-level retry attempts on transient network or task failures. |
| `--fresh-after FLOAT` | `None` | Skip mirror change-probes for local copies synced within this many days. |
| `--min-free-gb FLOAT` | `10.0` | Fail fast if disk free space falls below this threshold in GiB. |

---

## `xmatcher list`

Lists all configured catalogues, aliases, and known public collections.

```bash
xmatcher list [options]
```

### Options

| Option | Description |
|---|---|
| `--aliases` | Display full alias-to-canonical mapping table. |
| `--hats` | List HATS catalogues available in the cached IVOA registry. |

---

## `xmatcher describe`

Inspects columns, coordinate conventions, default access methods, and metadata for a configured catalogue.

```bash
xmatcher describe <catalogue_name>
```

### Example

```bash
xmatcher describe gaia
xmatcher describe nsc
```

---

## `xmatcher discover`

Queries a remote TAP endpoint to list available tables, or dumps the schema of a specific table.

```bash
xmatcher discover <endpoint> [--schema TABLE]
```

### Arguments & Options

- `endpoint`: Short-name (`vizier`, `noirlab`, `gaia`, `cadc`) or full TAP endpoint URL.
- `--schema TABLE`: Dump column names, types, descriptions, and emit a template YAML snippet for `xmatcher.yaml`.

---

## `xmatcher search`

Searches across all configured TAP archives for table names matching a search pattern.

```bash
xmatcher search [pattern]
```

### Example

```bash
xmatcher search gaia
xmatcher search des
```

---

## `xmatcher adopt`

Queries a remote TAP table and automatically appends a ready-to-use catalogue entry to `~/.config/xmatcher/xmatcher.yaml`.

```bash
xmatcher adopt <endpoint> <table> [--name NAME] [--alias ALIAS] [--dry-run]
```

### Options

| Option | Description |
|---|---|
| `--name NAME` | Catalogue key to register (default: derived from table name). |
| `--alias ALIAS` | Optional short alias. |
| `--dry-run` | Print generated YAML configuration without writing to disk. |

---

## `xmatcher sync`

Downloads and mirrors remote TAP catalogues or cloud HATS trees into the local HATS cache root for fast offline matching.

```bash
xmatcher sync <catalogues...> [options]
```

### Options

| Option | Default | Description |
|---|---|---|
| `--rate-limit FLOAT` | `1.0` | Maximum requests per second per remote host (0 disables throttling). |
| `--force` | `False` | Refetch every partition, ignoring cached manifests. |
| `--cache-root PATH` | auto | Destination cache root directory. |
| `--threads INT` | `8` | Parallel download worker threads. |
| `--hats-threshold INT` | `100000` | Max rows per HEALPix pixel when converting TAP tables to HATS. |
| `--fresh-after FLOAT` | `None` | Skip change-probe if cache was synced within this many days. |
| `--min-free-gb FLOAT` | `10.0` | Abort if destination disk space is less than this threshold in GiB. |

---

## `xmatcher project-hats`

Creates a lightweight, narrowed HATS tree by extracting only requested columns from a wide catalogue while preserving HEALPix partition topology and index ordering.

```bash
xmatcher project-hats <source> <output> --columns <col1,col2,...> [options]
```

### Options

| Option | Default | Description |
|---|---|---|
| `--columns COLS` | (Required) | Comma-separated columns to retain in the projection. |
| `--no-sync` | `False` | Require local cache; do not download missing partitions. |
| `--resume` | `False` | Resume an interrupted projection using `project.state`. |
| `--cache-root PATH` | auto | Local cache root directory. |
| `--hats-threshold INT` | `100000` | Max rows per pixel if converting a non-HATS source. |
| `--rate-limit FLOAT` | `1.0` | Requests/sec when streaming directly from HTTPS HATS trees. |

---

## `xmatcher completion`

Generates shell completion scripts for tab-completion of commands, flags, and catalogue aliases.

```bash
xmatcher completion <shell>
```

### Shell Installation

```bash
# Bash:
eval "$(xmatcher completion bash)"

# Zsh:
xmatcher completion zsh > "$HOME/.zsh/completions/_xmatcher"

# Fish:
xmatcher completion fish > "$HOME/.config/fish/completions/xmatcher.fish"
```

---

## `xmatcher doctor`

Compares your active local `~/.config/xmatcher/xmatcher.yaml` against the bundled configuration baseline and reports any schema drift or deprecated fields.

```bash
xmatcher doctor [options]
```

### Options

| Option | Default | Description |
|---|---|---|
| `--strict` | `False` | Exit with code 1 on any configuration drift (including local custom additions). |
| `--quiet` | `False` | Suppress detailed field diffs; print only a summary line. |
| `--json` | `False` | Output drift analysis as machine-readable JSON. |

---

## Common CLI Recipes

### 1. Simple Local Pairwise Match

```bash
xmatcher match cat1.parquet cat2.csv -r 1.5 -o matched.parquet
```

### 2. Match Against Remote Gaia DR3

Downloads the required sky cone on-the-fly and joins with local targets:

```bash
xmatcher match my_targets.csv gaia -r 1.0 -o targets_gaia.parquet
```

### 3. Astrometric Error Ellipse Match (Mahalanobis Distance)

```bash
xmatcher match xray.fits optical.parquet \
    --matcher skyellipse \
    --max-error 3.0 \
    --side1 ra_err_column=e_RA,dec_err_column=e_DE,pos_err_units=arcsec \
    --side2 ra_err_column=ra_error,dec_err_column=dec_error,pos_err_units=mas
```

### 4. Proper Motion Propagation & Wilson PM Prior

```bash
xmatcher match historical_catalog.csv gaia \
    --target-epoch 2016.0 \
    --pm-prior \
    -r 2.0 \
    -o propagated_matches.parquet
```

### 5. Distributed Multi-Survey Outer Join (Ray Union)

```bash
xmatcher match gaia allwise twomass des \
    --union \
    --engine ray-union \
    -o /data/hats/master_union.hats
```

### 6. Gaia Master Anchor Catalogue

Keeps one row per Gaia source and attaches the closest counterpart from each auxiliary survey:

```bash
xmatcher match gaia ps1 sdss \
    --engine ray-anchor \
    -r 1.5 \
    -o /data/hats/gaia_master.hats
```
