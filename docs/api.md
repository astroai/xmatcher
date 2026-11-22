# xmatcher Python API reference

This page covers the stable public surface of xmatcher 0.7.0: the names in
`xmatcher.__all__`, plus the modules named in their own sections below
(`xmatcher.convert`, `xmatcher.mirror`, `xmatcher.association`). Any other
module or name is internal.

xmatcher needs Python 3.13 or later. `torchsky>=0.6.0` (sky indexing and
association statistics) and `torchfits>=1.3.3` (FITS tables) are core
dependencies, together with Polars, PyArrow, NumPy, SciPy, Astropy, PyVO and
PyYAML. Ray, cdshealpix, hats, astroquery and the ML packages are extras.

`import xmatcher` is lazy: each public name is imported on first access.

---

## `CrossMatch`

`CrossMatch` loads the catalogue registry, resolves inputs, manages remote
archive access, checks that the inputs declare compatible coordinate frames,
and dispatches requests to the matching engines. It does not transform
coordinates between frames.

```python
from xmatcher import CrossMatch

cm = CrossMatch()  # bundled xmatcher.yaml plus the user overlay
```

```python
CrossMatch(
    config_file: str | Path | None = None,
    *,
    stilts_cmd_base: str | None = None,
    java_opts: str | None = None,
    tmpdir: str | None = None,
    endpoint: str | None = None,
)
```

- `config_file`: a YAML file to use instead of the bundled registry and user
  overlay. A missing file raises `ConfigError`.
- `stilts_cmd_base`, `java_opts`, `tmpdir`: STILTS command, JVM options and
  scratch directory; default to the `stilts_config` block of the registry.
- `endpoint`: default TAP endpoint for bare table ids that do not name one.

The registry is validated on load: an alias that shadows a catalogue key,
two aliases that differ only by case, an alias cycle, or an alias chain that
does not end at a catalogue key raises `ConfigError`.

### Methods

| Method | Purpose |
|---|---|
| [`crossmatch_request(req)`](#crossmatch_request) | Typed pairwise entry point taking a `MatchRequest`. |
| [`crossmatch(...)`](#crossmatch) | Pairwise match with keyword parameters. |
| [`crossmatch_multi(...)`](#crossmatch_multi) | Chain of pairwise matches over $N \ge 2$ catalogues; with `matcher="lr"` and $N \ge 3$, one $N$-way association. |
| [`union_match(...)`](#union_match) | Full-outer-join union catalogue (in memory, or distributed to HATS). |
| [`anchor_match(...)`](#anchor_match) | One row per anchor source with the best counterpart per survey (HATS output). |
| [`nway_match(...)`](#nway_match) | Simultaneous Bayesian $N$-way association (torchsky `associate_many`). |
| [`fof_match(...)`](#fof_match) | Friends-of-friends bundles over all cross-catalogue edges. |
| `resolve_source(value, overrides)` | Build a `CatalogueSource` from a frame, path, configured name or alias, remote HATS URI, or `vizier:` / `tap:` / `hats:` identifier. `overrides` is a dict of `SideOverrides` keys. |
| `resolve_name(name)` | Canonical catalogue key for a name or alias (case-insensitive, follows alias chains). Unknown names are returned lower-cased. |
| `get_catalogue_config(name)` | Registry entry for a name or alias merged with its archive service settings. Raises `CrossMatchError` for an unknown catalogue, archive or service. |

### Keyword parameters

`crossmatch`, `crossmatch_multi`, `union_match`, `anchor_match`, `nway_match`
and `fof_match` take matching options as `**params`:

- every `MatchSpec` field (`radius_arcsec`, `matcher`, `find`, ...);
- the `MatchRequest` options `engine`, `id_join`, `ra`, `dec`, `radius_deg`,
  `memory_budget_bytes`, `scratch_dir`, `partition_order`, plus `endpoint`,
  `columns_1`, `columns_2` and `hats_threshold`;
- per-catalogue overrides as `<key>_<n>`, where `<key>` is a column or
  astrometry field of `SideOverrides` and `n` is the 1-based catalogue
  position (`ra_column_1`, `pos_err_units_3`); `n` may not exceed the number
  of catalogues;
- options of the distributed `ray-union` and `ray-anchor` routes
  (`no_sync`, `force_sync`, `synclimit`, `threads`, `task_rows`,
  `cache_root`, `max_tuples`, `chunk_memory_gb`, `anchor_id_name`, `retries`,
  `fresh_after`, `min_free_gb`).

Any other keyword raises `TypeError` with close-match suggestions, so a
misspelling (`radius=5`) does not silently run with the default.

---

### `crossmatch_request`

```python
def crossmatch_request(
    self,
    req: MatchRequest,
    *,
    hats_threshold: int = 100_000,
    progress_cb: Callable[[str], None] | None = None,
) -> pl.DataFrame | pl.LazyFrame | None
```

- `req`: the request. Field validation happens when `MatchRequest` and
  `MatchSpec` are constructed; source metadata and engine support are checked
  at run time.
- `hats_threshold`: maximum rows per HEALPix pixel when `req.output_file` is a
  `.hats` tree.
- `progress_cb`: called with progress messages.

Returns a `pl.DataFrame`, a `pl.LazyFrame` when `req.lazy` is true, or `None`
when `req.output_file` is set and the result is written there.

```python
from xmatcher import CrossMatch, MatchRequest, MatchSpec

request = MatchRequest(
    cat1="cat_a.parquet",
    cat2="cat_b.parquet",
    spec=MatchSpec(radius_arcsec=1.5, matcher="sky", find="best"),
)
result = CrossMatch().crossmatch_request(request)
```

---

### `crossmatch`

```python
def crossmatch(
    self,
    catalogue_1_input: FrameInput,
    catalogue_2_input: FrameInput,
    output_file: str | Path | None = None,
    *,
    lazy: bool = False,
    progress_cb: Callable[[str], None] | None = None,
    **params: Any,
) -> pl.DataFrame | pl.LazyFrame | None
```

`FrameInput` is `str | Path | pl.DataFrame | pl.LazyFrame`. Builds a
`MatchRequest` from the arguments and runs it. There is no `spec=` parameter;
pass a `MatchSpec` through `crossmatch_request`.

- `catalogue_1_input`, `catalogue_2_input`: a local file (`.parquet`, `.csv`,
  `.tsv`, `.tab`, `.fits`, `.fit`), a local HATS directory, a configured
  catalogue name or alias (`"gaia"`, `"des"`), a remote HATS URI, an explicit
  `vizier:<id>` / `tap:<endpoint>/<table>` / `hats:<url-or-name>` identifier,
  or a Polars frame. See [naming.md](naming.md) for the resolution order.
- `output_file`: destination file or tree; the format follows the suffix
  (see `write_frame`). Writing may stream, but the match itself is in memory
  unless the spill path below applies.
- `lazy`: return an uncollected `pl.LazyFrame`.
- `**params`: see [Keyword parameters](#keyword-parameters). The main ones:
  - `radius_arcsec` (`float`): search radius in arcseconds.
  - `matcher` (`str`): `"sky"`, `"skyerr"`, `"skyellipse"`, `"lr"`, `"ml"`,
    `"xgb"`, `"auf"`, `"macauff"`.
  - `engine` (`str`): `"auto"`, `"stilts"`, `"astropy"`, `"fast"`,
    `"torchsky"`, `"zone"`, `"ray"`, `"ray-union"`, `"ray-anchor"`. See
    [Execution engines](#execution-engines).
  - `join_type` (`str`): `"1and2"` (inner), `"all1"` (left), `"all2"`
    (right), `"1or2"` or `"all"` (full outer), `"1not2"` (left anti),
    `"2not1"` (right anti).
  - `find` (`str`): `"best"` (nearest or top-ranked counterpart) or `"all"`
    (every pair within the criterion).
  - `ra`, `dec`, `radius_deg`: cone for remote downloads.

When `memory_budget_bytes` is set and both inputs are local CSV/Parquet
files, a pairwise match whose projected inputs exceed the budget runs on the
bounded-memory spill path (partitioned by sky zone under `scratch_dir`) and
writes `output_file` directly.

---

### `crossmatch_multi`

```python
def crossmatch_multi(
    self,
    catalogues: list[FrameInput],
    output_file: str | Path | None = None,
    *,
    lazy: bool = False,
    progress_cb: Callable[[str], None] | None = None,
    **params: Any,
) -> pl.DataFrame | pl.LazyFrame | None
```

Chains pairwise matches: catalogue 1 is matched with catalogue 2, and the
accumulated result with each later catalogue in turn. Each step matches on
catalogue 1's position; rows without one (full-outer joins) use the first
available later catalogue's position. Colliding column names from catalogue
`k` get the suffix `_k` (`_2`, `_3`, ...). At least two catalogues are
required.

With `matcher="lr"` and three or more catalogues (and not via `union_match`),
the call is one $N$-way association instead of a chain; see
[`nway_match`](#nway_match).

```python
# Three-way inner match: local Parquet, local CSV and remote Gaia in a cone
result = cm.crossmatch_multi(
    ["my_sources.parquet", "wise_cutout.csv", "gaia_esa"],
    ra=279.23,
    dec=38.78,
    radius_deg=0.01,
    radius_arcsec=1.5,
)
```

---

### `union_match`

```python
def union_match(
    self,
    catalogues: list[FrameInput],
    output_file: str | Path | None = None,
    *,
    lazy: bool = False,
    progress_cb: Callable[[str], None] | None = None,
    **params: Any,
) -> pl.DataFrame | pl.LazyFrame | None
```

Builds a union catalogue with full outer joins (`join_type` defaults to
`"1or2"`). Rows found in one catalogue only keep their own coordinates and
columns; the other catalogues' columns are null. The `_src_cats` column lists
the contributing catalogues (`"1+2+3"`).

- With `engine="auto"`, no cone (`ra`, `dec`, `radius_deg`), and every input
  remote, the call routes to `engine="ray-union"` and writes a full-sky HATS
  tree. An `output_file` is then required; without one the call raises
  `CrossMatchError`.
- `engine="ray-union"` supports `sky`, `skyerr` and `skyellipse`, full-outer
  join types only, and HATS output only. It rejects regions, filters, ID
  joins, probabilistic scores and extra distance columns. Interval planning
  handles mixed-order and RING inputs; distributed tasks widen the search by
  measured per-source uncertainty and epoch-motion halos, so planning may read
  additional partitions.
- `engine="ray"` with a full-outer join, a `.hats` or `vos:` output, a
  `sky`/`skyerr`/`skyellipse` matcher, no in-memory frames and no scoring
  options is also routed to `ray-union`. Otherwise `engine="ray"` distributes
  only the pairwise candidate search; inputs are materialized and the result
  is collected on the coordinator.
- The pairwise native HATS path materializes both catalogues for adaptive,
  RING, mixed-order and epoch-aligned cases.
- HATS union outputs carry non-null `_union_ra` and `_union_dec` routing
  coordinates taken from the lowest-indexed catalogue in each row; the tree
  metadata declares NESTED ordering and these columns as the spatial
  coordinates.
- Valid zero-row HATS inputs keep their advertised schema. The first nonempty
  input supplies the output tiling without changing catalogue order or column
  suffixes; if every input is empty, the result is a readable zero-row HATS
  catalogue.
- With `memory_budget_bytes`, a two-catalogue union of local CSV/Parquet
  files may use the spill path; more than two catalogues raises
  `CrossMatchError`.

---

### `anchor_match`

```python
def anchor_match(
    self,
    catalogues: list[FrameInput],
    output_file: str | Path | None = None,
    *,
    lazy: bool = False,
    progress_cb: Callable[[str], None] | None = None,
    **params: Any,
) -> None
```

Runs `engine="ray-anchor"`: one row per anchor source with the best
counterpart from each survey. Unlike `union_match`, sources found only in a
survey do not create rows.

- The first catalogue is the anchor; every later catalogue is a survey. An
  output HATS path is required, and `lazy=True` raises `CrossMatchError`.
- The output keeps the anchor's selected columns (its schema, or its
  `default_columns`). With `target_epoch`, coordinates are propagated and the
  `properties` file records the epoch in `hats_epoch`. The anchor ID column is
  renamed to `source_id` by default (`anchor_id_name`, CLI
  `--anchor-id-name`); an anchor without a usable identifier is rejected.
- Per survey, the output adds `<survey>_sep_arcsec`, `<survey>_sep_second`,
  `<survey>_n_within`, `<survey>_crowded`, `<survey>_pm_warn` and
  `<survey>_quality`.
- Only `matcher="sky"` and `find="best"` are supported. ID joins, filters,
  probabilistic scores, `prior_columns`, `pm_prior`, regions, extra distance
  columns, `columns_1`/`columns_2`, `memory_budget_bytes` and a `vos:` output
  raise `CrossMatchError`. The output is always a left join from the anchor;
  `join_type` may be `"1and2"`, `"1or2"` or `"all"`, and other values raise.
- `pm_warn` flags anchor motion over the anchor-survey epoch baseline.
- The run is resumable per anchor partition; a changed plan fingerprint
  removes the old dataset instead of mixing plans. See
  [ray-anchor.md](ray-anchor.md).

---

### `nway_match`

```python
def nway_match(
    self,
    catalogues: list[FrameInput],
    output_file: str | Path | None = None,
    *,
    radius_arcsec: float = 1.0,
    prior_columns: list[str] | None = None,
    max_tuples_per_source: int = 10_000,
    chunk_size: int = 50_000,
    hats_threshold: int = 100_000,
    progress_cb: Callable[[str], None] | None = None,
    **params: Any,
) -> pl.DataFrame | None
```

Simultaneous Bayesian association of two or more catalogues with
`torchsky.catalogs.associate_many` (Budavári & Szalay 2008 with full
covariances, per-catalogue completeness, background density and photometric
likelihood ratios). Catalogue 1 is the primary.

Inputs are resolved and materialized (remote TAP inputs are downloaded,
remote HATS trees loaded in full), optionally propagated to `target_epoch`,
and associated in process. For each primary row, every combination of one
gated counterpart or none per other catalogue is a hypothesis; posteriors are
normalized over all of them, and a secondary row is the counterpart of at most
one primary. Every catalogue needs positional covariances (declared errors
and correlations, propagated epoch covariances, or an explicit
per-catalogue error floor), used in full. Candidates pass a $\chi^2_2$ gate
($p = 10^{-4}$) and the `radius_arcsec` ceiling.

- `max_tuples_per_source`: primaries with more hypotheses are enumerated
  best-first, with a certified bound on the omitted posterior mass; `0`
  enumerates every hypothesis.
- `chunk_size`: validated (integer $\ge 1$) and otherwise unused; the
  association runs in one pass.
- `hats_threshold`: maximum rows per HEALPix pixel for `.hats` output.
- `engine`: only `"auto"` and `"fast"` are meaningful. Any other value logs a
  warning and runs in process, or raises `CrossMatchError` under
  `fallback_policy="error"`.

Photometric terms are learnt by EM, as for `matcher="lr"`:
`lr_magnitude_column` (every secondary) or `lr_magnitude_column_<n>`
(catalogue `n`) adds a magnitude likelihood ratio; `lr_color_column` (a
primary column) turns it into the colour secondary − primary; each
`prior_columns` entry, which must exist in every catalogue, adds the colour
`secondary[col] - primary[col]`. `lr_q` fixes every secondary's completeness
(default: fitted). `find="all"` (the default here) returns every hypothesis
with at least one counterpart; `find="best"` returns each primary's maximum
a posteriori (MAP) hypothesis. See
[algorithms §6C](algorithms.md#6-probabilistic--bayesian-matchers).

**Output columns**, after every catalogue's columns (suffixed `_2`, `_3`, ...
on collision; null for absent members):

| Column | Meaning |
|---|---|
| `sep_arcsec_<k>` | Separation of catalogue `k`'s member from the primary, arcsec. |
| `p_match` | Posterior probability of the hypothesis. |
| `lr` | Likelihood ratio of the hypothesis against "no counterpart", completeness priors divided out (the pairwise `matcher="lr"` value for two catalogues). |
| `p_any` | Probability that the primary has any counterpart. |
| `nway_best` | The hypothesis is the primary's MAP hypothesis. |

**`crossmatch_multi(..., matcher="lr")` with three or more catalogues** runs
the same association with these differences:

- `lr_q` (default `0.8`) is every secondary's completeness;
- a magnitude column (`lr_magnitude_column` or `lr_magnitude_column_<n>`) is
  required for every secondary;
- the posterior column is `reliability` instead of `p_match`; `sep_arcsec_<k>`,
  `lr`, `p_any` and `nway_best` are as above;
- `find` defaults to `"best"`;
- `join_type="1and2"` (default) keeps primaries with a counterpart,
  `"all1"` adds primaries without one; other join types, `filter_expr`,
  `extra_distance_cols`, `prior_columns` and `probabilistic` raise
  `CrossMatchError`;
- at most 4096 hypotheses per primary are enumerated before the bounded
  best-first cut.

---

### `fof_match`

```python
def fof_match(
    self,
    catalogues: list[FrameInput],
    output_file: str | Path | None = None,
    *,
    radius_arcsec: float = 1.0,
    hats_threshold: int = 100_000,
    progress_cb: Callable[[str], None] | None = None,
    **params: Any,
) -> pl.DataFrame | None
```

Friends-of-friends grouping: finds the cross-catalogue edges for every pair
of catalogues (`find="all"`), then takes connected components of the edge
graph (`scipy.sparse.csgraph.connected_components`). With `matcher="sky"`,
edges come from a SciPy `cKDTree` ball query on unit vectors; other matchers
use the `fast` engine; `engine="ray"` distributes edge discovery, but input
frames and components are still assembled on the coordinator.

Only components containing a source from the first catalogue are emitted;
isolated first-catalogue sources are kept, secondary-only components are
dropped. First-catalogue columns come from the bundle's first primary row
unchanged. For other catalogues, float columns are NaN-ignoring means, RA/Dec
a unit-vector mean, and every other column (integer IDs, strings) the first
member's value with its dtype kept. See
[usage §7](usage.md#7-friends-of-friends-output-policy).

**Output columns** (first):

| Column | Meaning |
|---|---|
| `bundle_id` | Integer label of the connected component, ordered by its first primary row. |
| `n_cats` | Number of distinct catalogues in the bundle. |
| `_src_cats` | Membership string, e.g. `"1+2+3"`. |

---

## Request objects

### `MatchRequest`

`xmatcher.MatchRequest` (defined in `xmatcher.request`): one pairwise request.

| Field | Type | Default | Description |
|---|---|---|---|
| `cat1`, `cat2` | `FrameInput` | required | Input catalogues (see `crossmatch`). |
| `spec` | `MatchSpec` | `MatchSpec()` | Matching criteria. |
| `output_file` | `str \| Path \| None` | `None` | Write the result here and return `None`. |
| `lazy` | `bool` | `False` | Return an uncollected `pl.LazyFrame`. |
| `memory_budget_bytes` | `int \| None` | `None` | Use the spill path for local CSV/Parquet inputs whose projected size exceeds this budget. |
| `scratch_dir` | `str \| Path \| None` | `None` | Parent directory for spill partitions. |
| `partition_order` | `str \| int` | `"auto"` | Sky-zone order of the spill path. |
| `engine` | `str` | `"auto"` | Engine name (see [Execution engines](#execution-engines)). |
| `id_join` | `bool` | `False` | Relational join on ID columns instead of a positional match. |
| `id_column_1`, `id_column_2` | `str \| None` | `None` | ID columns for `id_join`. |
| `side1`, `side2` | `SideOverrides` | `SideOverrides()` | Per-side column and astrometry overrides. |
| `ra`, `dec`, `radius_deg` | `float \| None` | `None` | Cone for remote TAP/CDS downloads; give all three or none. Required when both inputs are remote (except the same-service TAP join used for `sky`, `find="all"`, `join_type="1and2"`) and for remote `skyerr`, `skyellipse` or `target_epoch` requests, whose uncertainty or motion halo cannot be inferred. |
| `probabilistic` | `bool` | `False` | Add positional `p_match` scoring. Both catalogues need declared positional errors or an explicit `default_pos_error_arcsec`. Not valid with `id_join`. |

Invalid field values raise `ValueError` at construction.

### `MatchSpec`

`xmatcher.MatchSpec` (defined in `xmatcher.matchers`): the matching criteria.
Invalid values (unknown `matcher`, `join_type`, `find` or `fallback_policy`,
non-finite or out-of-range numbers) raise `ValueError` at construction.

| Field | Type | Default | Description |
|---|---|---|---|
| `radius_arcsec` | `float` | `1.0` | Search radius for `sky`, and an upper bound on the pair separation for every other matcher; in $(0, 648000]$. |
| `matcher` | `str` | `"sky"` | `"sky"`, `"skyerr"`, `"skyellipse"`, `"lr"`, `"ml"`, `"xgb"`, `"auf"`, `"macauff"`. |
| `max_error` | `float` | `3.0` | $N$-$\sigma$ cap for `skyerr`; cap on the Mahalanobis distance for `skyellipse` ($d_{\text{M}}^2 \le$ `max_error`²). |
| `lr_magnitude_column` | `str \| None` | `None` | Secondary magnitude column for `matcher="lr"` (required there); `lr_magnitude_column_<n>` names catalogue `n`'s column in $N$-way matches. |
| `lr_color_column` | `str \| None` | `None` | Primary magnitude column; the LR photometric term becomes the colour secondary magnitude − this column (e.g. W1 − G). |
| `lr_q` | `float` | `0.8` | Prior completeness $Q$ of the secondary catalogue, in $[0, 1]$. |
| `ml_color_columns` | `list[str]` | `[]` | Colour/magnitude features for `ml` and `xgb`; without them those matchers fall back to separation-based scoring. |
| `ml_model_path` | `str \| None` | `None` | Save/load path of the Random Forest model (`.joblib`). |
| `xgb_model_path` | `str \| None` | `None` | Save/load path of the XGBoost model (`.joblib`). |
| `macauff_flux_columns` | `list[str]` | `[]` | Multi-band flux columns for `macauff`. |
| `join_type` | `str` | `"1and2"` | `"1and2"`, `"1or2"`, `"all"`, `"all1"`, `"all2"`, `"1not2"`, `"2not1"`. |
| `find` | `str` | `"best"` | `"best"` or `"all"`. |
| `prior_columns` | `list[str]` | `[]` | Nonempty: Bayesian `p_match` scoring with heuristic photometric KDE terms. Columns must exist in both catalogues and hold comparable quantities. |
| `probabilistic` | `bool` | `False` | Positional `p_match` scoring (also settable on `MatchRequest`). |
| `target_epoch` | `float \| None` | `None` | Julian-year epoch for proper-motion propagation. |
| `pm_prior` | `bool` | `False` | Assumed proper-motion drift uncertainty model inspired by Wilson (2023). |
| `pm_prior_magnitude_column` | `str \| None` | `None` | Magnitude column for the Wilson (2023) distance-proxy scaling. |
| `filter_expr` | `str \| None` | `None` | Polars SQL `WHERE` clause applied after scoring and `find` selection; a rejected best match is not replaced. Uses input column names with the result suffixes. Malformed expressions and unknown columns raise `CrossMatchError`. |
| `extra_distance_cols` | `dict[str, float]` | `{}` | Columns and nonnegative weights for $N$-dimensional `find="best"` ranking. Columns must exist on both sides with finite numeric values, or `CrossMatchError` is raised. |
| `batch_size` | `int \| None` | `None` | Positive integer. Accepted and validated; no engine reads it in 0.7.0. |
| `fallback_policy` | `str` | `"warn"` | `"warn"` logs and substitutes a supported engine or matcher; `"error"` raises `CrossMatchError` when the request cannot be honoured as given (CLI `--fallback-policy error`). |

Result columns: every positional match adds `sep_arcsec` (great-circle
separation). Scoring adds `p_match` (`probabilistic` or `prior_columns`),
`lr` and `reliability` (`lr`), `ml_score` (`ml`), `xgb_score` (`xgb`),
`auf_prob` (`auf`) or `macauff_prob` (`macauff`). Right-side columns that
collide with left-side names get the suffix `_2`.

### `SideOverrides`

`xmatcher.SideOverrides` (defined in `xmatcher.request`): per-side overrides.
Every field defaults to `None`, meaning the registry entry or auto-detection
decides.

| Field | Type | Description |
|---|---|---|
| `ra_column`, `dec_column`, `id_column` | `str \| None` | Coordinate and identifier columns. |
| `ra_err_column`, `dec_err_column` | `str \| None` | Per-axis positional uncertainty columns. |
| `corr_column` | `str \| None` | RA/Dec uncertainty correlation column. |
| `astrometric_covariance_columns` | `dict[str, str] \| None` | Astrometric covariance column mapping for epoch propagation. |
| `pos_err_units` | `str \| None` | Unit of the error columns (registry default `"arcsec"`). |
| `default_pos_error_arcsec` | `float \| None` | Positional error to assume when no error column exists. |
| `epoch`, `epoch_column` | `float \| None`, `str \| None` | Reference epoch (Julian year), as a constant or a column. |
| `pm_ra_column`, `pm_dec_column`, `parallax_column`, `radial_velocity_column` | `str \| None` | Astrometry columns for epoch propagation. |
| `columns` | `list[str] \| None` | Column projection. |
| `endpoint` | `str \| None` | TAP endpoint for a bare table id. |
| `frame` | `str \| None` | Declared coordinate frame (registry default `"icrs"`). |

The CLI exposes every key except `columns`, `astrometric_covariance_columns`
and `endpoint` through repeatable `--side1` / `--side2` `KEY=VALUE` pairs; use
`--columns-1` / `--columns-2` and `--endpoint` for those three.

### `CatalogueSource`

`xmatcher.CatalogueSource` (defined in `xmatcher.sources`) is the resolved
input that `resolve_source` returns and that `project_hats`, `export_hats`,
`scan_hats` and the observation functions accept. It carries `name`,
`is_local`, the column and astrometry fields of `SideOverrides` (with
`pos_err_units="arcsec"` and `frame="icrs"` as defaults), the access fields
`access_method`, `archive`, `access_identifier`, `tap_url` and
`default_columns`, a local `path`, and release metadata. Build one with
`CrossMatch.resolve_source` rather than directly.

---

## Execution engines

Matching criteria (`matcher`) and candidate search (`engine`) are separate
choices. Every pairwise engine returns the same schema and, with
`find="best"`, keeps the lowest right row index among equally good
counterparts.

| Engine | Candidate search | Execution | Supported matchers and limits |
|---|---|---|---|
| `auto` | Chosen per request | See below | See below. |
| `fast` | torchsky `SkyIndex` (`match_within`, `match_nearest`): exact declination-zone index | In process; both sides materialized as CPU tensors; torch intra-op threads | All matchers; `extra_distance_cols` ranking. |
| `zone` | Same as `fast` | Same as `fast` | Same as `fast`. Before 0.7.0 this was a HEALPix-sharded `cKDTree`; the name is kept. |
| `torchsky` | `torchsky.catalogs.crossmatch_sky` | In process | `sky` and `skyerr` only; `extra_distance_cols` or another matcher raises `CrossMatchError`. |
| `astropy` | Astropy `match_to_catalog_sky` (`sky`, `find="best"`) or `search_around_sky` | In process | `sky`, `skyerr`, `skyellipse`, and candidates for the scoring matchers. Does not apply `extra_distance_cols` ranking. |
| `stilts` | STILTS `tmatch2` | Java subprocess; needs a working `stilts` command | `sky` and `skyerr`. Other matchers, target-epoch `skyerr`, `extra_distance_cols`, `prior_columns`, probabilistic scoring and `filter_expr` fall back to `fast` (or raise under `fallback_policy="error"`); so does a failed STILTS run. |
| `ray` | Depth-5 HEALPix pixel batches (cdshealpix); each Ray task queries a torchsky `SkyIndex` over its pixel's neighbours | Ray tasks; inputs materialized and results collected on the coordinator | All pairwise matchers and `extra_distance_cols`. Falls back to `fast` when Ray or cdshealpix is missing or Ray fails to start (raises under `fallback_policy="error"`). |
| `ray-union` | Mirrored HATS partitions; per-chunk neighbourhoods with SciPy `cKDTree` | Ray tasks; HATS output | `sky`, `skyerr`, `skyellipse`; full-outer joins only; handles mixed-order and RING inputs and uncertainty/epoch halos. |
| `ray-anchor` | Mirrored HATS partitions; per anchor partition a torchsky `SkyIndex` (`match_nearest` with `k=2`, `count_within`) | One Ray task per anchor partition; resumable; HATS output | `sky` only; best counterpart per survey plus `sep_second`, `n_within`, `crowded`, `pm_warn`, `quality`. |

`engine="auto"` resolves as follows:

- Pairwise matches use `stilts` when a STILTS command is available and
  `fast` otherwise; `skyerr` with `target_epoch` always uses `fast`.
- A local catalogue against a CDS X-Match source, or two TAP sources on the
  same service, is matched server-side when the request is `matcher="sky"`,
  `find="all"`, `join_type="1and2"` with no cone, epoch, scoring, filter,
  extra distance columns or column projection.
- The spill path (`memory_budget_bytes`) uses `fast`.
- `union_match` with all-remote inputs and no cone routes to `ray-union`.

Paths outside the `engine` switch:

- `nway_match` and `crossmatch_multi(matcher="lr")` with $N \ge 3$ run
  torchsky `associate_many` in process.
- `fof_match` with `matcher="sky"` finds edges with a SciPy `cKDTree` (see
  [`fof_match`](#fof_match)).
- `ml` and `xgb` compute a local-density feature with a SciPy `cKDTree` on
  the primary catalogue, after the engine's candidate search.

---

### Local search: torchsky zone index and Astropy

`fast` (and `zone`) and `astropy` are alternative in-process candidate
searches. Their relative speed depends on catalogue sizes, candidate density
and hardware; measure on representative data with
[`scripts/bench_engines.py`](../scripts/bench_engines.py). The zone index, the
search radius per matcher and the `extra_distance_cols` ranking are described
in [algorithms §3](algorithms.md#3-candidate-search).

---

## I/O helpers

```python
from xmatcher import (
    FrameLike,
    scan_frame,
    write_frame,
    is_hats_dir,
    astropy_table_to_polars,
    polars_to_astropy,
)
```

| Name | Signature | Description |
|---|---|---|
| `FrameLike` | type alias | `pl.DataFrame \| pl.LazyFrame`. |
| `scan_frame` | `(path: str \| Path) -> pl.LazyFrame` | Lazy scan of `.parquet`, `.csv`, `.tsv` and `.tab`. `.fits`/`.fit` files are read eagerly with torchfits (Astropy is used when torchfits fails or masked values may have been lost) and returned as a lazy frame. Other suffixes, including HATS directories, raise `InputError`; use `scan_hats` for HATS. |
| `write_frame` | `(frame: FrameLike, output_file: str \| Path, *, ra_column: str = "ra", dec_column: str = "dec", hats_threshold: int = 100_000) -> None` | Format by suffix. `.parquet` and `.csv` use Polars streaming sinks; `.tsv`/`.tab` are tab-separated; `.fits`/`.fit` are collected and written through Astropy; `.hats` writes a HATS tree partitioned on `ra_column`/`dec_column` with at most `hats_threshold` rows per pixel (needs cdshealpix). A `vos:` output is staged locally and uploaded. |
| `is_hats_dir` | `(path: str \| Path) -> bool` | `True` when `path` is a directory containing a `properties` (or `hats.properties`) file. A bare `_metadata` file does not count. |
| `astropy_table_to_polars` | `(table) -> pl.DataFrame` | Converts through Arrow/NumPy buffers. Multidimensional columns are dropped, byte-string columns decoded as UTF-8, masked values become nulls. |
| `polars_to_astropy` | `(frame: FrameLike) -> astropy.table.Table` | Collects a lazy frame; nullable columns become `MaskedColumn`. |

### `xmatcher.convert.convert`

Streaming conversion between FITS, Parquet, HATS and CSV. Not re-exported
from `xmatcher`; import it from `xmatcher.convert`.

```python
from xmatcher.convert import ConvertReport, convert

def convert(
    src: str | Path,
    dst: str | Path,
    *,
    columns: Sequence[str] | None = None,
    hdu: int | str | None = None,
    ra_column: str | None = None,
    dec_column: str | None = None,
    hats_max_rows: int = 1_000_000,
    hats_max_order: int = 10,
    keep_healpix29: bool = False,
    batch_rows: int = 262_144,
    spill_rows: int = 4_000_000,
    overwrite: bool = False,
) -> ConvertReport
```

- Formats come from the paths: `.fits`, `.fit`, `.fts`, `.fits.gz`;
  `.parquet` (a file, or as input a directory of Parquet files); `.csv`,
  `.tsv`, `.tab`; `.hats`, or as input any HATS directory. Every pair works
  in both directions, with memory bounded by `batch_rows`.
- FITS is read with torchfits (nulls become Arrow nulls) and written as one
  binary-table HDU in a single pass; string widths need one extra pass over
  string columns when the source is not FITS.
- A HATS output is written in three passes: count rows per HEALPix cell at
  `hats_max_order`, choose an adaptive tiling that splits cells holding more
  than `hats_max_rows` rows (spilling at most `spill_rows` buffered rows), and
  write each partition sorted by `_healpix_29` with spec-conformant metadata.
  `ra_column`/`dec_column` default to the source's HATS columns or one
  unambiguous common name.
- `columns` selects and orders columns; `hdu` names the FITS table HDU
  (default 1).
- Reading a HATS tree drops `_healpix_29` unless `keep_healpix29=True`;
  writing one adds it. Row order follows the source, except that HATS output
  is ordered spatially.
- The output is written to a temporary path and renamed on success. An
  existing output raises `InputError` unless `overwrite=True`, and
  overwriting the input itself is refused. A missing input or an unknown
  format also raises `InputError`.

`ConvertReport` is a dataclass with `src_format`, `dst_format` (each
`"fits"`, `"parquet"`, `"hats"` or `"csv"`), `rows`, `columns` (`list[str]`)
and `partitions` (HATS output partitions; `0` otherwise).

### `project_hats`

`xmatcher.project_hats` (defined in `xmatcher.hats_project`) writes a narrow
HATS tree with only the requested columns, preserving the partition layout
(including zero-row partitions) and row counts.

```python
def project_hats(
    source: CatalogueSource,
    output: str | Path,
    columns: Sequence[str],
    *,
    cache_root: str | None = None,
    hats_threshold: int = 100_000,
    sync: bool = True,
    rate_limit_rps: float = 1.0,
    resume: bool = False,
    progress_cb: Callable[[str], None] | None = None,
) -> dict[str, Any]
```

- `source` may be a local HATS tree or a remote HATS or TAP catalogue. An
  HTTP(S) HATS tree is projected partition by partition without mirroring
  the full tree. Other remote sources are mirrored first; with `sync=False`
  a cached mirror must already exist.
- Unknown columns, an empty column list, and an output that overlaps its
  input raise `CrossMatchError`. A non-empty output is rejected unless
  `resume=True`, which requires a matching `project.state` fingerprint and
  reuses finished partitions.
- Returns a dict with `rows`, `columns` and `partitions`.

### `export_hats` and `scan_hats`

`xmatcher.export_hats` and `xmatcher.scan_hats` (defined in
`xmatcher.hats_export`) flatten a local HATS tree to one `.parquet` or `.fits`
file, or return a lazy Polars scan over its partitions.

```python
def scan_hats(
    src: CatalogueSource | str | Path,
    *,
    columns: Sequence[str] | None = None,
) -> pl.LazyFrame

def export_hats(
    src: CatalogueSource | str | Path,
    output: str | Path,
    *,
    columns: Sequence[str] | None = None,
    keep_healpix29: bool = False,
) -> dict[str, Any]
```

- `scan_hats` pushes `columns` into the Parquet scan. Collecting the result
  may still need memory proportional to the selected data.
- `export_hats` writes Parquet through `write_frame`'s streaming sink and
  FITS through `xmatcher.convert.convert`. Other output suffixes raise
  `InputError`.
- Without `columns`, `_healpix_29` is dropped unless `keep_healpix29=True`;
  with `columns`, the list decides.
- Returns a dict with `rows`, `columns`, `partitions` and `output`.
- Remote HATS URIs must be mirrored first. Keep survey-scale trees
  partitioned when a single file is not required.

---

## Mirroring and caching (`xmatcher.mirror`)

The mirror backs offline queries, `xmatcher sync`, and the `ray-union` and
`ray-anchor` routes.

```python
from xmatcher.mirror import TokenBucket, ensure_mirrored, sync_catalogue
from xmatcher.storage import open_storage
```

- `sync_catalogue(src, *, cache_root=None, ...) -> SyncStats` mirrors a
  `CatalogueSource`; `ensure_mirrored(src, ...) -> CatalogueSource` mirrors
  when needed and returns a source pointing at the local copy.
  `open_storage(root)` returns a storage backend for a POSIX path or a `vos:`
  URI.
- **Cache root.** `XMATCHER_CACHE_ROOT` if set; on AstroAI/CANFAR sessions
  `/arc/projects/hats`; otherwise `~/.cache/xmatcher`. A catalogue is stored
  under `<cache>/<name>/<version>/`. The CANFAR working and output directory
  is `/arc/projects/hats/xmatcher`.
- **Remote HATS.** Trees over HTTP(S), `vos:`, `s3://` or `hf://` are
  mirrored into the cache. The manifest records each file's size; a file whose
  advertised size is unchanged is skipped, so use `--force` to pick up
  upstream edits that keep the same size.
- **Anonymous S3.** `xmatcher.s3.S3Remote` uses ListObjectsV2 and plain
  GET/HEAD over HTTPS: a public bucket tree is listed (one paginated walk,
  sizes included) and staged without `boto3` or credentials. It is
  read-only, so mirrors go to the cache, never back to the bucket.
  `XMATCHER_S3_ENDPOINT` selects an S3-compatible endpoint (path-style URLs).
- **Anonymous Hugging Face.** `xmatcher.hf.HFRemote` walks the Hub tree API
  (`Link`-cursor pagination, sizes included) and streams
  `/resolve/<revision>/<path>` downloads, so an `hf://datasets/<owner>/<name>`
  tree mirrors without `huggingface_hub`, a token or a clone. It is
  read-only; a gated or private dataset is reported as denied.
  `XMATCHER_HF_ENDPOINT` selects a Hub-compatible endpoint.
- **TAP tables.** Downloaded in keyset pages with a resume manifest
  (`sync.json`); a stable, unique, non-null key is required. Re-sync uses
  `COUNT(*)` probes, which detect row-count changes but not same-count value
  edits (use `--force`), and fetches appended higher keys from the tail.
- **Rate limiting.** `TokenBucket(rps, burst=10)` is a thread-safe per-host
  token bucket (`rps=0` disables it). Mirror HTTP requests retry 429, 502,
  503 and 504 responses, truncated bodies and transient network errors,
  waiting `Retry-After` (or 1 s) plus exponential backoff.

---

## Association contract & provenance graph (`xmatcher.association`)

`xmatcher.association` implements the content-addressed association contract
`xmatch.association.v1`: candidate associations, component graphs and
identity continuity between releases. The normative schema is in
[association-v1.md](association-v1.md).

```python
from xmatcher.association import (
    AssociationComponent,
    AssociationComponentMember,
    AssociationDecision,
    AssociationMemberEquivalence,
    AssociationProvenance,
    AssociationRecord,
    ScoreSemantics,
    iter_association_release,
    verify_association_component_release,
    verify_association_release,
    write_association_component_release,
    write_association_member_equivalence_release,
    write_association_release,
)
```

### Data models

#### `AssociationRecord`

A frozen dataclass for one evaluated candidate pair. Construction validates
every field and raises `ValueError` on violations.

| Field | Meaning |
|---|---|
| `evidence_id`, `source_id`, `candidate_id` | Opaque, release-scoped identifiers. |
| `method`, `method_version` | The method that produced the record. |
| `separation_arcsec` | Non-negative great-circle separation after epoch propagation. |
| `evaluation_epoch_jyear` | Positive Julian-year epoch, or `None` together with the `epoch_unknown` flag. |
| `decision` | `AssociationDecision`: `candidate`, `selected` or `rejected`. |
| `score_name`, `score_value` | Score label and finite value. |
| `score_semantics` | `ScoreSemantics`: `ranking_score` (any finite value, not a probability), `assumed_prior_posterior` (in $[0, 1]$, uncalibrated), or `calibrated_probability` (in $[0, 1]$; requires `calibration_id`). |
| `calibration_id` | Required for `calibrated_probability`, otherwise `None`. |
| `flags` | Unique free-text flags, stored sorted. |
| `provenance` | `AssociationProvenance(input_release_ids, software_version, parameters_sha256)`. |

`association_id` is a derived property:
`xmatch.association.v1:<sha256>` over the schema version, `evidence_id`,
`source_id`, `candidate_id`, `method`, `method_version`,
`evaluation_epoch_jyear`, and the provenance fields.

#### `AssociationComponent` and `AssociationComponentMember`

Sidecar graph of multi-catalogue components.

- `AssociationComponent(association_release_id, members, parent_component_ids=())`:
  the members of one component and the parent-release components it
  continues, which records splits and merges across releases.
- `AssociationComponentMember(input_release_id, member_id)`: a source in one
  input catalogue release (for example `input_release_id="catalog:gaia_dr3:v1"`).
- `AssociationMemberEquivalence(parent_association_release_id, current_association_release_id, parent_member, current_member)`:
  maps a member between two association releases.

### Release files

```python
# 1. Write an association release (records ordered by association_id)
manifest = write_association_release(
    sorted_records,
    "releases/cosmos_v1",
    provenance=provenance,
)

# 2. Verify checksums and ordering; fails closed
verified_manifest = verify_association_release("releases/cosmos_v1")

# 3. Stream records with bounded memory
for record in iter_association_release("releases/cosmos_v1"):
    process(record)

# 4. Write the component graph sidecar
comp_manifest = write_association_component_release(
    components,
    association_release_directory="releases/cosmos_v1",
    directory="components/cosmos_v1",
)
```

The module also provides `load_*_manifest`, `iter_*` and `verify_*`
functions for component, component-delta and member-equivalence releases.

---

## Exceptions

`xmatcher` raises these public exceptions; all derive from `CrossMatchError`:

```text
CrossMatchError     (base class; matching and request errors)
├── ConfigError     (missing, malformed or invalid registry YAML)
├── InputError      (unreadable or unsupported input or output)
├── TapError        (failed TAP query or job)
└── StiltsError     (STILTS subprocess failure or missing STILTS)
```

`CrossMatchError.source`, when set, holds the user-supplied identifier that
caused the error; `InputError(message, *, source=None)` sets it. Two
validation errors are not `CrossMatchError` subclasses: invalid
`MatchSpec`/`MatchRequest` field values raise `ValueError`, and unknown
`**params` keywords raise `TypeError`.

---

## Catalogue and model-training products

`xmatcher.observations` supplies `source_inventory`, `namespaced_source_id`,
`normalize_photometry`, `normalize_property_evidence`,
`required_observation_columns`, `validate_source_inventory`,
`write_observation_release` and `verify_observation_release`.
`xmatcher.candidates` supplies `write_candidate_release`,
`verify_candidate_release` and `normalize_candidate_hypotheses`.
All of them are also exported from `xmatcher`. See
[observation mappings](observations.md) and the
[catalogue-to-model pipeline](catalogue-pipeline.md) for contracts and
examples.
