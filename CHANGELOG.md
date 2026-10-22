# Changelog

All notable changes to `xmatcher` are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [0.6.0] — 2026-10-22

The `v0.6.0` tag is the GitHub release. See the
[release notes](docs/release-0.6.0.md).

### Added

- `engine="ray-anchor"` (`CrossMatch.anchor_match`, CLI
  `--engine ray-anchor`): a Gaia-anchored master HATS catalogue with one row
  per anchor source and the best counterpart from every survey. Output adds
  per-survey `sep_arcsec`, `sep_second`, `n_within`, `crowded`, `pm_warn`, and
  `quality` fields, preserves all anchor rows, and resumes per anchor
  partition. Currently supports `matcher="sky"`; see
  [docs/ray-anchor.md](docs/ray-anchor.md).
- `xmatcher project-hats` and `xmatcher.hats_project.project_hats`: write a
  narrow HATS tree with selected columns while preserving the partition layout
  and row counts. HTTPS trees are streamed partition by partition (up to eight
  `?columns=` names per request on CDS), so a wide catalogue is not mirrored
  in full first. `--resume` completes an interrupted projection from its
  `project.state` fingerprint and skips finished partitions.
- Pinned HATS catalogue aliases for products not in the IVOA registry:
  `gaia_hats`, `sdss_hats` (CDS `sdss_dr16`), `catwise_hats`, `vhs_hats`,
  `ps1_hats`, `galex_hats`, `splus_hats`, plus local CANFAR aliases
  `uhs_hats`, `splus5_hats`, `xp_hats`, and `dp2` driven by environment
  variables. `gaia_lsdb_hats` pins the 153-column LSDB Gaia DR3 tree as the
  compact anchor source.
- `scripts/run_gaia_master.sh` wraps the public-survey anchor run with local
  CANFAR products, preflight/dry-run modes, and validation;
  `scripts/validate_anchor_output.py` streams a master and reports row counts,
  null/duplicate IDs, and per-survey ambiguity coverage. The data inventory,
  quota model, and staging order are in
  [docs/gaia-master-data.md](docs/gaia-master-data.md).
- `docs/ray-anchor.md` documents the schema, epoch/PM policy, projection, and
  CANFAR local-product setup.
- Explicit catalogue identifiers: `vizier:<id>`, `tap:<endpoint>/<table>`, and
  `hats:<url>`, documented in [docs/naming.md](docs/naming.md). Configured
  tables resolve before any TAP_SCHEMA probe.
- Repeatable `--side1` / `--side2 KEY=VALUE` CLI overrides, so `skyerr`,
  `skyellipse`, target-epoch, and probabilistic matching work on plain local
  files without editing `xmatcher.yaml`.
- `--fallback-policy {warn,error}` on the CLI, matching
  `MatchSpec.fallback_policy`.
- `xmatcher list --aliases` prints alias-to-catalogue mappings, and
  `xmatcher describe` shows the alias used and the catalogue's release.
- `docs/naming.md` and `docs/README.md`.
- `scripts/canfar_headless_run.py` and `scripts/canfar_bootstrap.py` run the
  scenario matrix (aliases, explicit identifiers, matchers, multi/union/fof,
  discovery, HATS, Ray) in CANFAR headless sessions against live TAP services.
- `hats:<short_name>` resolves through the IVOA HATS registry instead of
  failing as an unimplemented namespace. The lookup is cached for seven days
  under `<cache>/registry/hats_registry.json`, reused when the registry is
  unreachable, refuses ambiguous prefixes by listing them, and names any
  registration it cannot read instead of substituting a different copy.
  HTTPS, VOSpace, `s3://` and `hf://` trees all resolve. `xmatcher list --hats`
  prints the live inventory.
- Anonymous `s3://` HATS support: S3 trees are listed with one paginated
  ListObjectsV2 walk (object sizes included) and streamed over HTTPS, so the
  STScI/IRSA/MAST cloud catalogues (`hats:gaia_dr3_stsci`,
  `hats:euclid_q1_ipac`, the Pan-STARRS and ZTF DR23 copies) mirror like
  HTTPS trees. Read-only, with no `boto3` or credential chain;
  `XMATCHER_S3_ENDPOINT` selects an S3-compatible endpoint. `s3://` collection
  roots descend through `collection.properties` to their primary table, and
  CSV partition tables without `file_loc`/`Nfiles` are resolved by deriving
  pixel paths — including `Npix=N/part*.parquet` pixel directories.
- Anonymous Hugging Face HATS support: `hf://datasets/<owner>/<name>` trees are
  read through the Hub's public API — one `Link`-cursor tree walk with file
  sizes, and `/resolve` downloads — so the 41 `mmu_*` Multimodal Universe and
  community HATS datasets (`hats:mmu_jwst_gds`, `hats:mmu_gaia_gaia`,
  `hats:tevcat_hats`) mirror like HTTPS trees. Read-only, with no
  `huggingface_hub` dependency, token or repository clone;
  `XMATCHER_HF_ENDPOINT` selects a Hub-compatible endpoint. Every registry
  entry is now readable by the mirror.
- `docs/hats-sources.md` lists the verified public HATS trees (LSDB, IRSA,
  MAST) with row counts, sizes, the collection-root form, and access notes.
- `tests/test_catalogue_live.py`: opt-in (`XMATCHER_LIVE=1`) check that every
  bundled `access_identifier` is a real table and that its declared RA/Dec,
  error, motion and `default_columns` exist in the live `TAP_SCHEMA` — the
  guard for the `galex` (`II/335` vs `II/335/galex_ais`) class of drift.
- CANFAR scenarios for engine-independent `skyerr` pair sets, join-type
  composition (`1and2`/`1or2`/`all1`/`all2`/anti joins, `find=all`), RA = 0/360
  seam handling, a distributed union checked against the sequential oracle, and
  a live HATS source check (registry name, collection descent, mirror, match).

### Fixed

- `project-hats` copied a source `properties` file verbatim, so a projection
  that dropped the coordinate columns still advertised `hats_col_ra`/
  `hats_col_dec`; the keys are now removed when their column is not in the
  projection. It also refused to write into an existing non-empty output, kept
  zero-row source partitions so the layout really is preserved, and made
  `--no-sync` on a remote source name the missing mirror instead of reporting
  “no HATS partitions”.
- `ray-anchor` wrote `hats_epoch=J2000` for any target epoch other than 2016.0;
  the requested epoch (or the anchor epoch when none is requested) is recorded.
  An anchor with no resolvable identifier is now an error instead of a master
  tree without `source_id`.
- LSDB/IRSA HATS trees could not be mirrored over HTTP: their Apache-style
  listings start with a `/parent/dir/` row, which the walker descended into,
  404'd, and then declared the whole catalogue incomplete. Absolute and
  external hrefs are now ignored by the listing and by both walkers.
- A HATS *collection* root (`https://data.lsdb.io/hats/gaia_dr3/`) resolves to
  its single primary tree, skipping HEALPix margin caches (`*_5arcs`,
  `*_10arcs`) and id indexes (`*_index`). A collection holding several real
  catalogues is left alone and reported, instead of guessing.
- `match` with a remote HATS catalogue raised `HATS path does not exist:
  https:/…` (the URL collapsed into a path). It now says to run `xmatcher sync`
  or use `engine='ray-union'`, which mirrors on demand.
- `engine='ray-union'` checks the rows a block will *emit* against
  `--chunk-memory-gb` before materialising them: a field much denser than the
  search radius used to consume the worker's memory and die (the pool guard
  only ever measured input rows). The error names the chunk, the row count and
  the knobs that fix it.
- CLI: an input `OSError` (missing directory, permission denied, unreadable
  parquet) is reported as `Error: …` instead of `Unexpected error` with a
  traceback, and a bare `xmatcher` prints a usage synopsis.
- `engine='stilts'` with `matcher='skyerr'` now caps each per-row error at
  `radius_arcsec / max_error`. STILTS bins and ranks candidates in its
  error-normalised space, so a corrupted error (some PS1 rows declare ~1000
  arcsec) made a far-away counterpart win `best`, only to be dropped by the
  radius filter — the live GAIA × PS1 cone returned 10 rows where the other
  engines returned 34. Capping keeps every pair inside the radius reachable and
  makes `skyerr` pair sets engine-independent again.
- `xmatcher match --help` preserves the example block and description layout;
  defaults are only shown when meaningful.
- Documented `--join` (the previous `--join-type` spelling in the user guide was
  not a real flag), association writer argument names, and the `mirror` exports.
- Output paths that cannot be created now raise an actionable `InputError`
  instead of logging a traceback.
- Removed the unsupported S3 storage-connector claim from the README.
- `galex` resolved to the VizieR catalogue parent `II/335`; it now names the
  `II/335/galex_ais` table.
- The GALEX primary key and two flux columns were misspelled (`obijd`,
  `Ffux`, `Nfux`), so every GALEX TAP query failed in the service. They are now
  `objid`, `Fflux`, and `Nflux`.
- Pre-computed pair tables (`nsc_x_gaia`, `des_x_gaia`, `decals_x_gaia`,
  `allwise_x_gaia`, `ukidss_x_gaia`) declare `default_pos_error_arcsec: 0.75`;
  without per-pair errors, positional-error matching treated them as having no
  uncertainty.
- The CDS Gaia DR3 correlation column is `RADEcor`, not `RADECor`, so
  `--matcher skyerr` against `gaia` failed in the TAP service.
- `--side1` / `--side2` overrides such as `ra_column=ra` collided with the
  dedicated `--ra-column-1` flags (`TypeError: dict() got multiple values`);
  side overrides now apply last and win.
- Re-matching an earlier output that already carries `ra_2` (for example a
  HATS directory) raised `DuplicateError`; colliding right-hand columns now
  take the next free suffix (`ra_2_2`).
- `xmatcher search` retries a lowercased name filter when the server-side TAP
  `LIKE` match finds nothing, so `search unWISE` finds `unwise_dr1` tables.
- `skyerr` ignored `radius_arcsec`: one corrupted per-row error (PS1 DR1 has
  `e_RAJ2000` values of hundreds of arcseconds) widened the acceptance circle
  to arcminutes. The radius is now an upper bound in every engine, and only
  the N-sigma test can narrow it.
- Alias resolution follows alias chains, so a user overlay written before a
  catalogue rename keeps working. Alias cycles and aliases that shadow a
  catalogue name are now `ConfigError`s instead of silent misresolution.

### Changed

- Catalogue keys now follow `<survey>[_<release>][_<product>][_<provider>]`:
  `gaia_cds` → `gaia_dr3_cds`, `des` → `des_dr2`, `nsc` → `nsc_dr2`,
  `ps1` → `ps1_dr1`, `twomass` → `twomass_psc`, `unwise` → `unwise_dr1`,
  `catwise` → `catwise_2020`, `desils` → `legacy_dr10`,
  `decals_objects` → `decals_dr10_object`, `galex` → `galex_ais`,
  `vhs` → `vhs_dr5`, `gaia_noao` → `gaia_noirlab`, `ukidss` → `ukidss_las`,
  `erosita` → `erosita_erass1`, and `xmm` → `xmm_dr13`. Every former key stays
  as an alias, and short names (`gaia`, `des`, `ps1`, …) are unchanged. `decals`
  still resolves to the tractor table. Mirrors sync into a new cache directory
  named after the canonical key; see [docs/naming.md](docs/naming.md).
- The 2MASS entry's `release` is now `allsky` (the PSC all-sky release) rather
  than the product token `psc`.

## [0.5.1] — 2026-10-08

Published on PyPI as `xmatcher`. The `v0.5.1` tag is the GitHub release.

### Changed

- **Breaking package and command rename**: Python imports and the CLI move from
  `xmatch` to `xmatcher`.
- **Breaking configuration rename**: the bundled and user configuration file
  becomes `xmatcher.yaml`; environment variables move from the `XMATCH_` prefix
  to `XMATCHER_`.
- **Changed local paths**: the default cache moves to `~/.cache/xmatcher`, and
  the documented CANFAR output root is `/arc/projects/hats/xmatcher`.
- The persisted `xmatch.*` association schema identifiers remain unchanged.

### Fixed

- FITS writes preserve explicit masks, including the distinction between empty
  strings and nulls. Reads fall back to Astropy when the fast reader could conceal masked
  values. CDS XMatch now uses a collision-free local row key, preserving input
  columns with the previous internal key name.
- Catalogue names resolve case-insensitively to their configured spelling;
  configurations with names that differ only by case are rejected. CLI table
  discovery and schema queries use configured archive authentication, and
  invalid CLI configuration is reported as a command error.
- HTTP HATS mirroring distinguishes complete listings from failed lookups,
  includes partition metadata, and removes stale cached files only after a
  successful refresh. A stale file that cannot be removed fails the sync and
  keeps its manifest entry, so the next sync retries the removal.
- Astrometric error floors and drift inflation preserve measured cross
  covariance for ellipse matching, and singular covariance pairs are rejected.
  Astropy uses the shared ellipse filter; separation-based matcher fallbacks
  retain every candidate for `find="all"`.
- Post-match filters apply the same row selection to matcher scores and respect
  the requested right-column suffix. Candidate normalization checks pair
  endpoint namespaces against the inventory, rejects contradictory target
  namespace evidence, and allows an explicitly named empty target population.
  Candidate-release setup failures also clean up their lock so publication can
  be retried.
- HTTP HATS refreshes publish metadata after successful data transfers and use
  a one-byte range probe when HEAD is unsupported. HTML listing fallback retains
  CSV partition metadata and empty-catalogue schemas. VOS replica replacement
  preserves nested paths and existing files when backup reads fail. TAP row
  limits include short final pages.
- Native HATS reads and distributed unions retain the schema of valid empty
  catalogues while rejecting missing partitions from nonempty catalogues.
  CANFAR scaling validation requires the observed worker count to match the
  requested count.

## [0.5.0] — 2026-10-07

Available from the public [GitHub release at tag `v0.5.0`](https://github.com/astroai/xmatch/releases/tag/v0.5.0).
No PyPI package is published because another project owns the `xmatch` name.

### Added

- **Source-Preserving Observation & Candidate Releases** (`observations.py`, `candidates.py`):
  - Publish source inventories with normalized photometry and property-evidence ledgers in immutable, checksummed Parquet releases (`write_observation_release`, `verify_observation_release`).
  - Publish scoped sparse candidate-pair releases (`write_candidate_release`, `verify_candidate_release`) and normalize caller-supplied per-target hypothesis weights with an explicit no-match alternative (`normalize_candidate_hypotheses`).
- **Versioned Association Contract** (`association.py`):
  - Deterministic `xmatch.association.v1` pair records, atomic release directories, component sidecar releases (`xmatch.association.component.release.v1`), component delta classifications (`created`, `continued`, `merged`, `split`, `retired`), and bijective member equivalence crosswalks.
- **HATS Mirroring & Distributed Full-Sky Union** (`mirror.py`, `ray_union.py`, `storage.py`):
  - `xmatch sync` CLI command and `sync_catalogue` / `ensure_mirrored` APIs to mirror remote TAP and HATS catalogues (over HTTP or CANFAR `vos:`) into a local HATS cache with keyset paging, token-bucket rate limiting, endpoint failover, and resumable checkpoints.
  - `engine="ray-union"`: distributed $N$-survey full-outer join over mirrored HATS inputs on Ray, producing partitioned HATS trees (`Norder=*/Dir=*/Npix=*.parquet`) with chunk-level resume state.
  - `ray_engine.py`: parallel HEALPix pixel-batch cone crossmatching (`engine="ray"`).
- **Astrometric Kinematics & Probabilistic Matchers** (`matchers.py`, `out_of_core.py`, `bayes.py`):
  - Six-dimensional epoch propagation (`target_epoch`) and 5-parameter astrometric covariance transport through motion Jacobians for both `skyellipse` and `skyerr` (eager and out-of-core spill paths).
  - Wilson (2023)-inspired proper-motion drift uncertainty model (`pm_prior=True`) with per-row error quadrature inflation and optional magnitude refinement.
  - Sutherland & Saunders (1992) Likelihood Ratio (`matcher="lr"`), Random Forest (`matcher="ml"`), XGBoost/LightGBM (`matcher="xgb"`), lightweight AUF-inspired empirical separation/background scoring (`matcher="auf"`), and a flux-augmented heuristic (`matcher="macauff"`); the latter two are not full reproductions of the published methods.
  - Multi-condition SQL post-filtering (`MatchSpec.filter_expr`), feature-weighted ranking of spatial candidates (`MatchSpec.extra_distance_cols`), and HEALPix pixel-group batching (`MatchSpec.batch_size`; this is not a general process-memory bound).
- **Formats & Catalogues** (`io_utils.py`, `xmatch.yaml`):
  - Native streaming `.tsv` and `.tab` reads and writes via Polars.
  - Added NOIRLab Data Lab catalogues (`nsc`, `des`, `decals`, `smash`, `unwise`, `allwise_dl`, and pre-computed `*_x_gaia` crossmatch tables) and COSMOS (`cosmos-web`, `zcosmos`).
  - Terminal progress indicator for remote TAP and VizieR downloads.

### Fixed

- Distributed HATS union now keeps output partitions spatially coherent across mixed input orders and RING/NESTED layouts, and reports non-null union routing coordinates. Tuple caps select the lowest-separation results from the complete candidate product; planner scans account for declared positional uncertainty and epoch motion.
- Friends-of-Friends matching searches cross-catalogue secondary links while retaining the primary-anchored component output policy.
- Probabilistic scores require declared positional errors or explicit error floors; the documentation states the equal-prior and isotropic-score assumptions. Remote `skyerr`, `skyellipse`, and `target_epoch` queries require an explicit search cone when service-side uncertainty or motion margins cannot be bounded.
- Invalid `filter_expr` SQL and missing, nonnumeric, or nonfinite declared ranking features now raise `CrossMatchError` instead of silently retaining rows or dropping/imputing features.
- ML synthetic negative sampling uses a local fixed seed, making repeated inputs reproducible without advancing the caller's NumPy random state.
- STILTS per-primary `find="best"` now uses `best1`, avoiding the symmetric one-to-one selection performed by STILTS `best`.
- An unreachable explicit Ray address no longer silently starts a local cluster under strict fallback policy, and the caller's address setting is preserved. Memory and storage budgets documented in GiB now use binary GiB.

### Changed

- Updated the Python requirement to $\ge 3.13$ and modernized type annotations to PEP 585 / PEP 604 built-ins. The lockfile targets Python 3.13. A targeted Python 3.14.8 / Polars 2 compatibility run passed 206 tests and skipped 24; it was not the complete release gate on that stack.
- Streamlined core runtime dependencies to `numpy`, `polars`, `pyarrow`, `astropy`, `scipy`, `pyyaml`, and `pyvo`, with direct PyArrow/NumPy `Table`/`MaskedColumn` conversions and optional `xmatch[cds]` extra for `astroquery`.
- Consolidated HATS reading and writing on the native HATS engine (`hats` + `cdshealpix`). Ray-union uses compressed interval planning and measured per-source halos for adaptive/mixed-order and RING inputs and uncertainty- or epoch-adjusted searches; it may scan additional partitions. The pairwise `hats_native` fast path remains fixed-radius, uniform-order NESTED and globally materializes other layouts or error/epoch-adjusted inputs.
- Removed the unusable `torchsky` package extra because that distribution is not published on PyPI. The tensor engine requires a separately installed compatible Torchsky checkout; the documented integration was tested with the sibling 0.4 development source installed editable and Torchfits 1.0.0 from PyPI.

---

## [0.4.0] — 2026-06-15

### Added

- Typed request API (`MatchRequest`, `SideOverrides`, `CrossMatch.crossmatch_request`).
- Multi-catalogue sequential crossmatching (`CrossMatch.crossmatch_multi`) and CLI support for $N \ge 3$ catalogues.
- Remote archive discovery module (`xmatch.discovery`) and CLI subcommands (`search`, `discover`, `adopt`, `list`, `describe`).
- Lazy bounding-cone estimation (`astro_utils.sky_extent_from_frame`) and streaming Parquet/CSV output sinks (`io_utils.write_frame`).

### Changed

- Switched Astropy–Polars table conversions (`astropy_table_to_polars`, `polars_to_astropy`) to Apache Arrow column buffers.
- Quoted ADQL identifiers in remote TAP queries to handle reserved words and special characters safely.
