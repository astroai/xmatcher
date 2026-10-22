# xmatcher 0.6.0

Release notes for the 0.6.0 source tree, prepared 2026-10-22 for tag `v0.6.0`.
This release makes remote catalogues addressable by name, adds a Gaia-anchored
master catalogue built from streamed remote HATS trees, adds anonymous S3 and
Hugging Face HATS reads, and fixes a class of positional-error matches that
silently widened past the requested radius.

## Highlights

### Catalogues have names

`hats:<short_name>` now resolves through the IVOA HATS registry, cached for
seven days under `<cache>/registry/hats_registry.json` and reused when the
registry is unreachable. `hats:<url>` still accepts any tree, and a collection
root (an LSDB catalogue directory, an S3 prefix, a Hub dataset) is descended to
its primary tree — `hats:gaia_dr3` → `…/hats/gaia_dr3/gaia` — while a
collection holding several real catalogues is refused rather than guessed.
`xmatcher list --hats` prints the live inventory; every one of the 90
registrations is readable by the mirror.

Explicit identifiers (`vizier:<id>`, `tap:<endpoint>/<table>`, `hats:<url>`)
resolve before any `TAP_SCHEMA` probe, and the naming grammar
(`<survey>[_<release>][_<product>][_<provider>]`) is documented in
[docs/naming.md](naming.md) with every former key kept as an alias.

### One row per Gaia source

`engine="ray-anchor"` (also `CrossMatch.anchor_match`) builds the master the
other way round: the first catalogue is the anchor, normally Gaia DR3, and it
decides the rows. Every later catalogue is matched independently and
contributes its best counterpart plus `sep_arcsec`, `sep_second`, `n_within`,
`crowded`, `pm_warn`, and a `quality` label. An anchor source with no
counterpart keeps its row with null survey columns, and the anchor ID becomes
`source_id` (`--anchor-id-name` renames it).

The run is a HATS pipeline: inputs are mirrored into the cache on demand (or
read from a local tree), each anchor partition is one Ray task, and a finished
partition is skipped on a rerun. The fingerprint in `resume.state` covers the
inputs, selected columns, radius, target epoch, and anchor ID name; changing
any of them into the same output wipes the old dataset rather than mixing two
plans. `--target-epoch` propagates both sides where epoch and proper motion
exist, `pm_warn` flags an anchor whose motion exceeds half the radius over the
baseline, and `hats_epoch` records the epoch of the written coordinates.

`xmatcher project-hats <source> <out> --columns …` writes the narrow HATS tree
the anchor run wants without mirroring the wide one first: it keeps the
partition layout and row counts, streams an HTTPS tree partition by partition
(CDS accepts up to eight column names per request), and refuses an output that
overlaps its input or is already populated. Pinned aliases cover the CDS HATS
products that are absent from the IVOA registry (`gaia_hats`, `sdss_hats`,
`catwise_hats`, `vhs_hats`, `ps1_hats`, `galex_hats`, `splus_hats`), and
`gaia_lsdb_hats` pins the 153-column LSDB Gaia DR3 tree as a smaller anchor.
[ray-anchor.md](ray-anchor.md) documents the schema and limits;
[gaia-master-data.md](gaia-master-data.md) is the data-side runbook, with
`scripts/run_gaia_master.sh` and `scripts/validate_anchor_output.py` for a
large run. Only `matcher="sky"` and the best counterpart are supported;
`ray-union` remains the engine for `skyerr`/`skyellipse` and for
secondary-only sources.

### HATS over HTTPS, S3 and Hugging Face

A HATS tree can now be mirrored from three more places, all anonymously:

- **HTTPS** listings, including Apache-style indexes that begin with a
  `/parent/dir/` row (which used to abort the walk and made every LSDB tree
  look incomplete).
- **S3** prefixes, read with one paginated `ListObjectsV2` walk (sizes
  included) and plain `GET`s, so `s3://stpubdata/…` and IRSA's buckets sync
  without `boto3`, credentials or an AWS configuration.
- **Hugging Face** datasets, read through the Hub's public tree API and
  `/resolve` downloads, so the 41 `mmu_*` Multimodal Universe catalogues sync
  without `huggingface_hub`, a token or a repository clone.

Public sources only: a bucket that needs signed or requester-pays access, and a
gated or private dataset, are reported as denied instead of being treated as an
empty catalogue. Mirrors are always written to the local cache (or VOSpace),
never back to the source. [docs/hats-sources.md](hats-sources.md) lists the
verified trees with row counts, and the CSV partition tables and
`Npix=N/part*.parquet` pixel directories those exports use are accepted
without a hand-written `partition_info`.

### Positional-error matching respects the requested radius

`skyerr` used a per-row N-sigma criterion with no upper bound, so one corrupted
uncertainty (PS1 DR1's `e_RAJ2000` reaches 1655″) accepted unrelated
counterparts arcminutes away. The requested radius is now an upper bound in
every engine — numpy, Astropy, STILTS, Torchsky, the zoned reader and the
spill path — and only the N-sigma test narrows it further. STILTS additionally
caps the error it feeds its ranking, so a far-away counterpart can no longer
evict the real match: the live GAIA × PS1 cone that returned 10 rows now
returns the same 34 as the other engines.

The release also corrects bundled column names that made live queries fail
(NSC, unWISE, AllWISE, SMASH, GLIMPSE, NVSS, GALEX, CDS Gaia) and gives the
pre-computed pair tables a 0.75″ positional floor.

### Failures read like answers, not tracebacks

An input `OSError` (missing directory, permission denied, unreadable Parquet) is
reported as `Error: …`; a bare `xmatcher` prints the command shape; a remote
HATS catalogue passed to `match` names the `xmatcher sync` command instead of
failing on a mangled path; and `ray-union` checks the rows a block will *emit*
against `--chunk-memory-gb` before materialising them, naming the chunk, the
row count and the knobs that fix it.

## Upgrade notes

- Catalogue keys follow the documented grammar. Every former key remains an
  alias, so existing commands keep working; mirrors sync into a new cache
  directory named after the canonical key, so the first re-sync after upgrading
  re-downloads.
- `xmatcher list` lists catalogues; aliases are behind `--aliases`, and the
  registry inventory behind `--hats`.
- `hats:<name>` performs a registry lookup on first use (network); the snapshot
  is cached for seven days and reused offline.
- `project-hats` writes a new directory by default: an existing non-empty
  output is refused, because a stale partition from an earlier projection would
  otherwise survive. `--resume` continues an interrupted projection when its
  `project.state` fingerprint matches (same source and columns); a mismatch is
  reported rather than merged.
- `ray-anchor` needs an identifier column on the anchor (`id_column`, or a
  recognised name such as `source_id`/`Source` in the HATS schema); a bare
  `hats:<url>` anchor with no id is rejected. The output is a new master tree,
  never an in-place edit of an input.
- The 0.5.1 `xmatch` → `xmatcher` rename (package, command, `xmatcher.yaml`,
  `XMATCHER_*` environment variables) is unchanged; see the
  [migration notes](release-0.5.1.md).

## Verification

- Offline suite: **901 passed, 51 skipped, 14 warnings** in 246.97 s, including
  new end-to-end mirrors of a HATS tree served over S3 and over a Hub-shaped
  API, the registry resolution and CLI contracts, and the documentation drift
  guards. The anchor engine is checked against an all-pairs brute-force
  reference over a multi-partition tree (best/second separation, counts,
  `crowded`, `quality`, and the chosen counterpart), together with resume,
  fingerprint invalidation, target-epoch coordinates, and local-vs-HTTPS
  projection parity. `ruff check .`, `ruff format --check .`, `mypy
  src/xmatcher tests scripts` (88 files) and `python -m compileall` are clean.
- Live HATS registry: all 90 registrations resolve; all 41 `hf://` entries
  resolved name → collection → tree → `properties` in 58 s with no failures.
- Live pinned trees: the seven CDS HATS endpoints answer their `properties`
  with the row counts in [hats-sources.md](hats-sources.md), and
  `gaia_lsdb_hats` exposes all 153 columns (every `default_columns` entry,
  plus the ID, coordinate, epoch, proper-motion, parallax and radial-velocity
  columns) at 1,812,731,847 rows.
- Live mirroring: `hats:mmu_cfa_seccsn` over Hugging Face produced 66 files
  (362 KB) and read back 64 rows; the IRSA Euclid Q1 S3 tree listed 123 objects
  (114 pixel parts, 344.6 GiB) and its smallest partition opened with 17 rows
  and 1591 columns; the LSDB TNS tree for 2026-10-22 mirrored over HTTPS again
  after the reader refactor (17 files, 21.8 MB, 210,788 rows read back).
- Live TAP schema: of the catalogues whose columns changed in this release,
  `allwise` (and its pair table), `des_dr2`, `nsc_dr2`, `smash` and `unwise_dr1`
  validated against `TAP_SCHEMA`. `galex_ais`, `glimpse`, `nvss` and
  `gaia_dr3_cds` could not be validated: VizieR's TAP service answered 503 or
  reset the connection after three attempts during the run, and the opt-in
  check skips rather than fails on an archive outage. Each query is now bounded
  by a socket timeout, so a stalled service cannot hang the run.
- CANFAR: the last remote `hats-sources` session passed all five checks
  (registry listing, collection descent, dated-collection refusal, anonymous S3
  descent to 29.95 M rows, live LSDB mirror + match). The `hats-hf-access`
  check added with this release runs the same resolve-and-index path that was
  verified against the live Hub above; it has not yet run in a remote session.

## Install

The release assets are the CI-tested wheel and source distribution with
`SHA256SUMS`:

```bash
python -m pip install ./xmatcher-0.6.0-py3-none-any.whl
```

From PyPI, once the tag is published:

```bash
python -m pip install "xmatcher[cds,hats-ray,torchfits,ml]==0.6.0"
```
