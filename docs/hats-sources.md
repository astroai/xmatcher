# HATS catalogue sources

HATS is a storage format, not a registry: a HATS catalogue is named by the URL
of its tree. Two discovery routes are available, and `xmatcher` supports both.

## Resolving a registry name

The IVOA registry publishes HATS services with a `short_name`. `xmatcher`
resolves those names, so the URL does not have to be pasted:

```bash
xmatcher list --hats                  # registered catalogues and their URLs
xmatcher sync hats:two_mass           # mirror the tree into the cache
```

Resolution rules:

- Case-insensitive exact `short_name` (or exact resource title) wins.
- A unique prefix is accepted (`hats:des_dr2`); an ambiguous one lists the
  candidates instead of guessing.
- The registry snapshot is cached under
  `<cache>/registry/hats_registry.json` for seven days and is reused when the
  registry is unreachable, so a name that resolved once keeps working offline.
- HTTPS, VOSpace, `s3://` and `hf://datasets/…` registrations resolve. S3
  trees are read anonymously over plain HTTPS (no credentials, no `boto3`); a
  bucket that requires signed or requester-pays access is reported as denied.
- Hugging Face datasets are read through the Hub's public HTTP API (tree
  listing with sizes, `/resolve` downloads), so no token, no
  `huggingface_hub` and no repository clone is involved. A private or gated
  dataset is reported as denied.
- Anything else registered as an access URL is rejected with the URL named
  rather than substituted with a different copy.

`hats:<name>` produces the same `CatalogueSource` as `hats:<url>`; the resolved
URL is logged, and the mirror cache directory is named after the tree.

## Resolving a URL

`hats:<url>` accepts either a single HATS tree or a *collection root*, over
HTTPS, VOSpace, S3 or Hugging Face:

- A tree is used as given (it has `properties` / `hats.properties`).
- A collection root (LSDB publishes one per catalogue, e.g.
  `…/hats/gaia_dr3/`) is descended to its single primary tree, skipping the
  auxiliary HEALPix margin trees (`*_5arcs`, `*_10arcs`) and id indexes
  (`*_index`) that sit beside it. `…/hats/gaia_dr3/` therefore resolves to
  `…/hats/gaia_dr3/gaia`.
- A collection holding several real catalogues (for example the time-versioned
  TNS and VSX collections) is **not** guessed: pass one dated tree URL.

## Verified sources

All URLs below were resolved through the registry and read successfully on
2026-10-22 (HTTPS trees, anonymous S3 buckets, and public Hugging Face
datasets). Row counts are the catalogues' own `hats_nrows`; they change with
each upstream rebuild.

| Registry name | Catalogue | Resolved tree | Rows | Size |
|---|---|---|---:|---:|
| `gaia_dr3` | Gaia DR3 `gaia_source` | `https://data.lsdb.io/hats/gaia_dr3/gaia` | 1,812,731,847 | ~712 GiB |
| `two_mass` | 2MASS PSC | `https://data.lsdb.io/hats/two_mass/two_mass` | 470,992,970 | ~41 GiB |
| `allwise` | AllWISE | `https://data.lsdb.io/hats/wise/allwise/allwise` | 747,634,026 | ~246 GiB |
| `des_dr2` | DES DR2 | `https://data.lsdb.io/hats/des/des_dr2/des_dr2` | 662,428,385 | ~636 GiB |
| `des_y6_gold` | DES Y6 Gold | `https://data.lsdb.io/hats/des/des_y6_gold/des_y6_gold` | 691,483,608 | ~1.2 TiB |
| `desi_dr1_z_catal` | DESI DR1 redshift catalogue | `https://data.lsdb.io/hats/desi/desi_dr1_zcat/desi_dr1_zcat` | 28,425,963 | ~12 GiB |
| `euclid_q1` | Euclid Q1 merged objects | `https://data.lsdb.io/hats/euclid_q1/euclid_q1_merFinalCatalog` | 29,767,806 | ~22 GiB |
| `ztf_dr14_objects` | ZTF DR14 objects | `https://data.lsdb.io/hats/ztf_dr14/ztf_object` | — | — |
| `skymapper_dr4` | SkyMapper DR4 | `https://data.lsdb.io/hats/skymapper_dr4/catalog/sky_mapper_dr4` | 724,329,344 | ~96 GiB |
| `tic_v8_2` | TESS Input Catalogue v8.2 | `https://data.lsdb.io/hats/tess/tic/tic` | 1,727,981,580 | ~397 GiB |
| `vsx` | AAVSO VSX (dated collection) | `https://data.lsdb.io/hats/vsx/2026-10` | 10,304,730 | ~535 MiB |
| `tns` | Transient Name Server (dated collection) | `https://data.lsdb.io/hats/tns/2026-10-22` | 210,788 | ~34 MiB |

The last two are refreshed on a rolling basis; their collection roots hold one
tree per date, so a reproducible run pins the dated tree URL.

`xmatcher list --hats` prints the live list. It currently holds 90 entries,
most of them small survey or model catalogues hosted on LSDB, plus the
Pan-STARRS and ZTF copies at MAST/IRSA and the 41 Multimodal Universe model
catalogues on Hugging Face (see below). Every registered entry is readable
here.

### CDS pinned HATS products

CDS also serves HATS on the fly. These products are not in the IVOA HATS
registry, so `xmatcher` pins the tree URL in `xmatcher.yaml`; use the aliases
or pass `hats:<url>` directly.

| Alias | Tree | Rows | Notes |
|---|---|---:|---|
| `gaia_hats` | `https://vizcat.cds.unistra.fr/hats/gaia_dr3/main` | 1,811,709,771 | Gaia DR3 core; no XP coefficients |
| `sdss_hats` | `https://vizcat.cds.unistra.fr/hats/sdss_dr16/main` | 1,231,051,050 | Static SDSS imaging photometry; DR20 adds no imaging |
| `catwise_hats` | `https://vizcat.cds.unistra.fr/hats/catwise2020/main` | 1,890,715,640 | CatWISE2020, including proper motions |
| `vhs_hats` | `https://vizcat.cds.unistra.fr/hats/vhs_dr5/main` | 1,374,207,485 | VHS DR5, per-band MJD |
| `ps1_hats` | `https://vizcat.cds.unistra.fr/hats/ps1_dr2/main` | 1,868,236,315 | PS1 DR2, 67 columns |
| `galex_hats` | `https://vizcat.cds.unistra.fr/hats/galex_gr67/main` | 82,992,086 | GALEX GR6/7 |
| `splus_hats` | `https://vizcat.cds.unistra.fr/hats/splus_dr4/main` | 27,235,968 | S-PLUS DR4 public HATS; DR5 is not public |

The CDS service supports row limits (`:n=`) and asks that large catalogues are
not bulk-harvested. The mirror's rate limit and incremental manifest apply.
`sdss_hats` is the photometry source for the Gaia-anchored workflow: SDSS
DR20 explicitly states that it contains no new or updated imaging data, and
the DR16 HATS carries the static `ugriz` PSF/model magnitudes and per-band
MJD epochs. See [ray-anchor.md](ray-anchor.md).

### S3-hosted sources

The STScI, IRSA and MAST cloud copies are registered as `s3://` prefixes. They
are public and answer anonymous `ListObjectsV2`/`GET`, so a `sync` of the
registry name mirrors them exactly like an HTTPS tree. Each is a *collection*
root: the `collection.properties` file names the primary table
(`hats_primary_table_url`), which is the tree that gets synced.

| Registry name | Registered URL | Resolved tree | Rows |
|---|---|---|---:|
| `gaia_dr3_stsci` | `s3://stpubdata/gaia/gaia_dr3/public/hats` | `…/hats/gaia` | 1,812,731,847 |
| `pan_starrs1_obje` | `s3://stpubdata/panstarrs/ps1/public/hats/otmo` | `…/otmo/otmo` | 10,560,724,292 |
| `pan_starrs1_dete` | `s3://stpubdata/panstarrs/ps1/public/hats/detection` | `…/detection/detection` | 72,338,190,483 |
| `ztf_dr23_objects` | `s3://ipac-irsa-ztf/contributed/dr23/objects/hats` | `…/hats/ztf_dr23_objects-hats` | 7,568,652,529 |
| `ztf_dr23_lightcu` | `s3://ipac-irsa-ztf/contributed/dr23/lc/hats` | `…/hats/ztf_dr23_lc-hats` | 4,973,896,193 |
| `euclid_q1_ipac` | `s3://nasa-irsa-euclid-q1/contributed/q1/merged_objects/hats` | `…/hats/euclid_q1_merged_objects-hats` | 29,953,430 |

Sizes are large: the mirror's file list for the Euclid tree is 118 files (114
pixel parts) totalling **344.6 GiB**; the bucket itself holds 123 objects
(also skymaps, line counts and checksums, which are not mirrored). Gaia DR3 is
~712 GiB. Check the free-space floor (`XMATCHER_MIN_FREE_GB`, default 10 GiB)
before syncing one of these.

Their `partition_info.csv` carries only `Norder,Dir,Npix,count` — no `file_loc`
and no `Nfiles` — and pixels are written as `Npix=N/part0.snappy.parquet`
directories. The mirror derives the partition path from the pixel numbers and
accepts a pixel directory as a partition, so both forms resolve without a
hand-written table.

### Hugging Face-hosted sources

The Multimodal Universe catalogues (`mmu_*`) and a few community HATS trees are
registered as `hf://datasets/<owner>/<name>`. A Hub dataset is a git
repository, so the reader uses its public API: one paginated tree listing for
names and sizes, and `/resolve/<revision>/<path>` for the bytes. No token, no
`huggingface_hub` dependency and no clone of a multi-gigabyte repository; the
mirror is written locally, never back into the Hub.

Each dataset is a *collection* root (`collection.properties` at the top,
`hats_primary_table_url` naming the tree), exactly like the S3 copies, so the
same descent applies.

| Registry name | Registered dataset | Resolved tree | Rows |
|---|---|---|---:|
| `mmu_gaia_gaia` | `hf://datasets/UniverseTBD/mmu_gaia_gaia` | `mmu_gaia_gaia` | 122,302,572 |
| `mmu_legacysurvey` | `hf://datasets/hugging-science/mmu_legacysurvey_dr10_south_21` | `mmu_legacysurvey_dr10_south_21` | 123,185,970 |
| `mmu_euclid_q1` | `hf://datasets/hugging-science/mmu_euclid_q1` | `euclid` | 29,953,430 |
| `mmu_desi_edr_sv3` | `hf://datasets/UniverseTBD/mmu_desi_edr_sv3` | `mmu_desi_edr_sv3` | 1,126,441 |
| `mmu_jwst_gds` | `hf://datasets/UniverseTBD/mmu_jwst_gds` | `mmu_jwst_gds` | 17,494 |
| `mmu_btsbot` | `hf://datasets/UniverseTBD/mmu_btsbot` | `mmu_btsbot` | 818,214 |
| `galaxy_descripti` | `hf://datasets/Smith42/galaxy_descriptions_hats` | `galaxy_descriptions` | 275,613 |
| `tevcat_hats` | `hf://datasets/youyouli-astro/TeVCat_HATS` | `tevcat` | 363 |
| `mmu_cfa_seccsn` | `hf://datasets/UniverseTBD/mmu_cfa_seccsn` | `mmu_cfa_seccsn` | 64 |

On 2026-10-22 all 41 `hf://`-registered catalogues resolved live (name →
collection → tree → `properties`), and `hats:mmu_cfa_seccsn` was mirrored end
to end: 66 files, 362 KB, 64 rows read back. Sizes vary enormously —
`mmu_jwst_gds` alone is ~10 GiB — so check the free-space floor before syncing
a large model catalogue.

## Access and attribution

- LSDB's `data.lsdb.io` copies are public HTTP; no credentials.
- IRSA/STScI/MAST cloud data are public, without egress charges or
  credentials; `xmatcher` sends unsigned S3 requests only, and the mirror is
  written locally (never back into the bucket).
- Hugging Face datasets are read anonymously over HTTPS; `xmatcher` sends no
  token and writes its mirror to the local cache, not to the Hub.
- CDS also serves HATS on the fly at `https://vizcat.cds.unistra.fr/hats/`
  (experimental; supports `:n=` row limits, and asks that large catalogues are
  not bulk-harvested).
- No HATS catalogue carries a separate format licence: follow the source
  survey's citation, acknowledgement and data-use terms.

## Using a source

```bash
# Mirror a tree into the cache (incremental; safe to re-run).
xmatcher sync hats:https://data.lsdb.io/hats/tns/2026-10-22

# Local match against the directory `sync` reports (cache/<name>/<version>).
xmatcher match <mirrored-directory> targets.parquet -r 1.0 -o out.parquet

# Distributed N-way union: remote HATS inputs are mirrored on demand.
xmatcher match hats:gaia_dr3 hats:two_mass --engine ray-union -r 2.0 -o union.hats
```

`match` (the in-memory engines) needs a mirrored tree, because a remote HATS
catalogue cannot be zoned locally without downloading it; the error names the
`xmatcher sync` command to run. `ray-union` mirrors remote inputs itself, and
`--no-sync` refuses to start without an existing copy.

A live check of the whole path — registry lookup, collection descent, HTTP
walk, mirror — is `scripts/canfar_headless_run.py hats-sources` on CANFAR, which
mirrors the small TNS tree and matches a synthetic source against it.

## Metadata the mirror needs

`xmatcher` finds partitions from `dataset/partition_info.parquet` when it
exists, otherwise it walks the tree (HTTP listing, S3 object listing or Hub
tree API) and accepts `partition_info.csv`, `_common_metadata`, and the
per-pixel parquet files. Trees whose listing has no `properties` /
`hats_nrows` are rejected before any pixel is downloaded.
