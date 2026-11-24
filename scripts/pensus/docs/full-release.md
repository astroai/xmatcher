# Full-sky release audit

## Matching and I/O

The original PENSUS production instructions pointed to a tile driver that
returned an error for real inputs and a view assembler that only wrote a
summary of demo edge markers. The generic xmatcher `ray-union` enumerates
combinations around a position and can repeat a survey row in a crowded
field. The Gaia-centric `ray-anchor` route retains every Gaia row and leaves
out objects that have no Gaia counterpart.

The full release uses an ordered cascade of reciprocal-nearest matches.
Each survey seeds components from its unclaimed rows. Its nearest pairs
with each later survey are resolved independently, so a component contains
at most one row per survey. Every eligible native row is retained, including
secondary-only sources. Tie-breaking uses the native key and the output
HEALPix pixel owns the component at the seed's matching coordinates.
Survey priority and pair radii are part of the release plan and should be
calibrated on the pilot.

For a reciprocal decision, a competing source can be two match radii away
from a source of interest. The tile cover adds twice the largest pair
radius at each cascade level. Motion-enabled surveys also include their
declared maximum shift from native position to the common epoch. Set
`target_epoch_jyear`, each moving survey's `epoch_jyear`, its two `pm`
columns in mas/yr (`pm_ra_cosdec`, `pm_dec`) and `motion_bound_arcsec`.
Valid positions with both motion components are propagated. Missing PM
keeps native coordinates and the known native epoch. Surveys with unknown
epochs carry a null. Workers stop if measured motion exceeds the pinned
bound. Use verified reference epochs and motion units when enabling this.

Planning scans HATS metadata and records file versions. Each worker only
opens intersecting Parquet partitions and selects the declared columns.
When the tree has a valid `_healpix_29`, Parquet predicates can skip row
groups. Use `filter_index: false` for inputs that require recomputing the
index from coordinates. Projected batches are clipped to the cover before
joining. Match work is tile-local, a search index is reused for each active
spine, and output writes stream in bounded row groups. No mirror or
full-catalogue intermediate is required. The output is already spatially
partitioned, so finalization verifies HATS footers without shuffling or
rewriting the tile data. It can validate and discard each footer as it goes,
while retaining the partition inventory and shared schema.
[HATS margin geometry](https://docs.lsdb.io/en/stable/tutorials/margins.html)

## Survey review

The November 23 CANFAR headless inventory found 62 mounted HATS object
tables. The [26-survey pilot manifest](../config/full-union-canfar-pilot.yaml)
pins verified column names for 27,588,645,537 advertised input rows. Plan from
the live inventory and record the reason for each excluded tree.
Check permissions on restricted products before distributing a public view.

The sampled Legacy DR11, PS1 OTMO, CatWISE, KiDS and several VISTA leaves
each had a single Parquet row group. A tile may have to decode that group's
selected columns even when the spatial window is small. Compare adjacent
tiles in the same worker so they can benefit from filesystem caching. Tune
the output order against measured ARC I/O and dense-field memory.

| Candidates recorded in the checkout | Audit needed on CANFAR |
|---|---|
| Gaia DR3, PS1 OTMO, SkyMapper DR4, Legacy Surveys DR11, DES Y6, DELVE DR3, SDSS | Select optical products and their priority. Use Legacy's composite key. Verify coordinates and HATS ownership for DES and DELVE, where the registry records QA warnings. Measure alternative optical products separately for duplicate-band coverage. |
| 2MASS PSC and XSC, CatWISE, AllWISE, VHS, UHS, UKIDSS, VIKING, VIDEO, VMC, VVVX | Confirm native IDs and overlaps. Retain native uncertainty and blend/contamination flags. Review extended-source products and WISE bands with lower angular resolution. |
| GALEX, S-PLUS, J-PLUS, J-PAS, VST ATLAS, KiDS, HSC, Euclid and other footprint-limited imaging | Verify measurement and flag shapes and band definitions. The sampled J-PLUS and J-PAS releases store `MAG_AUTO` as text. Verify and convert the per-band arrays before including those products in model inputs. Review HSC's per-band object IDs for a shared-key photometry join and choose the Euclid release products without duplicating their measurements. |
| Gaia XP and GSPC, Pristine, repeat detections, image products, redshift compilations | Treat catalogue products that share a native identity as keyed data where possible. Review source, detection and image tables by data product before giving a catalogue a position in the cascade. |

The mounted 2MASS XSC sample exposes only its designation, coordinates and
HEALPix index. The PSC projection already carries J, H and Ks measurements
and their uncertainties. The XSC photometry would require a corresponding
measurement table.

The current cascade treats selected object catalogues uniformly. Lower
resolution and blended catalogues such as GALEX, AllWISE W3/W4, and SPHEREx
need separate calibration before using a one-to-one association as a science
default. Preserve ambiguous candidates or native-ID-linked measurements in
an accompanying table when those cases are needed. The earlier v0 attachment
policy is documented in [matching-policy.md](matching-policy.md).

Release finalization requires a marker for every sky pixel, exact aggregate
per-survey row counts and agreement of native-ID membership tallies before
and after the union. It verifies the pinned input metadata and file versions,
HATS partition schemas and row counts, and all output SHA-256 checksums
with `--deep`. The full run also needs measured ARC throughput and peak
memory on a representative pilot. Before publication, check footprint
completeness, band and uncertainty null fractions, astrometric residuals
by epoch and source density, reciprocal-match purity against independent
references, colour and magnitude distributions, and survey selection
masks. Full-sky processing preserves the actual footprints of each survey.

## Science handoff

Each row has `component_id`, the seed's `spine_release`, matching coordinates,
`position_epoch_jyear`, and `_healpix_29`. The component key uses the seed release
namespace and native key. It is deterministic within a pinned plan. Use
`id_<tag>` plus the release namespace for survey joins across PENSUS versions.
Composite native keys are JSON objects with named fields. Binary IDs keep
their native representation in `id_<tag>` and use hex in component keys.
The column map
records the original names and their output equivalents. Every survey also
has a `sep_arcsec_<tag>` with its match distance. Seed separations are zero
and absent surveys are null.

- **Masked Stellar Autoencoder.** Use `id_gaia` to join Gaia XP coefficients
  and XP quality masks. Use the projected photometry and uncertainty columns
  to construct masked inputs. Gaia XP and stellar labels retain their own
  provenance.
- **USPM.** Use the release's band map to resolve passbands and build masked
  measurement inputs. Carry upper limits, native flags and the distinction
  among sigma, variance and inverse variance when defining a normalized
  measurement mapping. The native columns and units are preserved. The
  observation interface can produce calibrated long-form photometry from
  audited mappings. [Observation contract](../../../docs/observations.md)
- **Zensus and photo-z.** Link against a versioned Zensus release through a
  shared native key when available, and use positional candidate links with
  separation and ambiguity otherwise. Keep the PENSUS component ID,
  Zensus key, redshift method and quality in the derived training and
  evaluation products. Record sky and source splits to prevent leakage.
- **SPHEREx.** Use PENSUS IDs and positions as priors for forced
  spectrophotometry against DR1 spectral images, or associate with an
  appropriate released source product. Record the image or spectrum
  provenance, band or wavelength, flux uncertainty and blending flags.
  Store SED latents and inferred redshift posteriors with their model and
  data release; separate them from catalogue redshift measurements in
  training and evaluation. DR1's spectral images and 102-band all-sky cube
  are distinct archive products from the high-reliability source catalogue.
  [IRSA DR1](https://irsa.ipac.caltech.edu/data/SPHEREx/docs/overview_dr1.html)
  [IRSA release schedule](https://irsa.ipac.caltech.edu/spherex-archive-documentation/spherex-data-releases/)

The release workflow produces the photometry union and join keys. Model
adapters, Zensus links and image-level SPHEREx measurements are downstream
products.

## Handoff

The CANFAR verification artifacts are under
`/arc/projects/astroai/pensus/tests/xmatcher-20261123-2259`.
`inventory.json` lists mounted object tables, and `schemas.json` records a
sample partition's schema, row groups and file size for each table.
The Gaia–2MASS pilot completed in headless session `wwzjmr2e` with 55,227
components across four tiles. Its draft and per-tile receipts are in
`gaia-2mass-pilot/`. The second headless regression pass reported success
for lint, format, types and all 60 targeted tests in `r2/qa-result.json`.
The 26-survey preflight pinned its plan at `r2/broad-pilot/plan.json`.

The upstream `astroai/main` commit `99da90d` is an ancestor of the fork
baseline `266e85c`. The baseline includes the fork's Ray anchor memory
reservation update and concurrent footer planning. The wide PENSUS
cascade has its own pixel geometry and projected input budget.

The merged-revision source prepared for headless verification is
`src-r3.tar.gz`. Acceptance of the final CANFAR session request could not
be confirmed during authenticated session timeouts. The launch decision
still requires the merged regression receipt, a successful live wide
pilot and the final inventory and science selections.

The production manifest should record the live inventory decisions,
science-reviewed band mappings, priority, pair radii and coordinate epochs.
Re-run dense, sparse, overlap and footprint-edge fields with that manifest
and use their resource measurements to set the full-sky job size and ARC
budget. Keep in-progress files in the AstroAI PENSUS project and use the
archive paths as mounted inputs. Finalize the draft after the full tile
inventory, membership tallies and data checks pass.
