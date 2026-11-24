# PENSUS photometry and association decisions

Use calibrated, signed linear flux density in Jy for model inputs. Keep the
survey's observed native photometry, actual error representation, quality
flags and measurement method in the union. Derive calibrated views when they
are consumed. Store the bandpass identifier and version, calibration
reference and native system with each mapping. Keep magnitude, colour and
luptitude features as derived views with their transformations and masks.
[SDSS photometric conventions](https://www.sdss4.org/dr16/algorithms/magnitudes/)
[Lupton, Gunn and Szalay](https://arxiv.org/abs/astro-ph/9903081)

## Photometry

- Use native calibrated flux where available, including negative measured
  flux. For a native inverse variance $w>0$, the statistical uncertainty is
  $1/\sqrt{w}$ in native flux units. Zero or invalid inverse variance yields
  an unavailable error. If $f_\mathrm{Jy}=sf$, scale the standard error by
  $s$. Calibrated native counts need a survey- and passband-specific scale.
- Use a magnitude and its matching uncertainty when that is the available
  calibrated measurement. Convert with $f_\nu=f_{\nu,0}10^{-0.4m}$.
  For likelihoods use the native magnitude error, or the two asymmetric
  transformed bounds when a consumer supports them. Treat an archive
  upper limit as censored with its stated confidence and aperture. Preserve
  states for observed flux, limit, masked data, an uncovered position, and
  unknown coverage. A catalogue non-match alone leaves coverage unknown.
- Select PSF measurements for unresolved stellar sources. For galaxies choose
  matched-aperture colours and a documented total-flux estimate when
  available. Keep PSF, model and aperture products separately labelled.
  Observe the native attenuation convention and apply extinction only in a
  named derived view. Use passband response curves for SED likelihoods and
  redshifts. A band name or effective wavelength alone is insufficient.
- Keep shared-exposure families in the feature provenance. Treat Legacy
  unWISE, AllWISE and CatWISE as related WISE evidence. Treat KiDS NIR and
  standalone VIKING as related measurements, as well as DES, DELVE and
  Legacy where the DECam exposures overlap. Select a preferred measurement
  within a family or fit an explicit covariance before combining signals.
  [Legacy DR11 data model](https://www.legacysurvey.org/dr11/files/)
  [KiDS DR5 release paper](https://doi.org/10.1051/0004-6361/202346730)

The pilot manifest pins native measurements using the sampled CANFAR schemas.
The following decisions apply to those products.

| Products | Measurement selected and release conditions |
|---|---|
| Gaia DR3 | Retain mean G/BP/RP photoelectrons per second and errors. Use the documented DR3 AB zero points for band-averaged Jy. Keep native G for MSA scaling, BP/RP quality, source ID and astrometry. Treat XP and synthetic photometry as related Gaia measurements. [Gaia DR3 calibration and passbands](https://gea.esac.esa.int/archive/documentation/GDR3/Data_processing/chap_cu5pho/cu5pho_sec_photProc/cu5pho_ssec_photCal.html) |
| Legacy DR11 | Keep observed Tractor model flux and inverse variance. Preserve `PHOTSYS` for northern and southern optical passbands, composite key, extinction transmission, observing counts, fit and mask bits, and the reference ID plus its catalogue. Retain forced WISE with its measurement method and blending proxies. [Legacy DR11 data model](https://www.legacysurvey.org/dr11/files/) |
| PS1 OTMO | Use native AB mean PSF and Kron magnitudes with their errors and flags. Mask the documented finite sentinel. Use `epochMean` as the MJD of the mean position and keep RA/Dec errors. Calibrate the stack-only and low-detection-count strata explicitly. [MAST mean-object fields](https://outerspace.stsci.edu/spaces/PANSTARRS/pages/298812411/PS1%2BMeanObjectView%2Btable%2Bfields) |
| DES Y6 Gold and DELVE DR3 Gold | Keep observed BDF flux and its matching error, fit/deblend and footprint flags, and the weighted PSF magnitude for point sources. Pin FITVD flux calibration from the exact product before mapping to Jy. Check any corrected measurement and error as a matched pair with their documented aperture and extinction convention. [DES Y6 Gold](https://lss.fnal.gov/archive/2024/pub/fermilab-pub-24-0932-ppd.pdf) [DELVE DR3 data dictionary](https://datalab.noirlab.edu/data-explorer?showTable=delve_dr3.coadd_objects) |
| SDSS PhotoPrimary | Use PSF and model fluxes with their inverse variances in nanomaggies and retain band flags. Account for the SDSS passband and small system offsets in precision SED work. The optical identity uses the actual imaging epoch when available. [SDSS photometric conventions](https://www.sdss4.org/dr16/algorithms/magnitudes/) |
| SkyMapper DR4 and S-PLUS DR4 | Keep SkyMapper PSF (uvgriz), flags and its mean epoch. Use the documented S-PLUS dual AUTO AB magnitudes with errors and distinct narrow-band curves. Extended SkyMapper photometry and S-PLUS aperture or PSF variants require separate mappings. [SkyMapper DR4](https://skymapper.anu.edu.au/data-release/dr4/) [S-PLUS DR4](https://splus.cloud/documentation/DR4) |
| VST ATLAS, KiDS DR5, CLAUDS | Preserve ATLAS AperMag3 and quality bits. Use KiDS GAaP colour measurements and aperture and astrometry flags. Pin the delivered magnitude-system or flux calibration for the exact ATLAS and KiDS products before converting; KiDS GAaP flux columns and calibrated magnitudes have different conventions. Keep CLAUDS native values with errors pending identification of the mounted product and its filters and units. [VST ATLAS DR5](https://www.eso.org/rm/api/v1/public/releaseDescriptions/240) [KiDS DR5 formats](https://kids.strw.leidenuniv.nl/DR5/format.php) [CLAUDS products](https://www.clauds.net/available-data) |
| Rubin DP1 Object | Use observed PSF and cModel fluxes in nJy and band flags. Keep the reference-band detection and deblend context for overlap tests. Link DIA and photo-z products by their supported IDs. [Rubin DP1 Object](https://dp1.lsst.io/products/catalogs/object.html) [Rubin flux units](https://dp1.lsst.io/tutorials/portal/103/portal-103-3.html) |
| 2MASS PSC | Keep Vega J/H/Ks and combined errors, error ellipse, `jdate`, confusion and read flags. Use each band's `rd_flg` character, including 0 or 6 for the 95% limit and 9 for a missing measurement. Pin the 2MASS Vega zero points. The mounted XSC sample contains geometry and IDs only. [PSC definition](https://irsa.ipac.caltech.edu/2MASS/download/allsky/format_psc.html) [2MASS zero points](https://www.ipac.caltech.edu/2mass/releases/allsky/doc/sec6_4a.html) |
| VHS, VIKING, VIDEO, VVVX, VMC, UHS and four UKIDSS programmes | Keep native AperMag3 magnitudes and corresponding errors, plus available selection and quality flags. Use the particular VIRCAM/WFCAM bandpasses and Vega transformations. Verify primary/secondary policy, astrometric epoch and depth independently for each product. Include dedicated VVVX/VMC crowded and variable-source controls. [ESO VISTA product conventions](https://www.eso.org/sci/observing/phase3/policies_eps_vista.html) [UKIRT WFCAM archive](https://about.ifa.hawaii.edu/ukirt/working-with-wfcam-data-and-the-wfcam-science-archive/) |
| CatWISE, AllWISE, GALEX GUVcat | Link as separate measurements. Retain WISE profile-fit raw DN and errors, calibrated magnitudes or limits, flags and fit statistics. Convert DN only after verifying the pipeline scale for that product. Retain GALEX calibrated FUV/NUV microJy and errors, artifacts, coverage and visit context where available. A group with several plausible objects retains the original flux and all candidate links. [CatWISE columns](https://irsa.ipac.caltech.edu/data/WISE/CatWISE/gator_docs/catwise_colDescriptions.html) [AllWISE photometry](https://irsa.ipac.caltech.edu/data/WISE/docs/release/AllWISE/expsup/sec2_1.html) [GALEX GUVcat](https://archive.stsci.edu/hlsp/guvcat) |

The remaining mounted products need a release-specific decision. Group HSC
forced broadband and narrow-band tables by verified same-release object
key, then use calibrated flux and geometry from their documented reference
band. For Euclid select versioned DR1 native VIS/Y/J/H catalogues and
their documented fluxes and flags. Check offsets from
`ra_from_object_id` / `dec_from_object_id` to true centroids against
the proposed HATS cover before using them for matching. Keep externally
imported Euclid bands in their original exposure family. Recover J-PLUS and
J-PAS passband order while converting serialized arrays to typed values.
Treat the UNIONS CFIS/ShapePipe/GAaP products and CLAUDS overlaps as
footprint-specific inputs with provenance and publication access. Resolve
Hubble multi-instrument magnitude fields with their filter, aperture and
uncertainty conventions. Keep LoTSS and eROSITA as beam-aware radio/X-ray
links and keep Zensus redshift evidence distinct from the photometry.
[HSC PDR3 photometry FAQ](https://hsc-release.mtk.nao.ac.jp/doc/index.php/faq__pdr3/)
[Euclid DR1 MER data model](https://euclid.esac.esa.int/dr/dr1/dpdd/merdpd/dpcards/mer_finalcatalog.html)

To derive a ledger with `normalize_photometry`, use the release
`column_map` to resolve native column names in the wide tree. Map
`filter`, `value`, `uncertainty` and `uncertainty_kind` to the ledger's
`passband`, `value_column`, `error_column` and `error_kind`. Carry
calibration, limit and missing-value definitions with that mapping.
Where a mapping declares a passband selector such as Legacy `PHOTSYS`,
partition by the selector and resolve each actual passband before
normalization. The model adapter must carry forward native quality flags.

## Association and radii

Use a verified release-scoped native ID first. Gaia XP and Pristine join by
Gaia ID, and HSC bands join by a verified within-release object ID. Inspect
external ID links such as Legacy `REF_CAT` with `REF_ID`, including nulls,
provenance, conflicts and collisions. Keep mismatches and alternatives as
audit evidence.

Use Gaia as the stellar astrometric reference where it has a suitable
solution. For sources outside Gaia, prefer the available high-resolution
optical or NIR centroid with clean deblending. Measure and document any
preference that varies by footprint or object extent. Keep high-motion
recovery and low-resolution links from changing an optical source's identity.

For positional identity use covariance-aware scoring at the partner
position's epoch with a local background density. In the common tangent
plane, evaluate $d^2=\boldsymbol\Delta^TC^{-1}\boldsymbol\Delta$.
$C$ uses both transported covariances and fitted differential and
morphology terms. Test for correlated astrometry in shared imaging or
reference-frame products when fitting the differential errors. The
likelihood ratio for a Gaussian core is proportional
to $e^{-d^2/2}/(2\pi\sqrt{|C|}\rho)$, where $\rho$ is the background
density in the candidate catalogue in compatible angular units. Fit
covariance floors on held-out residuals. Include a no-counterpart
hypothesis and competing candidates. A starting Gaussian plausibility
gate is $d^2 \le 11.83$, the 99.73% contour for two degrees of freedom.
It is a test setting and its coverage needs measurement. Treat close
competitors, resolution-dependent blends and large-galaxy centroids as
ambiguous until their evidence supports a claim.
[Gaia DR3 crossmatch algorithm](https://gea.esac.esa.int/archive/documentation/GDR3/Catalogue_consolidation/chap_crossmatch/sec_crossmatch_algorithm/)
[Nway likelihoods and null hypotheses](https://doi.org/10.1093/mnras/stx2651)

| Pair family | Initial pilot settings |
|---|---|
| Gaia and precision optical | 0.7–0.85″ circular baseline for Rubin, PS1, Legacy, DES/DELVE, KiDS and CLAUDS. Include 0.3, 0.5, 0.8 and 1.2″ in the radius comparison. Expect position shifts for extended centroids. |
| Ground optical with broader astrometric tails or longer time baselines | 1.0–1.25″ baseline for VST ATLAS, SDSS, S-PLUS and SkyMapper. Compare 0.5, 0.8, 1.2 and 1.8″, including magnitude and high-motion strata. |
| Resolved near-IR catalogues | 1.0–1.1″ baseline for WFCAM/VIRCAM. Start at 1.5″ for 2MASS and 1.6″ for Legacy–2MASS. Compare 0.6, 1.0, 1.5 and 2.0″ after epoch alignment, with ellipse and crowding checks. |
| GALEX and WISE links | 5″ GALEX, 4″ CatWISE and 5″ AllWISE as initial candidate scopes. Use per-source error information and local density for centroids, and band-dependent PSF or aperture footprints to enumerate possible contributors. GALEX's published comparison used a 3″ cone and tracked multiple plausible matches. [GUVmatch](https://arxiv.org/abs/2007.03808) [WISE crowding study](https://academic.oup.com/mnras/article/481/2/2148/5090169) |
| SPHEREx, LoTSS and eROSITA | Use positional likelihoods and image or component footprints from the product. For SPHEREx use forced, jointly deblended spectrophotometry with high-resolution priors for SEDs. Retain native source and image provenance and the option of an unidentified or confused source. [IRSA SPHEREx spectrophotometry](https://irsa.ipac.caltech.edu/onlinehelp/spherex/spherex/sp.html) |

The manifest's circular baseline uses the larger of the two input caps
and any explicit pair override. These are starting values for measuring
trade-offs. The current runner implements deterministic reciprocal nearest
neighbours and optional alignment to one common epoch. Its
`candidates` route keeps all positional possibilities and unmatched
primaries on a chosen tile for calibration. It pins WISE and GALEX link
inputs while excluding them from the identity cascade. Epoch-per-pair
covariance scoring, posterior calibration, conflict resolution and
beam-aware links are still required for the science release. Whole
connected conflict groups must be reconciled across tile edges if global
assignment is used. The reciprocal cascade's finite halo guarantee only
applies to its own per-level decisions.

Use the measured position epochs and their units. PS1 supplies MJD, 2MASS
supplies JD, and Gaia reference epochs are Julian years. J2000 is an
equinox or frame label for many catalogues. Propagate trusted moving
sources over the actual interval, with covariance, and retain a separate
high-motion recovery pass whose search margins account for the actual
predicted displacement. Where PM or epoch is missing, calibrate a
population-dependent uncertainty and preserve that status. Avoid widening
a uniform cone to cover the high-motion tail. The expected number of
uniform random neighbours scales as `π r² ρ`; doubling (r) quadruples
it at fixed density. [Gaia DR3 crossmatch algorithm](https://gea.esac.esa.int/archive/documentation/GDR3/Catalogue_consolidation/chap_crossmatch/sec_crossmatch_algorithm/)

The runner accepts a per-row `position_epoch` column with an explicit
`jyear`, `mjd` or `jd` format and optional native missing sentinels.
Positions without a valid row epoch use the manifest's scalar
`epoch_jyear` only when it is supplied for that catalogue. Candidate files
report the effective Julian year alongside each position.

## Calibration and release gates

1. Build disjoint spatial calibration and holdout sets within each pair's
   common footprint. Cover high Galactic latitude, bulge and plane, dense
   clusters, Magellanic Clouds, crowded extragalactic cores, bright-star
   masks, large nearby galaxies, deep fields, sparse fields, both
   hemispheres, poles, the RA seam, HATS boundaries and survey edges.
   Bin by true coverage, depth or detection significance, local density,
   source extent, band resolution, epoch interval and motion. Record the
   footprint and depth-map versions used for every stratum.
2. Export candidates before any winner is selected. Measure residuals,
   directional systematics, multiplicity, the null fraction and failure
   rates across the radius grid. Use verified ID matches, Gaia neighbours
   and external survey matches with their own selection biases accounted
   for. Measure the false-link background with several shifts within
   the same depth and density regime, preserving masks and excluding
   the real core. Use image injections and resolved duplicates to assess
   completeness and blending. Compare tile interiors to exact spherical
   and independent reference results, including ties and both sides of
   boundaries.
3. Fit the positional floor, local background and no-counterpart fraction
   on calibration fields. Validate the credibility of match probabilities
   and the upper limits of contamination per stratum on held-out fields.
   As a pilot objective, target at most 0.5% contamination for automatic
   stellar and spectroscopic training links within the supported quality
   strata. Report recovery and null fractions alongside this figure with
   uncertainty and denominator. Keep uncalibrated or underpowered strata
   as ambiguity, lower-confidence evidence or unmatched rows. Measure
   sensitivity to seed order and confirm that accepted secure matches do
   not change when the sky is retiled.
4. Verify the photometry independently using colours, overlap flux pulls,
   faint-end negative-flux fractions, limits, source morphology, saturation,
   and missingness vs depth and flags. Carry a depth/coverage mask to
   separate the survey selection function from model missingness. In USPM
   and photo-z training evaluate calibration by tile, depth and passband
   coverage. Compare against a flux-only model and a matched-aperture
   subset. Reserve secure, independent spectroscopic Zensus links for
   photo-z evaluation, and split spatially and by native source or
   exposure family to avoid leakage. For MSA, join XP by Gaia ID and use
   the model's missing-data, scaling and label contracts.
5. On CANFAR, run spatially adjacent tile ranges and measure ARC reads,
   output bytes, CPU, scratch use and peak RSS across the dense tail and
   catalogue edges. Order 7 and 64-tile batches are pilot values. Use
   smaller job ranges or a finer fixed output order if dense pools exceed
   the resource envelope, then rerun the spatial invariance check.
   Project columns from the mounted trees, and reuse astrometry-only
   candidates for the radius and posterior sweeps. Keep the versioned draft
   on ARC. Workers read the survey trees in place.
6. Pin the exact inventory, science mappings, algorithms and calibrations.
   Require complete tile coverage, row and member conservation, valid
   checksums, and an explicit disposition for restricted or unresolved
   products before publishing. Publish the native union and link evidence
   with their policy and mask versions.

A pilot candidates file can be produced from a pinned plan as follows.

```bash
pixi run python -m scripts.pensus.release candidates --draft "$PILOT" \
  --left gaia --right tmass --pixel 109033 --radius-arcsec 2.0 \
  --output /arc/projects/astroai/pensus/calibration/gaia-tmass-109033.parquet
```

The file carries the two native keys, positions used by the pilot, angular
separation and input-plan provenance. Rows with a null right key are the
unmatched primaries. Repeat for all relevant tiles and directions. Include
planned native fields with `--left-column` and `--right-column` and join
depth maps by their release-specific identifiers before fitting the production
score. `radius-summary` reuses the same file for multiple trial radii and
reports the unmatched, single-candidate and multiple-candidate populations.
