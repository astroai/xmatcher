# Mounted catalogue roles

This is a code-side inventory from the CANFAR headless snapshot on
November 23, 2026. It records the intended treatment of the 62 observed
object tables. The live catalogue inventory and the schema preflight
remain inputs to each run. The pipeline checks the catalogue version,
layout and projected schema when planning and reading tiles.

## Pilot inputs

The [pilot manifest](../config/full-union-canfar-pilot.yaml) declares the
keys, original photometry and flags, initial search caps and verified
position-epoch fields for these products.

| Role | Mounted paths |
|---|---|
| Stellar reference | `gaia/dr3` |
| Optical and near-IR identity | `legacysurvey/dr11`, `ps1/otmo`, `des/y6_gold`, `delve/dr3_gold`, `skymapper/dr4`, `sdss/photoprimary_dr13`, `atlas/dr5`, `kids/dr5`, `clauds`, `splus/dr4`, `lsst/dp1/object_collection/object_lc` |
| Resolved near-IR identity | `2mass/psc`, `uhs/dr3`, `vhs/dr7`, `viking/dr5`, `video/dr6`, `vmc/dr7`, `vvvx/dr1`, and `ukidss/dr11_{las,gcs,dxs,uds}` |
| UV and mid-IR link evidence | `galex/guvcat_ais_gr67`, `catwise/2020`, `allwise/catalog` |

- PS1 mean positions have `epochMean` in MJD. Preserve the documented
  finite missing sentinel and the mean-position flags. Stack detections
  supply a distinct position product. [MAST PS1 mean-object fields](https://outerspace.stsci.edu/spaces/PANSTARRS/pages/298812411/PS1%2BMeanObjectView%2Btable%2Bfields)
- SkyMapper mean positions have `mean_epoch` in MJD.
  `e_raj2000` and `e_dej2000` are the scatter of observations about
  the mean, so a calibrated positional covariance needs additional work.
  [SkyMapper DR4 table browser](https://skymapper.anu.edu.au/table-browser-standalone/dr4/)
- 2MASS PSC `jdate` is the observation's Julian date. Use the
  catalogue's position ellipse and per-band read/confusion flags.
  [2MASS All-Sky documentation](https://www.ipac.caltech.edu/2mass/releases/allsky/faq.html)
- Gaia reference years apply to its catalogue coordinates. Treat the
  fit conventions and position epochs of CatWISE, AllWISE and Legacy
  independently. Preserve the reported astrometric quantities for
  calibration with their original units. [Gaia DR3 source model](https://gea.esac.esa.int/archive/documentation/GDR3/Gaia_archive/chap_datamodel/sec_dm_main_source_catalogue/ssec_dm_gaia_source.html)

## Assigned roles outside the pilot

| Mounted paths | Intended treatment |
|---|---|
| `des/y6_gold_prera_backup`, `uhs/dr3_prera_backup` | Historical copies. Reference one authoritative release per product. |
| `hsc/pdr3/forced_{g,r,i,z,y}`, `hsc/pdr3/forced_nb{0387,0816,0921,1010}` | Validate shared keys by product and release, then form one keyed HSC measurement view. Preserve the reference-band centroid and projected flags. Distinguish instrumental `instFlux` from calibrated flux. Narrow-band releases may need an explicit equivalence table. [HSC PDR3 FAQ](https://hsc-release.mtk.nao.ac.jp/doc/index.php/faq__pdr3/) |
| Three tables under `euclid/dr1/mer_catalogue_*`, `euclid/q1`, `unions/euclid_dr1_mer_catalogue_wide` | Reconcile deep, wide and external-photometry coverage through the documented native object key and release. Select VIS/Y/J/H native measurements with photometric flags. Confirm the position and extent associated with each native key before selecting the astrometric reference. Preserve the provenance of auxiliary bands. [Euclid DR1 MER data model](https://euclid.esac.esa.int/dr/dr1/dpdd/merdpd/dpcards/mer_finalcatalog.html) |
| `jplus/dr3`, `jpas/e2dr` | Pin the field and object key and the documented ordered passbands during conversion. Preserve the distinct aperture measurements, their uncertainty arrays and flag arrays. |
| `hubble/hsc_v4` | Keep `MatchID` and native filter, instrument and aperture definitions. Treat `NumImages` and `NumVisits` as sampling context. Use calibrated uncertainty-bearing measurements when available. [MAST Hubble Source Catalog v4](https://archive.stsci.edu/blog/2026/10-Oct/hubble-source-catalog-releases-v4-0.html) |
| `unions/cfis_DR6_{r,u}`, `unions/cfis_DR6_{r,u}_processed`, `unions/cfis_LSB_DR6_r`, `unions/gaap_ugriz_photoz_ext` | Select one native detection identity per release family and retain distinct aperture/low-surface-brightness measurements with provenance. Keep supplied photo-z information as evidence. Apply the collaboration access and release rules to any public product. |
| `unions/PGM`, `unions/ShapePipe/cut_struct_v1.6.6`, `unions/ShapePipe/psf_v1.6a` | Astrometric, structural or PSF context keyed to a native release. |
| `gaia/dr3_xp_continuous`, `pristine/dr2` | Join Gaia source IDs by release. Keep XP spectra and Pristine CaHK or metallicity provenance with the measurements. |
| `lsst/dp1/dia_object_collection/dia_object_lc`, `lsst/dp1/object_photoz` | Time-domain and redshift evidence. Check the release association table before linking the DIA and direct-imaging object namespaces. |
| `2mass/xsc` | Extended-source identity and geometry in the sampled mounted product. Add flux separately when a photometric XSC table is available. |
| `lotss/dr3`, `erosita/erass3` | Radio and X-ray measurement links. Use beam or component morphology and positional likelihoods. Retain energy-band flux and uncertainties in their native domain. [LoTSS DR3](https://www.lofar-surveys.org/dr3.html) [eRASS:3 data model](https://erosita.mpe.mpg.de/dr2/AllSkySurveyData_dr2/Catalogues_dr2/RamosM_DR2/eRASS3_Main_v1.3.html) |
| `zensus/20261019/zensus_20261019` | Redshift evidence. Use its versioned identifier and distinguish spectroscopic, photometric and inferred values. |

The [science policy](science-policy.md) defines the calibration plan that
uses these roles. The code team's test suite runs on synthetic data and
verifies the pipeline independently of the state of these mounted inputs.
