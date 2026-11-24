# CANFAR HATS survey audit

Source snapshot from `sfabbro/masked-stellar-autoencoder` at
`/arc/projects/astroai/msa/runs/hats_survey_audit_20261124/`. The original ARC
snapshot is retained. The imported JSON is unchanged. The archive contains the
original per-partition CSVs and sample JSON. Extract it alongside this file
before verifying all of the original files with `sha256sum -c SHA256SUMS`.

`summary.json` covers 74 catalogue roots, including indexes and historical
copies. Read `structure/` for layout and footer evidence, `fullscan/` for
coordinate and page-read checks and `followup/followup.json` for checks across
specific catalogues. Some audit probes are samples. The recorded coverage
fields determine which counts can be interpreted as full catalogue totals.

## Physical repairs

| Catalogue | Evidence and repair |
| --- | --- |
| ATLAS DR5 | Eight partitions had corrupt pages and two had zero bytes. The original aggregate described 150,198,243 rows, and the readable footers covered 149,445,334. Rebuild from the original ESO conversion shards and replace only the affected files. Match the original partition tree, schema and each original partition count, and fully decode each replacement. |
| DELVE DR3 Gold | Wrap eight exact 360° RA values across seven partitions. |
| PS1 OTMO | Wrap eight out-of-range RAs, each within a few millionths of a degree of 0° or 360°. |
| DES Y6 Gold and UHS DR3 pre-RA backups | Two DES partitions and one UHS partition show endpoint coordinates. The primary paths were already corrected when audited. The historical trees receive the same bounded repair while retaining their identifiers and original files in the repair receipt. |
| LSST DP1 object photo-z | Derive the absent `hats_order` from `partition_info.csv` and update both properties spellings. |
| UNIONS GAaP SP photo-z | The 1,650 partitions contain 637,129,635 rows. The tree has no properties and its aggregate has zero row groups. After verifying both reported coordinate fields and their HEALPix keys, supply null `N_EPOCH` for the 20 partitions that lack it, regenerate the metadata and create truthful recovery provenance. |

Coordinate corrections keep row counts and order, the existing spatial index,
and the original partition boundary. The repair tool rejects anomalies outside
the audited range. The aggregate metadata is rebuilt from the physical
replacement and unchanged partition footers. It records staged hashes and
copies the original files before replacing live files. Run repairs when
consumers of the affected catalogue can refresh their cached metadata.

Gaia XP and three Euclid MER object tables have usable coordinate and layout
metadata. Their upstream properties omit a HATS version, and Euclid omits
creation dates. These are provenance gaps. A fabricated builder version or
timestamp would be misleading. The index rows in the generic summary are
false positives for object coordinates and HEALPix order. These tables have
`dataproduct_type=index` and store their Parquet data under `dataset/index`.
Read them as indexes, using their own metadata and the parent association.
[IVOA HATS v1.0](https://www.ivoa.net/documents/Notes/HATS/20250822/NOTE-hats-ivoa-1.0-20250822.pdf)

## Measurement and association decisions

| Finding | PENSUS treatment |
| --- | --- |
| Legacy DR11 mixed releases | In 400 mixed-release partitions, 21.09% of rows had another-release neighbour within 0.1″. Keep `RELEASE,BRICKID,OBJID` as the measurement key, plus `PHOTSYS` and flags. Treat close cross-release observations as candidate associations and retain their filter system. A global north/south rule would also remove measured sky coverage. [Legacy Survey release mapping](https://b.legacysurvey.org/release/) |
| PS1 OTMO low detection counts | 75.43% had at most one single-epoch detection. Keep the count and per-band counts, stack count and existing quality flags. Evaluate stack-only, single-epoch and repeated cohorts independently when calibrating precision and training masks. [MAST PS1 archive data products](https://outerspace.stsci.edu/spaces/PANSTARRS/pages/298812394/PS1%2BDatabase%2Bobject%2Band%2Bdetection%2Btables) |
| SDSS repeated IDs | The mounted `id` equals `THING_ID` and has three repeated values. Use the native imaging tuple `RERUN,RUN,CAMCOL,FIELD,ID`, preserve `id` as an association key, and use a neutral mounted-product label while its upstream release label remains ambiguous. The audit verified uniqueness for `RUN,CAMCOL,FIELD,ID`. [SDSS imaging release caveats](https://www.sdss4.org/dr17/imaging/caveats/) |
| Pristine repeated Gaia keys | 58,177 Gaia keys have repeat measurements that frequently differ in CaHK. A Gaia join needs a one-to-many measurement link or a separately defined quality-selected view. Preserve the CaHK uncertainties and CASU flags. |
| S-PLUS field overlaps | 9.61% of rows have a different-field neighbour within 0.5″. Preserve field and flux provenance, require independent position and photometry checks for a common-source link, and keep real same-sky repeats. [S-PLUS DR4](https://splus.cloud/documentation/DR4) |
| GALEX calibrated vs instrumental magnitudes | The measured AUTO offsets are 18.82 mag in FUV and 20.08 mag in NUV. The pilot uses calibrated microjansky flux and its uncertainty, masks the `-999` sentinel and preserves calibrated AB magnitude columns. [GALEX data dictionary](https://www.galex.caltech.edu/wiki/Public%3ADocumentation/Appendix_A.1) |
| Euclid MER wide quality | Of 888,152,165 rows, 199,125,435 are NIR-detected, 55,966,502 have the spurious flag, and 160,144,348 have nonzero detection quality. A stringent VIS selection is an opt-in science view. Retain detection band, per-band and detection quality flags and source provenance for SED and photo-z. [Euclid MER photometry cookbook](https://euclid.esac.esa.int/dr/dr1/dpdd/merdpd/merphotometrycookbook.html) |
| Sampled Euclid deep and LoTSS names | The sample reports repeated native IDs or names. Measure their full repetition rates and determine a patch or component key before using those products as identity inputs. Keep candidates and original measurements. |

## Running and recovering

Run on CANFAR headless with PyArrow, NumPy, Astropy and cdshealpix available.
The repair script stages by default. Use a new directory under
`/arc/projects/astroai/pensus/repairs/` for each invocation. Pass `--apply`
to publish a preflight-validated repair. Each run has a `plan.json` followed
by a `result.json` or `error.json`. The same run path with
`--rollback` restores the verified backups when a live file still matches the
applied repair. Keep run directories for provenance and rollback.

For ATLAS, run `arc/rebuild_atlas_audit_20261124.sh` from
`scripts/pensus` and use the path recorded in its `source.path` as
`--atlas-source`. The rebuild writes its temporary data to `/scratch`
and its candidate to the supplied project directory.
