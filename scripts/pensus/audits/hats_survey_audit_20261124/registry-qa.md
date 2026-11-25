# CANFAR HATS registry comparison

QA ran on November 24, 2026 Vancouver time. CANFAR headless job
`vbcp6jmm` checked the mounted inputs for the PENSUS pilot and the
recently repaired releases. Job `xqt4o0xf` compared the live summaries
with metadata used by data.lsdb.io. The registry snapshot is pinned by
Git tree `c37fbd895b601d323357c61e6f2e5040b2dffc20`. The machine-readable
results and the job inputs are in
`/arc/projects/astroai/pensus/qa/hats-registry-20261124/`.
[LSDB catalogue metadata](https://github.com/astronomy-commons/data.lsdb.io/tree/main/data)

## CANFAR checks

The pass covered 30 catalogue roots. Of these, 29 passed and the PS1 OTMO
header was recorded provisionally while PS1 reduction was in progress.
Every leaf in the six stable repaired trees was checked against its footer
and aggregate metadata, including file names and row-group counts.
The job decoded coordinates and checked HEALPix ownership for every
modified Parquet file, plus up to five sampled leaves per stable tree.
It decoded all columns in each of the ten recovered ATLAS partitions and
checked that all restored GAaP `N_EPOCH` values were null. Altogether the
job inspected 18,308,146 coordinate rows and found no range, ownership,
or order-29 index discrepancies in those rows. Pilot projections were
checked against the available schemas and sampled leaves.

| Repaired catalogue | Rows verified from all leaf footers | HATS leaves |
| --- | ---: | ---: |
| ATLAS DR5 primary-source build | 150,198,243 | 385 |
| DELVE DR3 Gold | 2,588,267,795 | 6,477 |
| DES Y6 Gold historical copy | 691,483,608 | 1,633 |
| LSST DP1 object photo-z | 544,644 | 4 |
| UHS DR3 historical copy | 514,336,270 | 1,111 |
| UNIONS GAaP SP photo-z | 637,129,635 | 1,650 |

The live PS1 OTMO header advertised 10,560,724,292 rows, 27,161 leaves
and maximum order 7. This pass deferred the PS1 leaf and aggregate reads
while reduction was running. After publication, pin the resulting product's
row count, schema and partition inventory before starting the union.
[PS1 public Objects catalogue](https://github.com/astronomy-commons/data.lsdb.io/blob/main/data/Pan-STARRS/Pan-STARRS1_%28objects%29/catalog.json)

## Registry comparisons

The live comparison covered 23 paths with public reference entries.
Sixteen of these match on rows, leaf partitions and science columns.
They include 2MASS PSC, AllWISE, DELVE DR3 Gold, the two DES Y6 Gold paths,
eRASS3, the three Euclid DR1 MER products, the five HSC PDR3 broad-band
forced catalogues, and Rubin DP1 Object and DIA. The comparison subtracts the
spatial index from the physical Parquet column counts to match LSDB's
summary convention.
[LSDB catalogue browser](https://data.lsdb.io/)
[IRSA Euclid catalogue inventory](https://irsa.ipac.caltech.edu/euclid-archive-documentation/data-products/)

| Catalogue | Interpretation |
| --- | --- |
| Gaia DR3 | ARC, the current US-West served HATS header and LSDB's CDS entry agree with ESA's 1,811,709,771 sources. The US-West site summary still shows 1,812,731,847, which the older 2025 LineA mirror also advertises. [ESA Gaia DR3 content](https://cesar.esa.int/upload/202210/RG_Gaia_DataRelease3.pdf) [Current US-West HATS header](https://data.lsdb.io/hats/gaia_dr3/gaia/hats.properties) |
| Legacy DR11 | The mounted 3,967,618,454 rows equal the sum of the public North and South releases. The 9,222-leaf ARC tree has its own layout. Preserve the release-aware measurement IDs and overlap policy. |
| PS1 OTMO | The published Objects row count agrees with the provisional ARC total. The public build lists 9,577 leaves and uses a different builder. Compare the finished ARC summary against the exact product selected for the run. |
| SkyMapper DR4 | The public and ARC counts agree at 724,329,344. ARC has 356 leaves and 121 science fields. The public summary lists 23,629 leaves and 122 fields. |
| S-PLUS DR4 dual | The two summaries agree at 80,155,547 rows. The 218-leaf ARC projection has 34 science fields and the public build lists 971 leaves and 368 fields. Use the ARC schema when deciding which uncertainty and quality fields are available. |
| Rubin DP1 object photo-z | Both products have 544,644 rows. ARC has 4 leaves and 130 science fields. The current LSDB entry has 5 leaves and 67 fields. ARC carries point estimates and magnitudes, while the registered build documents nested PDF data. [Rubin DP1 photo-z entry](https://github.com/astronomy-commons/data.lsdb.io/blob/main/data/Rubin/DP1/object_photoz/catalog.json) |
| Euclid Q1 | ARC has 29,767,806 MER Final Catalog rows, the count used in the HATS specification example. The 29,953,430-row IRSA entry is a combined MER, PHZ and SPE product. [IVOA HATS specification](https://www.ivoa.net/documents/Notes/HATS/20250822/NOTE-hats-ivoa-1.0-20250822.html) [IRSA Q1 product description](https://caltech-ipac.github.io/irsa-tutorials/euclid-q1-hats-intro/) |
| ATLAS DR5 | The 5,073 staged FITS tiles contain all 163,755,167 ESO rows. Conversion retains 150,198,243 primary rows and excludes 13,556,924 secondary rows. Every live row group reports `PRIMARY_SOURCE == 1` with no nulls. The mounted release is the primary-source selection. [ESO VST ATLAS catalogue](https://www.eso.org/qi/catalog/show/438) |
| UHS DR3 | The ARC input is a WSA seamless `uhsSource` export using `priOrSec IN (0, frameSetID)`. Quote its 514,336,270 rows in this selection. The release paper's H-band detection count describes a different population. [UHS DR2 and DR3 release paper](https://www.research.ed.ac.uk/en/publications/the-second-and-third-data-releases-from-the-ukirt-hemisphere-surv/) |
| UNIONS GAaP SP photo-z | Use the local versioned build and verified footprint and schema as the reference for this collaboration product. |

GALEX MCAT, Hubble Source Catalog v3, and SDSS DR7 spectra are
different products from the mounted GUVcat AIS, Hubble v4, and SDSS
DR16 primary imaging sweeps. Their row counts are unsuitable for direct
mirror comparisons. Registry totals and the sampled live checks support
the present summaries within their stated product selections.
