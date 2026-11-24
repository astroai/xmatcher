# Anchor-centric crossmatching (`ray-anchor`)

`engine="ray-anchor"` builds a master catalogue that is driven by one
**anchor** catalogue (such as Gaia DR3 or a primary target list). The output has
exactly one row per anchor source, including anchor sources with no counterpart.
Every auxiliary catalogue is matched independently against the anchor; auxiliary
surveys cannot create or remove an anchor row.

This is the workflow for "all measurements of every anchor source", not a
full-outer union. Use [`ray-union`](usage.md#distributed-full-sky-union-ray-union)
when the master must retain sources that are absent from the anchor as well.

## Quick start

```bash
xmatcher sync gaia_hats sdss_hats ps1_hats catwise_hats
xmatcher match gaia_hats sdss_hats ps1_hats catwise_hats \
  --engine ray-anchor \
  -r 1.5 \
  --target-epoch 2016.0 \
  -o /arc/projects/astroai/xmatcher/gaia_master.hats
```

The first catalogue is the anchor. Every later catalogue is a survey. Remote
inputs (TAP tables or remote HATS trees) are mirrored into the HATS cache
(`--cache-root`) on demand; `--no-sync` requires an existing cached copy. An
output path (`-o`) is required and must be a local directory that does not
overlap any input.

For a large survey list, use a shell variable:

```bash
SURVEYS="sdss_hats ps1_hats catwise_hats vhs_hats galex_hats splus_hats"
xmatcher match gaia_hats $SURVEYS --engine ray-anchor -r 1.5 \
  -o gaia_master.hats
```

## Output schema

The anchor's selected columns are preserved: its whole schema, or the
`default_columns` listed in its catalogue entry plus the configured position,
identifier, error and epoch/motion columns. The anchor ID column is
renamed to `source_id` by default (`--anchor-id-name` changes it). An anchor
whose schema has no column that can serve as an identifier is rejected, rather
than producing a master that cannot be keyed. Each survey contributes:

| Column | Meaning |
|---|---|
| `<survey>_sep_arcsec` | Separation of the best counterpart, arcsec; null when unmatched |
| `<survey>_sep_second` | Separation of the second nearest counterpart inside the radius; null when fewer than two |
| `<survey>_n_within` | Number of survey sources inside the radius |
| `<survey>_crowded` | `n_within > 1` |
| `<survey>_pm_warn` | Proper-motion/epoch warning (see below) |
| `<survey>_quality` | `good`, `poor`, `ambiguous`, `pm_warn`, `ambiguous+pm_warn`, or `no_match` |

`<survey>` is the resolved catalogue name, lower-cased with every run of
non-alphanumeric characters replaced by `_` (a repeated name gets `_2`,
`_3`, ...). An alias resolves to its catalogue key first, so `sdss_hats`
produces the tag `sdss_dr16_hats` and `ps1_hats` produces `ps1_dr2_hats`.

Survey measurements keep their original names with a `_<survey>` suffix,
e.g. `objID_sdss_dr16_hats`, `upmag_sdss_dr16_hats`, `gmag_ps1_dr2_hats`.
Only the columns listed in the catalogue's `default_columns` are carried by
default, plus the configured position, identifier, error and epoch/motion
columns; a HATS input passed without a configured projection carries its
full schema.

`quality` is policy, not a calibrated probability:

- `good`: exactly one candidate, best separation at most 80% of the radius,
  no PM warning.
- `poor`: exactly one candidate but the best separation is beyond 80% of the
  radius.
- `ambiguous`: more than one candidate inside the radius.
- `pm_warn`: the anchor's proper motion (or a long epoch baseline with no
  usable motion) is large enough to move it by more than half the radius.
- `ambiguous+pm_warn`: both conditions.
- `no_match`: no survey source inside the radius.

Labels are applied in that order: a crowded match with a PM warning is
`ambiguous+pm_warn`, and a single `poor` match that also has a PM warning is
reported as `pm_warn`. Use the individual boolean/count columns when a
consumer needs the conditions separately.

## Candidate search

For each anchor partition, the plan selects the survey partitions whose own
cells overlap a cone around the anchor cell's centre. The cone radius is that
anchor cell's centre-to-boundary radius plus the match radius plus a motion
halo (0 without `target_epoch`; 600″ with one, split between anchor and survey
motion; a larger propagated move is an error). The cone depends only on the
anchor partition, so a survey with order-0 partitions no longer widens every
task to the whole sky: at order 2 the cone is about 10° plus the radius.

Survey rows with a non-finite coordinate or `|dec| > 90` are dropped before
matching instead of aborting the task; the count per survey is written to the
output `properties` as `xmatcher_invalid_survey_rows` (and per partition to
`resume.state`). Invalid anchor rows still abort, because every anchor row
must appear in the master.

Within those rows, the search uses
torchsky's exact zone index (`torchsky.catalogs.SkyIndex`): one query returns
the two nearest survey sources and the count inside the radius, under a single
inclusive rule (separation ≤ radius), with ties going to the lowest survey
row. `sep_arcsec`, `sep_second` and `n_within` therefore agree with each
other. Torchsky (`>=0.6.0`) is a core dependency of `xmatcher`.

## Epochs and proper motion

`--target-epoch` propagates both the anchor and any survey row that has epoch
and proper-motion metadata. The anchor's coordinate columns in the output hold
the propagated position, and `properties` records the epoch in `hats_epoch`
(`J2020.000` for `--target-epoch 2020`). Surveys without motion metadata stay
at their published coordinates; `pm_warn` still flags the anchor's motion over
the anchor-survey epoch baseline. Without `--target-epoch`, `hats_epoch`
records the anchor epoch when one is configured, and `J2000` otherwise. For a
reproducible run, set `--target-epoch` to the anchor epoch (2016.0 for Gaia
DR3) and use `--fallback-policy error`.

If propagation moves a source further than the candidate halo,
0.5 × (match radius + largest partition diagonal), the run stops with an error
that names the catalogue, rather than missing pairs the partition plan never
read. This usually indicates wrong proper-motion columns or units.

`pm_warn` compares the anchor epoch (`--target-epoch`, else the anchor's
configured epoch) with the survey's configured epoch; it is false when either
is unknown or they are equal. A row with a usable anchor proper motion is
flagged when its drift over the baseline exceeds half the radius. A row
without usable motion (a null proper motion, or an anchor with no
proper-motion columns) is flagged when the baseline exceeds five years, rather
than being treated as stationary.

## Gaia XP sidecar

XP coefficients are deliberately **not** merged into the master row. They are
large arrays, and Gaia DR4 will provide XP for all sources; keeping XP in a
versioned sidecar avoids rewriting the master when the XP release changes.
Keep the sidecar keyed by Gaia `source_id`:

```bash
xmatcher project-hats xp_hats gaia_xp_dr3.hats \
  --columns source_id,bp_coefficients,bp_coefficient_errors,rp_coefficients,rp_coefficient_errors
```

If the XP product is stored as Parquet rather than HATS, point
`XMATCHER_GAIA_XP_HATS` at the HATS root produced by the conversion, then use
`xp_hats` as the sidecar name. Join the sidecar to the master on `source_id`
with `xmatcher match ... --id-join` or with Polars/Arrow. The CDS Gaia HATS
does **not** carry XP coefficients; only the ESA table or a staged CANFAR copy
does.

## HATS projection

Wide HATS trees (Gaia DR3 has 225 columns) can be narrowed before a run:

```bash
xmatcher project-hats gaia_hats gaia_core.hats \
  --columns Source,RA_ICRS,DE_ICRS,Plx,e_Plx,pmRA,e_pmRA,pmDE,e_pmDE,RUWE,Gmag,BPmag,RPmag,BP-RP

xmatcher project-hats sdss_hats sdss_phot.hats \
  --columns objID,RA_ICRS,DE_ICRS,upmag,gpmag,rpmag,ipmag,zpmag,uMJD,gMJD,rMJD,iMJD,zMJD
```

`project-hats` preserves the partition layout and row counts — including
zero-row partitions — writes a valid HATS `properties`/`partition_info`, and
refuses output that overlaps its input or already contains a tree, unless
`--resume` is given. Resume requires the `project.state` fingerprint written
by the interrupted run (same source and columns), skips partitions that are
already on disk, and completes the metadata; a mismatched or missing state is
an error rather than a silently mixed tree. An HTTPS HATS tree is streamed
partition by partition without mirroring the wide tree first (CDS accepts up
to eight column names per `?columns=` request); other remote sources are
mirrored unless `--no-sync` is given, in which case a cached copy must already
exist. A multi-TB projection can be resumed with `--resume`;
the `project.state` fingerprint binds the source, columns, and layout, and
completed partitions are skipped. Coordinate keys (`hats_col_ra`/`hats_col_dec`)
are dropped from the output `properties` when the projection does not carry
those columns.

## Resume and scheduling

Each anchor partition is one output partition under
`<out>/dataset/Norder=…/Dir=…/Npix=…parquet`. A completed partition is skipped
on a rerun. The fingerprint in `<out>/resume.state` covers the inputs (roots,
partitions and input files), selected and output columns, radius, target and
anchor epochs, the epoch and proper-motion metadata, the fallback policy, and
the anchor ID name; changing any of them into the same output directory wipes
the old dataset first. An output directory that already holds a dataset but no
valid `resume.state` is refused.

The driver joins the cluster at `RAY_ADDRESS` when it is set; if that
connection fails it starts a local cluster with a warning, or stops under
`--fallback-policy error`. Without `RAY_ADDRESS` it starts a private local
cluster, and it reuses a Ray session already initialised in the same process.
If Ray is not installed (it is in the `ray` and `hats-ray` extras), the same
partition loop runs serially. The cache and output must be visible to every
worker.

## CANFAR local products

On CANFAR, inputs whose trees are in the community root `/arc/projects/hats`
are read there in place (see
[HATS sources](hats-sources.md#community-trees-on-canfar)); only the other
remote inputs are mirrored into the cache root `/arc/projects/astroai/hats`.
For example, in

```bash
xmatcher match gaia_lsdb_hats twomass_hats vhs_dr7 ps1_otmo \
  --engine ray-anchor -r 1.5 -o msa_master.hats
```

`gaia_lsdb_hats` reads `/arc/projects/hats/gaia/dr3` (`local_paths`),
`twomass_hats` reads `/arc/projects/hats/2mass/psc` (its `PENSUS_SYNC.json`
matches the LSDB URL and version), and `vhs_dr7` / `ps1_otmo` read their
community trees; nothing is copied. The entries' `default_columns` keep the
master narrow. The log names the copy used for each input.

The bundled configuration also exposes local HATS aliases for products staged on
CANFAR (the full list of names and variables is in
[HATS sources](hats-sources.md#cds-pinned-hats-products)). Point the
environment variables at the HATS roots before resolving the alias:

```bash
export XMATCHER_UHS_DR3_HATS=/arc/projects/<group>/uhs_dr3/hats
export XMATCHER_SPLUS_DR5_HATS=/arc/projects/<group>/splus_dr5/hats
export XMATCHER_GAIA_XP_HATS=/arc/projects/<group>/gaia_xp/hats
export XMATCHER_DP2_HATS=/arc/projects/<group>/dp2/hats
```

Use `uhs_hats`, `splus5_hats`, `xp_hats`, and `dp2` as catalogue names.

## Published pinned HATS sources

The survey aliases used above (`gaia_hats`, `sdss_hats`, `ps1_hats`,
`catwise_hats`, `vhs_hats`, `galex_hats`, `splus_hats`) are pinned CDS HATS
trees that are not in the IVOA registry; their URLs, row counts and notes,
including why SDSS DR16 is the photometry source, are in
[HATS sources](hats-sources.md#cds-pinned-hats-products).

## Limits

- The anchor must have a usable identifier column (`id_column` in the
  catalogue entry, `--id1`, or a recognised name such as `source_id`/`Source`
  in its HATS schema).
- The current engine supports `matcher="sky"` and keeps the best counterpart
  per survey. Use `ray-union` for `skyerr`/`skyellipse`.
- It always performs a left join from the anchor. `--join` accepts only
  `1and2`, `1or2` or `all`, all of which mean that left join.
- These options are rejected rather than silently ignored: `--find all`,
  `--id-join`, `--probabilistic`, `--priors`, `--filter-expr`,
  `--extra-distance-cols`, `--columns-1`/`--columns-2`,
  `--memory-budget-bytes`, `--pm-prior`, and a region
  (`--ra`/`--dec`/`--radius-deg`).
- The output is a local HATS directory; a `vos:` output is refused (stage it
  with `xmatcher sync` afterwards).
- A single anchor partition whose survey candidate pool exceeds
  `--task-rows` or `--chunk-memory-gb` aborts with an actionable error; raise
  the limits or lower `--hats-threshold`.
