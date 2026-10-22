# Gaia-anchored master catalogue (`ray-anchor`)

`engine="ray-anchor"` builds a master catalogue that is driven by one
**anchor** catalogue, normally Gaia DR3. The output has exactly one row per
anchor source, including anchor sources with no counterpart. Every survey is
matched independently against the anchor; a survey cannot create or remove an
anchor row.

This is the workflow for "all measurements of every Gaia source", not a
full-outer union. Use [`ray-union`](usage.md#distributed-full-sky-union-ray-union)
when the master must retain sources that are absent from Gaia as well.

## Quick start

```bash
xmatcher sync gaia_hats sdss_hats ps1_hats catwise_hats
xmatcher match gaia_hats sdss_hats ps1_hats catwise_hats \
  --engine ray-anchor \
  -r 1.5 \
  --target-epoch 2016.0 \
  -o /arc/projects/hats/xmatcher/gaia_master.hats
```

The first catalogue is the anchor. Every later catalogue is a survey. Remote
inputs are mirrored into the HATS cache on demand; `--no-sync` requires an
existing cached copy.

For a large survey list, use a shell variable:

```bash
SURVEYS="sdss_hats ps1_hats catwise_hats vhs_hats galex_hats splus_hats"
xmatcher match gaia_hats $SURVEYS --engine ray-anchor -r 1.5 \
  -o gaia_master.hats
```

## Output schema

The anchor's selected columns are preserved: its whole schema, or the
`default_columns` listed in its catalogue entry. The anchor ID column is
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

Survey measurements keep their original names with a `_<survey>` suffix,
e.g. `objID_sdss_hats`, `upmag_sdss_hats`, `gmag_ps1_hats`. Only the columns
listed in the catalogue's `default_columns` are carried by default; a HATS
input passed without a configured projection carries its full schema.

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

## Epochs and proper motion

`--target-epoch` propagates both the anchor and any survey row that has epoch
and proper-motion metadata. The anchor's coordinate columns in the output hold
the propagated position, and `properties` records the epoch in `hats_epoch`
(`J2020.000` for `--target-epoch 2020`). Surveys without motion metadata stay
at their published coordinates; `pm_warn` still flags the anchor's motion over
the anchor-survey epoch baseline. Without `--target-epoch`, `hats_epoch`
records the anchor epoch when one is configured. For a reproducible run, set
`--target-epoch` to the anchor epoch (2016.0 for Gaia DR3) and keep
`--fallback-policy error`.

`pm_warn` uses the anchor proper motion when available. If the anchor has no
usable motion and the epoch baseline exceeds five years, every anchor row for
that survey is flagged rather than silently treated as stationary.

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
on a rerun. The fingerprint in `<out>/resume.state` covers the inputs, selected
columns, radius, target epoch, and anchor ID name; changing any of them into
the same output directory wipes the old dataset first.

The driver uses a running Ray cluster when `RAY_ADDRESS` is set, otherwise it
starts a local cluster. If Ray is not installed, the same partition loop runs
serially. The cache and output must be visible to every worker.

## CANFAR local products

The bundled configuration exposes local HATS aliases for products staged on
CANFAR. Point the environment variables at the HATS roots before resolving
the alias:

```bash
export XMATCHER_UHS_DR3_HATS=/arc/projects/<group>/uhs_dr3/hats
export XMATCHER_SPLUS_DR5_HATS=/arc/projects/<group>/splus_dr5/hats
export XMATCHER_GAIA_XP_HATS=/arc/projects/<group>/gaia_xp/hats
export XMATCHER_DP2_HATS=/arc/projects/<group>/dp2/hats
```

Use `uhs_hats`, `splus5_hats`, `xp_hats`, and `dp2` as catalogue names. If a
variable is unset, resolution fails with the variable named instead of
searching a fallback sky.

## Published pinned HATS sources

These products are not in the IVOA HATS registry but are available as pinned
CDS HATS trees and are bundled as aliases:

| Alias | Pinned tree | Rows |
|---|---|---:|
| `gaia_hats` | `https://vizcat.cds.unistra.fr/hats/gaia_dr3/main` | 1,811,709,771 |
| `sdss_hats` | `https://vizcat.cds.unistra.fr/hats/sdss_dr16/main` | 1,231,051,050 |
| `catwise_hats` | `https://vizcat.cds.unistra.fr/hats/catwise2020/main` | 1,890,715,640 |
| `vhs_hats` | `https://vizcat.cds.unistra.fr/hats/vhs_dr5/main` | 1,374,207,485 |
| `ps1_hats` | `https://vizcat.cds.unistra.fr/hats/ps1_dr2/main` | 1,868,236,315 |
| `galex_hats` | `https://vizcat.cds.unistra.fr/hats/galex_gr67/main` | 82,992,086 |
| `splus_hats` | `https://vizcat.cds.unistra.fr/hats/splus_dr4/main` | 27,235,968 |

SDSS DR20 contains no new or updated imaging data. The DR16 HATS is the
static SDSS imaging photometry product; use it rather than building a DR20
photometry tree.

## Limits

- The anchor must have a usable identifier column (`id_column` in the
  catalogue entry, `--id1`, or a recognised name such as `source_id`/`Source`
  in its HATS schema).
- The current engine supports `matcher="sky"` and keeps the best counterpart
  per survey. Use `ray-union` for `skyerr`/`skyellipse`.
- It always performs a left join from the anchor; `find="all"`, ID joins,
  probabilistic scores, filters, regions, and extra-distance columns are
  rejected rather than silently ignored.
- A single anchor partition whose survey candidate pool exceeds
  `--task-rows` or `--chunk-memory-gb` aborts with an actionable error; raise
  the limits or lower `--hats-threshold`.
