# xmatcher 0.7.0

Release notes for the 0.7.0 source tree, for tag `v0.7.0`. This release has
three parts:

- torchsky becomes xmatcher's matching engine.
- `xmatcher convert` converts catalogues between FITS, Parquet, HATS and CSV,
  in both directions.
- A release audit of correctness, HATS conformance, remote acquisition, the
  Ray engines and the install path is folded in.

The full list is in the [changelog](../../CHANGELOG.md).

**Prerequisite:** torchsky 0.6.0 must be on PyPI before this tag is built.
Then:

1. set `torchsky >= 0.6.0` in `pyproject.toml`;
2. replace the `pixi.toml` git pin with the PyPI requirement (and drop the
   build tools and `no-build-isolation`);
3. run `pixi lock`.

## Highlights

### torchsky is the engine

[torchsky](https://github.com/astroai/torchsky) (>=0.6.0) and torchfits are
now core dependencies:

- **`fast` and `zone`.** The default in-memory engine and `engine="zone"` take
  their candidates from torchsky's exact declination-zone index (`SkyIndex`,
  `match_within`, `match_nearest`). Separations are exact great-circle arcs,
  and ties go to the lowest right row by construction. The matcher filters
  and rankings are xmatcher's, and every engine-parity test passes as before.
- **Ray engines.** Ray pixel tasks and `ray-anchor` search with torchsky too.
  `ray-anchor` reports the best match, the second nearest and `n_within`
  under one inclusive radius rule.
- **Propagation and coverings.** Epoch propagation (proper motion and full
  space motion, with Jacobians for target-epoch covariances) and HATS
  margin/pruning coverings use torchsky only. Astropy is the tests' oracle.
- **`matcher="lr"`.** It is computed by `torchsky.catalogs.associate`. It is
  the Sutherland & Saunders likelihood ratio, with three changes:
  - q(m) is learnt by EM;
  - reliabilities are Bayesian posteriors with `lr_q` as the prior completeness;
  - candidates pass a χ² gate and the radius ceiling, and in crowded fields a
    right row is the counterpart of at most one left row.

  `lr_color_column` makes the photometric term a colour against the primary
  (e.g. W1 − G).
- **N-way association.** `nway_match`, and `matcher="lr"` with three or more
  catalogues, run `torchsky.catalogs.associate_many`. For each primary row,
  every combination of one counterpart or none per catalogue is a hypothesis.
  Hypotheses use full covariances, completeness, background density and
  magnitude or colour likelihood ratios learnt by EM. Partial associations
  are kept, and oversized primaries are enumerated best-first with a certified
  bound instead of truncated. New output columns: `sep_arcsec_<k>`, `lr`,
  `p_any`, `nway_best`.

For scale, torchsky's crossmatch kernel is 1.2–11.5× faster than scipy's
cKDTree from 10⁶ to 10⁸ rows on CPU. On a GPU it beats CuPy's KD-tree and
Open3D on sparse and dense fields at 10⁶–10⁷ rows (torchsky-harness
scorecards).

### `xmatcher convert`

```bash
xmatcher convert gaia_field.fits gaia_field.hats
xmatcher convert gaia_field.hats gaia_field.parquet --columns source_id,ra,dec
xmatcher convert survey.hats survey.fits
```

Formats come from the paths: `.fits`, `.parquet` or a Parquet directory,
`.hats` or a HATS directory, and `.csv`. Rows stream in batches.

- **HATS outputs** are written in three passes (count, spill, sorted write).
  They carry `_healpix_29`, pass `hats` strict validation, and read as plain
  Parquet datasets.
- **FITS outputs** are standard binary tables written in one pass.
- **Measured round trip:** 5 million rows Parquet → HATS → FITS → Parquet come
  back exactly. The HATS write takes 15 s at 1.3 GB peak memory; the FITS steps
  take 3–6 s at 0.3 GB.

### Release audit

These are the audit's fixes. Each has a regression test that fails without it.

- **Matching correctness.** The ray-union outer join no longer drops rows.
  `fof_match` keeps Gaia `source_id` intact. The likelihood-ratio density is
  correct. `skyellipse` works at the poles, and the radius ceiling applies on
  every engine.
- **HATS conformance.** Every tree xmatcher writes passes `hats` strict
  validation. Local and synced collections are matched.
- **Robust sync.** Trees listed only by CSV are no longer re-downloaded on every
  sync, and truncated downloads are rejected. Sync takes a lock per tree,
  streams HTTP downloads, and checks free space.
- **Round 2 of the audit (merged here).** Ray-union assembles each output pixel
  once. Association edge shards reject float IDs and verify canonical bytes.
  The CLI keeps `--help` light and rejects unknown keywords. Matcher inputs
  share one frame.

## Upgrade notes

- **Dependencies.** `pip install xmatcher` now pulls torchsky and PyTorch. The
  `[torchsky]` and `[torchfits]` extras are gone. Python stays at 3.13 or
  later; the proposed 3.11 support was dropped because torchsky ships wheels
  for 3.12 and 3.13 only.
- **Engine names.** `engine="zone"` no longer needs `cdshealpix` and never
  falls back.
- **Changed LR values.** `matcher="lr"` reliabilities and likelihood ratios
  change value, for the reasons listed above.
- **Not yet ported.** `nway_match` with photometric `prior_columns`, the Bayes
  and KDE `probabilistic` scoring, and the ML, AUF and macauff scorers keep
  their current statistics, applied to torchsky candidates. Porting the N-way
  photometric priors needs `terms=` in torchsky's `associate_many`.
