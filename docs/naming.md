# Catalogue naming

This page defines how catalogues are named in `xmatcher`: what a bare name
means, when to use a registry identifier instead, and how the current keys map
to that scheme. It is the reference for `xmatcher list --aliases`,
`xmatcher describe <name>`, and every `match` / `sync` input.

---

## Input forms

`xmatcher` resolves a catalogue argument in this order:

| Form | Example | Meaning |
|---|---|---|
| Local path | `targets.parquet`, `/data/survey.hats` | A file or local HATS directory. |
| Bare name or alias | `gaia`, `ps1`, `des` | A curated entry in `xmatcher.yaml` (bundled plus user overlay). |
| `vizier:<id>` | `vizier:II/349/ps1` | A VizieR catalogue/table id. |
| `tap:<endpoint>/<table>` | `tap:noirlab/nsc_dr2.object` | A table on a supported public TAP endpoint. |
| `hats:<url>` | `hats:https://example.org/hats/my_survey/` | A remote HATS tree over HTTP(S), VOSpace, S3 or Hugging Face (a tree or a collection root). |

Bare names are matched case-insensitively. Explicit identifiers avoid the two
places where a bare string is ambiguous:

- `ps1` is a curated alias, while `II/349/ps1` is the VizieR table id.
- `nsc` is a curated alias, while `nsc_dr2.object` is a Data Lab table id.

Supported `tap:` endpoints are the public endpoints listed by
`xmatcher discover --help`: `vizier`, `noirlab`, `gaia`, and `cadc`. An unknown
endpoint fails with the list of valid names.

> `hats:<name>` resolves through the IVOA HATS registry (cached for seven days)
> and `hats:<url>` accepts a single tree or an LSDB-style collection root, which
> is descended to its primary catalogue tree. The same survey still appears
> under several provider-qualified names (`gaia_dr3`, `gaia_dr3_stsci`,
> `gaia_dr3_uw`), so pin the provider when reproducibility matters. HTTPS,
> VOSpace, `s3://` and `hf://datasets/…` trees are read; an unsupported access
> URL is reported rather than silently substituted. The verified sources are
> listed in [hats-sources.md](hats-sources.md).

---

## Bare-name grammar

A curated catalogue key follows:

```text
<survey>[_<release>][_<product>][_<provider>]
```

- **survey** — the community acronym, lowercase ASCII: `gaia`, `sdss`, `des`,
  `nsc`, `ps1`, `twomass`. Use `[a-z0-9_]`, and start with a letter. Alias
  `2mass` is allowed because users type it, but the canonical key is
  `twomass_psc`.
- **release** — `dr1`, `dr2`, `dr3`, `edr3`, `y6`, `q1`, `erass1`, or the
  release year when there is no official token.
- **product** — only when the survey publishes several tables: `_psc`,
  `_ais`, `_object`, `_tractor`, `_source`, `_objects`. Crossmatch
  products use `<survey>_x_<survey>` (`nsc_x_gaia`, `des_x_gaia`).
- **provider** — only when the same product is available from more than one
  archive: `_cds`, `_esa`, `_noirlab`. Never encode the access mechanism
  (`tap`, `hats`, `vizier`) in a bare name; use the namespace prefix instead.

The bare name is a convenience alias. The canonical entry is the one under
`catalogues:`; aliases only redirect to it. An alias may point at another
alias (an overlay written before a rename keeps working), and resolution
follows the chain; a cycle is rejected at load time. A bare name must not be
the only record of a release, so consumers that need reproducibility should
print `xmatcher describe <name>` (which shows the release, table, and epoch) or
use the explicit `vizier:` / `tap:` identifier.

Provider and release belong together. `gaia` is a provider-neutral default;
`gaia_dr3_cds` is the VizieR copy; `gaia_esa` and `gaia_noirlab` are the
mirrors. Pinning a provider in a bare name without pinning the release is the
pattern to avoid — that is why the canonical keys below include the release.

---

## What is already standard

- **VizieR ids are canonical.** `I/355/gaiadr3`, `II/349/ps1`,
  `J/A+A/682/A34/erass1-m`. `vizier:<id>` is the right identifier when the
  exact table matters. The existing `access_identifier` values are the source
  of truth; `xmatcher list` deliberately does not ask users to paste them.
- **TAP table ids are canonical per endpoint.** Data Lab and ESA use
  `schema.table` (`nsc_dr2.object`, `gaiadr3.gaia_source`); VizieR uses slash
  ids. A table id is only unambiguous with its endpoint, which is why the
  `tap:<endpoint>/<table>` form exists.
- **HATS is a format, not a survey registry.** HATS trees are named by their
  location. IVOA HATS services register `short_name` values in the VO registry
  (LSDB does this for `two_mass`, `gaia_dr3_stsci`, …), but those names are
  provider-specific and the registry is not a stable catalogue namespace:
  `hats:<name>` is resolved for convenience and logged, while a repeated run
  should pin the tree URL. See [hats-sources.md](hats-sources.md).

---

## Migration (applied)

The keys below are canonical now. Every former key remains an alias, so
scripts and notebooks written before the rename keep resolving to the same
table with the same columns.

| Former key | Canonical key | Also reachable as |
|---|---|---|
| `gaia_cds` | `gaia_dr3_cds` | `gaia`, `gaia_dr3`, `gaiadr3` |
| `galex` | `galex_ais` | `galex_cds` |
| `vhs` | `vhs_dr5` | `vhs_cds` |
| `ps1` | `ps1_dr1` | `panstarrs`, `panstarrs1`, `ps1_cds` |
| `twomass` | `twomass_psc` | `2mass`, `twomass_cds` |
| `catwise` | `catwise_2020` | `catwise_cds` |
| `des` | `des_dr2` | `des_noao` |
| `nsc` | `nsc_dr2` | `nsc_noao` |
| `smash` | `smash_dr2` | `smash_noao` |
| `unwise` | `unwise_dr1` | `unwise_noao` |
| `desils` | `legacy_dr10` | `decals`, `decals_dr10`, `ls_dr10`, `desils_dr10`, `desils_noao` |
| `decals_objects` | `decals_dr10_object` | `decals_noao`, `ls_dr10_objects` |
| `gaia_noao` | `gaia_noirlab` | — |
| `ukidss` | `ukidss_las` | `ukidsslas`, `ukidsslas_noao` |
| `erosita` | `erosita_erass1` | `erosita_cds` |
| `xmm` | `xmm_dr13` | `xmm_cds` |

Notes:

- `decals` deliberately keeps resolving to the tractor table
  (`legacy_dr10`). Pointing it at the object table would silently drop
  photometry from existing matches; use `decals_objects` or
  `decals_dr10_object` when you want detections only.
- `nsc_x_gaia`, `des_x_gaia`, `decals_x_gaia`, `allwise_x_gaia`, and
  `ukidss_x_gaia` were **not** renamed: the `<survey>_x_<survey>` pattern is
  already consistent.
- `_noao` provider suffixes remain valid aliases but are legacy; the naming
  rule for new entries is `_noirlab` (`gaia_noirlab` is the only such key so
  far).
- `xmatcher describe <former key>` prints the canonical entry, the release, and
  the alias it resolved through, so results remain reproducible.

### Mirror-directory names change once

Mirrors live under the catalogue name
(`<cache>/<name>/<version>/`). After upgrading, a renamed catalogue syncs into
its new directory once; the old tree is not deleted or reused. If you keep a
large mirror on a tight disk, either keep using the former alias until you are
ready to re-sync, or move the directory
(`mv <cache>/gaia_cds <cache>/gaia_dr3_cds`) before the first sync.

---

## Rules for new entries

1. Add the canonical key under `catalogues:`; add at least one short alias
   under `catalogue_aliases:` when the survey is commonly typed.
2. Include `release`, `access_identifier`, `table_name`, `ra_column`,
   `dec_column`, and either measured error columns or an explicit
   `default_pos_error_arcsec`. Do not leave an entry that cannot run `skyerr`.
3. Use `vizier:` / `tap:` in examples and tickets when the exact table matters;
   use the bare name in tutorials and exploratory work.
4. A new HATS mirror is a URL (or a `fallback:`), not a new survey name. Only
   add a `hats:<name>` key after registry lookup is implemented.
5. Run `xmatcher doctor` after editing the user overlay; a renamed field is
   reported as structural drift.
