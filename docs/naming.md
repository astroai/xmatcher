# Catalogue naming

This page defines how catalogues are named in `xmatcher`: what a bare name
means, when to use an explicit identifier instead, and how the curated keys
follow that scheme. It is the reference for `xmatcher list --aliases`,
`xmatcher describe <name>`, and every `match` / `sync` input.

---

## Input forms

`xmatcher` (`CrossMatch.resolve_source`) resolves a catalogue argument in
this order; the first form that applies wins:

| Form | Example | Meaning |
|---|---|---|
| Polars frame (Python API only) | `pl.DataFrame(...)` | An in-memory catalogue. |
| `vizier:<id>` | `vizier:II/349/ps1` | A VizieR table id. A curated CDS entry with the same `access_identifier` is used when one exists, otherwise the table is described from TAP_SCHEMA. |
| `tap:<endpoint>/<table>` | `tap:noirlab/nsc_dr2.object` | A table on a supported public TAP endpoint, with the same curated-entry check against that endpoint's archive. |
| `hats:<url>` | `hats:https://example.org/hats/my_survey/` | A remote HATS tree over HTTP(S), VOSpace (`vos:`), S3 (`s3://`) or Hugging Face (`hf://datasets/…`), either a single tree or an LSDB-style collection root. |
| `hats:<dir>` | `hats:/data/survey.hats` | A local HATS directory. |
| `hats:<name>` | `hats:two_mass` | A `short_name` (or resource title) in the IVOA HATS registry. |
| Bare name or alias | `gaia`, `ps1`, `des` | A curated entry in `xmatcher.yaml` (bundled plus user overlay). |
| Remote HATS URI | `https://…`, `vos:…`, `s3://…`, `hf://datasets/…` | Same as `hats:<url>`. |
| Local path | `/data/survey.hats`, `targets.parquet` | A local HATS directory, or a local file. A missing path that starts with `/`, `./`, `../` or `~`, or has a catalogue suffix, is an error. |
| Curated table id | `II/349/ps1`, `nsc_dr2.object` | The curated entry whose `access_identifier` or `table_name` matches. |
| Other table id | `I/350/gaiaedr3`, `ls_dr9.tractor` | Described from TAP_SCHEMA on the endpoint guessed from the id form, else the `endpoint` override or `CrossMatch(endpoint=...)`. |

Bare names and aliases are matched case-insensitively. Because a bare name
is checked before the filesystem, a local file whose name equals a
catalogue key or alias needs a path prefix (`./gaia`). Explicit identifiers
avoid the two places where a bare string is ambiguous:

- `ps1` is a curated alias, while `II/349/ps1` is the VizieR table id.
- `nsc` is a curated alias, while `nsc_dr2.object` is a Data Lab table id.

Supported `tap:` endpoints are the public endpoints listed by
`xmatcher discover --help`: `vizier`, `noirlab`, `gaia`, and `cadc`. An
unknown endpoint fails with the list of valid names.

> `hats:<name>` lookups use a registry snapshot cached for seven days, and a
> name that resolved once keeps working when the registry is unreachable. A
> collection root is descended to its primary catalogue tree. The same survey
> still appears under several provider-qualified registry names
> (`gaia_dr3`, `gaia_dr3_stsci`, `gaia_dr3_uw`), so pin the tree URL when
> reproducibility matters. An unsupported access URL is reported rather than
> silently substituted. The verified sources are listed in
> [hats-sources.md](hats-sources.md).

---

## Bare-name grammar

A curated catalogue key follows:

```text
<survey>[_<release>][_<product>][_<provider>]
```

- **survey**: the community acronym, lowercase ASCII: `gaia`, `sdss`, `des`,
  `nsc`, `ps1`, `twomass`, `legacy`. Keys use `[a-z0-9_]` and start with a
  letter. Aliases may break this when users type the other spelling:
  `2mass` resolves to `twomass_psc`, `cosmos-web` to `cosmos_web`.
- **release**: the survey's release token (`dr1`, `dr2`, `dr3`, `dr5`,
  `dr10`, `dr13`, `erass1`), or the release year when there is no official
  token (`catwise_2020`).
- **product**: only when the survey publishes several tables: `_psc`
  (`twomass_psc`), `_ais` (`galex_ais`), `_las` / `_gps` (`ukidss_las`,
  `ukidss_gps`), `_object` (`decals_dr10_object`). Crossmatch products use
  `<survey>_x_<survey>` (`nsc_x_gaia`, `des_x_gaia`).
- **provider**: only when the same product is available from more than one
  archive: `_cds`, `_esa`, `_noirlab` (`gaia_dr3_cds`, `gaia_esa`,
  `gaia_noirlab`).

The access mechanism (`tap`, `vizier`) is not part of a bare name; use the
namespace prefix instead. The one exception is `_hats`: curated HATS trees
are separate entries with their own column layout and carry a `_hats` suffix
after the release (`gaia_dr3_hats`, `ps1_dr2_hats`, `sdss_dr16_hats`).

Keys added before this grammar do not all carry a release (`allwise`,
`sdss`, `skymapper`, `rosat`, `gaia_esa`, `gaia_noirlab`) or use a different
token order (`catwise2020_hats`). Their `release` field still records the
release; `xmatcher describe <name>` prints it.

The bare name is a convenience. The canonical entry is the key under
`catalogues:`; aliases under `catalogue_aliases:` only redirect to it. An
alias may point at another alias (an overlay written before a rename keeps
working), and resolution follows the chain. Loading the registry fails with
`ConfigError` on an alias cycle, an alias chain that does not end at a
catalogue key, an alias that shadows a catalogue key, or two aliases that
differ only by case. A bare name must not be the only record of a release,
so consumers that need reproducibility should print
`xmatcher describe <name>` (which shows the release, table, and epoch) or use
the explicit `vizier:` / `tap:` identifier.

Provider and release belong together. `gaia` resolves to the VizieR copy
`gaia_dr3_cds`; `gaia_esa` (ESA Gaia archive) and `gaia_noirlab` (Astro Data
Lab) are the same DR3 table from other archives. Those two keys pin a
provider without pinning the release, which is the pattern to avoid in new
keys; the migrated keys below include the release.

---

## What is already standard

- **VizieR ids are canonical.** `I/355/gaiadr3`, `II/349/ps1`,
  `J/A+A/682/A34/erass1-m`. `vizier:<id>` is the right identifier when the
  exact table matters. The `access_identifier` values in the registry are the
  source of truth; `xmatcher list` deliberately does not ask users to paste
  them, although a pasted id does resolve to its curated entry.
- **TAP table ids are canonical per endpoint.** Data Lab and ESA use
  `schema.table` (`nsc_dr2.object`, `gaiadr3.gaia_source`); VizieR uses slash
  ids. A table id is only unambiguous with its endpoint, which is why the
  `tap:<endpoint>/<table>` form exists.
- **HATS is a format, not a survey registry.** HATS trees are named by their
  location. IVOA HATS services register `short_name` values in the VO
  registry (LSDB does this for `two_mass`, `gaia_dr3_stsci`, …), but those
  names are provider-specific and the registry is not a stable catalogue
  namespace: `hats:<name>` is resolved for convenience and logged, while a
  repeated run should pin the tree URL. See [hats-sources.md](hats-sources.md).

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
- `_noao` provider suffixes remain valid aliases but are legacy; new entries
  use `_noirlab` (`gaia_noirlab` is the only such key so far).
- `xmatcher describe <former key>` prints the canonical entry, the alias it
  resolved through, the release, and the table, so results remain
  reproducible.

### Mirror-directory names change once

Mirrors live under the canonical key (`<cache>/<name>/<version>/`),
whichever alias you type. After upgrading, a renamed catalogue syncs into its
new directory once; the old tree is not deleted or reused. To reuse a large
existing mirror, move the directory before the first sync
(`mv <cache>/gaia_cds <cache>/gaia_dr3_cds`).

---

## Rules for new entries

1. Add the canonical key under `catalogues:`; add at least one short alias
   under `catalogue_aliases:` when the survey is commonly typed.
2. Include `release`, `access_identifier`, `ra_column`, `dec_column`, and
   either measured error columns or an explicit `default_pos_error_arcsec`;
   TAP entries also need `table_name`. Do not leave an entry that cannot run
   `skyerr`.
3. Use `vizier:` / `tap:` in examples and tickets when the exact table
   matters; use the bare name in tutorials and exploratory work.
4. A HATS copy of an existing table with the same columns is a `fallback:`
   on that entry, not a new name (a fallback whose `default_columns` differ
   from the primary's is rejected). A HATS tree with its own columns gets a
   `<survey>_<release>_hats` key. A tree that the IVOA registry already names
   needs no key: use `hats:<name>`.
5. Run `xmatcher doctor` after editing the user overlay; an entry that
   differs from the bundled one in a structural field (`ra_column`,
   `default_columns`, …) is reported as outdated.
