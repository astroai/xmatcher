# Pensus product spec (S-SPEC / S-COLS / D-PS1)

## Product

Source-preserving UV→IR multi-survey photometry compilation for photo-z,
stellar parameters (incl. Pristine metallicity), and QSO/AGN work.

- Measurements live as `_<survey_tag>` columns — **no silent flux averaging**.
- **No triple-counting** for science defaults / identity order; still retain
  each survey’s fluxes under tags.
- **Zensus** = evidence/conflict consumer; pensus does not absorb that ontology.
- **Observation releases** (normalized SHA photometry): **deferred past v0.1**.
  v0.1 uses native IDs + namespaces + projected columns.
- Immutable versioned views under `/arc/projects/hats/pensus/views/vN/`.

## Optical backbone

| View | Backbone |
|------|----------|
| v0.1 | **PS1 (N) + SkyMapper DR4 (S)** interim |
| v0.2 | **Legacy Tractor DR11** (DR10.1 optional early pin → replace drill) |

## PS1 product choice (D-PS1)

**Decision (v0):** prefer **Pan-STARRS1 mean object** photometry when the CDS
or local HATS pin is available; fall back to the LSDB/PS1 DR1 HATS alias
(`ps1_dr1` / `panstarrs`) already registered in xmatcher until the mean
product tree is pinned under k-pop `inputs/ps1/<rel>/`.

Rationale: mean-object fluxes are the usual photo-z / stellar-parameter input;
forced/detection-epoch products stay out of v0.1 defaults.

## Column sheets (v0.1 minimum)

Common identity / geometry:

| Column | Meaning |
|--------|---------|
| `component_id` | Deterministic pensus component id |
| `spine_release` | Cascade level spine namespace |
| `spine_id` | Native id in spine release |
| `ra`, `dec` | Spine (or cascade spine) ICRS degrees |
| `healpix_nest_oN` | Tile ownership pixel |

Per-survey photometry (examples; full sheets live beside projected inputs):

| Tag | Key photometry columns |
|-----|------------------------|
| `_gaia` | `phot_g_mean_mag`, `phot_bp_mean_mag`, `phot_rp_mean_mag` + errors |
| `_ps1` | `g_mean_psf_mag`…`y_mean_psf_mag` + errors (or DR1 aliases) |
| `_skymapper` | `u_psf`…`z_psf` + `e_*` |
| `_catwise` | `w1mpro`, `w2mpro` + errors |
| `_2mass` | `j_m`, `h_m`, `k_m` + errors |
| `_galex` | `fuv_mag`, `nuv_mag` + errors (attachment) |
| `_pristine` | `[Fe/H]` (+ flags) via **ID-join**, not positional |

Sidecars (separate trees under `sidecars/`): Gaia GSPC, Gaia XP coefficients,
Pristine Fe/H table keyed by `source_id`.

## Validation invariants

1. Every input row appears in exactly one component at its first eligible cascade level.
2. No member in two components.
3. Every finished tile has a valid `{rows,bytes,sha256}` marker.
4. Resume: resubmit missing markers → byte-identical tile outputs.
5. Oracle: STILTS/lsdb agreement ≥ threshold on sky-mode pilot tiles (threshold set in pilot report).
6. Science gates: SED coverage, QSO loci, CMD, Pristine [Fe/H], null-rate ceilings by footprint.

## Code home (D-HOME)

- Generic matching / HATS I/O / association / tile primitives → **xmatcher** (geometry/epoch in **torchsky** when needed).
- Release-specific scripts → **`xmatcher/scripts/pensus/`** while thin.
- Revisit split to `/arc/projects/k-pop/pensus/code` or a separate repo after Phase 2 spike if the driver grows multi-module with its own test/config schema.
