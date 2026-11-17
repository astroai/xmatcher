# Pensus matching policy (S-MATCH / S-TILE / S-EPOCH / S-PAIR0)

Locked for v0; numeric pair radii calibrated in pilot (`pair_policy.toml`).

## Topology

- **Spine × survey star**, not all-pairs. Bounded components (diameter ≤ 2·r_max
  per level) so a fixed HEALPix tile margin is sufficient.
- Edges are only spine↔survey (or cascade-level spine↔survey). GALEX and
  AllWISE W3–W4 are **attachments** with allowed multiplicity; they never
  define identity.

## Cascade (secondary-only survival)

| Level | Spine for leftovers |
|------:|---------------------|
| L1 | Gaia DR3 |
| L2 | PS1 (N) / SkyMapper DR4 (S); Legacy Tractor in v0.2 |
| L3 | CatWISE2020 |
| NIR floor | 2MASS (+ VHS/UHS when present in later views) |

Every input row appears in **exactly one** component at its **first eligible**
cascade level.

## 1:1 rule

- **Mutual-best** per pair, tile-local.
- Global assignment (Hungarian) only for components entirely interior to a tile
  core; never across tile boundaries.

## Ownership

Emit a component from the tile whose **core** contains the cascade-level spine
member. Deterministic tie-break: lowest HEALPix pixel id.

## Margin

\[
\mathrm{margin} \ge 2 \cdot r_{\max} \cdot L + \max|\mu|\cdot\Delta t_{\max}
\]

with \(L\) = cascade levels (v0.1: 3). Include PM halo (e.g. Gaia–2MASS ≈ 17 yr).

## Epoch (S-EPOCH)

- Prefer partner-epoch proper-motion propagation when both sides have usable PM.
- Surveys without PM (GALEX, VHS, UHS floors, …): σ floors and/or larger sky
  radii in `pair_policy.toml`; do not invent PM.
- YAML `epoch` fields that are equinox placeholders (e.g. 2000.0) must not drive
  PM; treat as unknown epoch until verified.

## Edges / engines

- Start **`sky` + pair radii**; promote **`skyerr`** after pilot calibration.
- Production: `engine="fast"`, `fallback_policy="error"` — never `auto` or
  `ray-union` (see `route_guard.py`).
- Oracles: STILTS and lsdb margin-cache crossmatch on pilot tiles.

## Pristine

**ID-join on Gaia `source_id` only** — never positional match (would
double-count Gaia positions).
