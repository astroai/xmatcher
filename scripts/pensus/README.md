# PENSUS on CANFAR

Use `scripts.pensus.release` for the full-sky photometry union. It reads the
mounted HATS trees in place and preserves selected native measurements,
uncertainties, flags and source IDs for every selected survey. A row without a
counterpart survives as a singleton.

| Path | Role |
|---|---|
| `/arc/projects/hats` | Existing, read-only input catalogue trees |
| `/arc/projects/astroai/pensus/drafts/views/<release>` | Pinned input plan, resumable tiles and finalized draft |
| `/scratch/pensus/<job>` | Optional temporary output buffer, scoped to one running job |
| `/arc/projects/hats/pensus/views/<release>` | Versioned publication destination after release QA |

## Inventory and pilot

Run from a pinned xmatcher checkout in a CANFAR session. Install the same
locked runtime on the planner and workers, and use the same checkout revision.
The example manifest uses Gaia DR3 and 2MASS PSC. The
[26-survey CANFAR pilot manifest](config/full-union-canfar-pilot.yaml) carries
the principal UV, optical, and infrared projections reviewed against the
mounted catalogue schemas. [Gaia data model](https://gea.esac.esa.int/archive/documentation/GDR3/Gaia_archive/chap_datamodel/sec_dm_main_source_catalogue/ssec_dm_gaia_source.html)

```bash
WORK=/arc/projects/astroai/pensus
PILOT="$WORK/drafts/views/full-union-pilot"
pixi run python -m scripts.pensus.release inventory
pixi run python -m scripts.pensus.release plan --pilot --draft "$PILOT" \
  --spec scripts/pensus/config/full-union-pilot.yaml
pixi run python -m scripts.pensus.release run --draft "$PILOT" \
  --pixel 105351 --scratch /scratch/pensus/pilot --pool-gib 4
pixi run python -m scripts.pensus.release status --draft "$PILOT"
```

Choose pilot pixels deliberately, covering the Galactic plane and bulge,
sparse extragalactic fields, both hemispheres, survey footprint edges and
close pairs straddling tile boundaries. Compare candidate and reciprocal
matches with an independent spherical oracle. Repeat with a different output
HEALPix order to check memberships across different tilings. Use the pilot to
calibrate pair radii and the order and memory budget for the densest fields.

## Full release

Use the live inventory to fill out the ordered manifest. List each
out-of-scope, alternate or unreadable tree with its reason under `exclude`.
The manifest pins the exact paths, key columns, selected fields, optional band
metadata and pair radii. A key can be one integer, string or binary column or a list
of columns, such as `[RELEASE, BRICKID, OBJID]` for Legacy Surveys. See the
[release audit and science handoff](docs/full-release.md) for the survey review
and downstream contracts.

```bash
DRAFT="$WORK/drafts/views/pensus-v1"
pixi run python -m scripts.pensus.release plan --draft "$DRAFT" \
  --spec "$WORK/pensus-v1.yaml"

# Run disjoint [start, stop) ranges in CANFAR headless sessions.
pixi run python -m scripts.pensus.release pending --draft "$DRAFT" --tiles-per-job 64
pixi run python -m scripts.pensus.release run --draft "$DRAFT" \
  --start 0 --stop 64 --scratch /scratch/pensus/shard-0 --pool-gib 4

pixi run python -m scripts.pensus.release status --draft "$DRAFT"
pixi run python -m scripts.pensus.release finalize --draft "$DRAFT" --deep
```

Submit the ranges printed by `pending`. Each job reuses its loaded plan and
partition indexes for its entire range. Repeating a range resumes from its
valid markers. Use `run --repair --pixel <n>` to regenerate a damaged
tile before finalization. The scratch option buffers one Parquet output at a
time, then copies it to ARC and commits it atomically. Leave it off to write
directly to the draft filesystem. The pool budget covers projected input
data. Request additional job RAM for Parquet decoding, geometry, indexes and
wide output batches. [CANFAR batch storage](https://www.opencadc.org/canfar/edge/platform/sessions/batch/)

The finalized tree is `$DRAFT/catalog`. Its receipt records the release plan
hash, declared surveys and exclusions, column maps, source conservation
checks, and whether output checksums were verified. It is ready for science
QA before publication. Keep the matching plan and tile receipts with the
draft for recovery and provenance.

Finalization writes `partition_info.csv` and `dataset/_common_metadata`.
Use `--aggregate-metadata` for small exports that need a combined row-group
footer. The HATS standard makes the combined footer optional. Keeping
partition-level footers avoids a catalogue-wide metadata allocation on
large, wide releases. [IVOA HATS metadata specification](https://www.ivoa.net/documents/Notes/HATS/20250822/NOTE-hats-ivoa-1.0-20250822.html)

The older `tile_submit.py`, `tile_run.py`, `view_assemble.py` and `campaigns/`
scripts are the v0 demo harness. The acquisition and promotion scripts under
`arc/` document how individual HATS trees were staged.
