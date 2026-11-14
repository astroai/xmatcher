# pensus scripts (thin layer)

Release-specific orchestration for the **pensus** UV–IR photometry union.
Generic matching, HATS I/O, and association live in the `xmatcher` package;
these scripts stay thin (D-HOME).

## Path split

| Path | Role |
|------|------|
| `/arc/projects/hats/pensus` | **Public release only** |
| `/arc/projects/k-pop/pensus` | **All work** (staging, inputs, tiles, drafts) |
| `/scratch/pensus/<run>` | Ephemeral ≤290 GiB |

## Quick start (offline / CI)

```bash
WORKDIR=$(mktemp -d)
python scripts/pensus/spike.py --work-root "$WORKDIR" --demo
python scripts/pensus/campaigns/pilot.py --work-root "$WORKDIR" --demo
python scripts/pensus/tile_audit.py --campaign pilot-v0 --work-root "$WORKDIR"
```

Before pushing script changes: `pixi run format` then `pixi run preflight-push`
(or install the repo pre-push hook with `pixi run install-git-hooks`).

## Production (CANFAR)

1. `quota_probe.py --write-ledger`
2. `layout_init.py`
3. Ingest pins (`ingest/*.py --print-commands`)
4. `tile_submit.py` → headless `tile_run.py` (real trees; drop `--demo`)
5. Promote checksummed drafts → `/arc/projects/hats/pensus`

URL sync (`url_sync_launch.py`) size gates require an explicit project free
estimate: set `PENSUS_SYNC_FREE_GIB` (from a VOS/quota probe) and/or
`PENSUS_SYNC_MAX_GIB`. Do not use `df` on `/arc` — that is the shared Ceph
pool, not the k-pop quota. Legacy DR10.1 (~4.7 TiB) stays paused until
Sébastien decides free-space policy.

Matching: `engine=fast`, `fallback_policy=error` (`route_guard.py`).
Docs: `docs/matching-policy.md`, `docs/product-spec.md`, `config/pair_policy.toml`.
