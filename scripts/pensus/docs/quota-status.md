# Quota probe status (Day-0)

Authenticated **2026-11-11 America/Vancouver** with `~/.ssl/cadcproxy.pem`
(`CN=sfabbro_5aa`, valid through **2026-12-06 UTC**).

## ARC project quotas

| Project | Path | Quota | Used | Free |
|---------|------|------:|-----:|-----:|
| **hats** | `/arc/projects/hats` (`vos://…~arc/projects/hats`) | 5.000 TB | 4.657 TB | **~319 GiB** |
| **k-pop** | `/arc/projects/k-pop` (`vos://…~arc/projects/k-pop`) | 33.000 TB | 20.204 TB | **~11.64 TiB** |

Write groups: `hats`, `k-pop`. Vault `vos:hats` has no quota property;
`vos:k-pop` does not exist on vault (ARC only).

## Scratch

Skaha session `/scratch` only (not on this agent VM). Budget **≤290 GiB**;
target peak ≤250 GiB.

## Layout

Created under ARC:

- `k-pop/pensus/{staging,inputs,hats_build,tiles,drafts,logs,docs}`
- `hats/pensus/{views,sidecars,docs}`

Ledger copies: `…/pensus/docs/quota-probe.json` on both projects.

## Ops note

**hats free ~319 GiB is tight** for public promote. Keep work/drafts on k-pop;
preflight free space before every release promote.
Dual budget templates: `config/disk-budget-kpop.toml`, `config/disk-budget-hats.toml`.
