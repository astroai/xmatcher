# Quota probe status (Day-0)

## This agent VM

- `/arc` and `/scratch` are **not mounted** (no local `df`/ACL).
- Anonymous VOSpace: **`vos:hats`** exists (`groupread=hats`, `groupwrite=hats-rw`,
  `ispublic=false`). No `quota` property; empty listing anonymously.
- **`vos:k-pop` / `vos:kpop`**: NotFound anonymously (needs CANFAR auth / Skaha).
- Personal `vos:sfabbro` quota is **not** the hats project quota.

## Action on CANFAR

```bash
python scripts/pensus/quota_probe.py --json --write-ledger
df -h /arc/projects/hats /arc/projects/k-pop /scratch
# plus getfacl / project portal quotas
```

Until then hats+k-pop quotas remain **TBD**. Dual budget templates:

- `scripts/pensus/config/disk-budget-kpop.toml`
- `scripts/pensus/config/disk-budget-hats.toml`
