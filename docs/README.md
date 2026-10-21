# xmatcher documentation

## Current guides

- [User guide and cookbooks](usage.md) — installation, local and remote
  matching, CLI error models, memory controls, and distributed workflows.
- [Public API reference](api.md) — `CrossMatch`, `MatchRequest`, `MatchSpec`,
  engines, storage, releases, and exceptions.
- [Algorithms and score semantics](algorithms.md) — matcher definitions,
  uncertainty assumptions, and expected limits.
- [Catalogue naming](naming.md) — bare-name grammar, `vizier:` / `tap:` /
  `hats:` identifiers, and the applied key migration with kept aliases.
- [Source and measurement releases](observations.md) — lazy measurement
  ledgers, photometric normalization, and checksummed releases.
- [Catalogue-to-model pipeline](catalogue-pipeline.md) — pinned observations,
  sparse candidate releases, and association hypotheses.
- [Association contract v1](association-v1.md) — normative versioned record,
  release directory, and component-sidecar specification.
- [0.5.1 migration notes](release-0.5.1.md) — the `xmatch` → `xmatcher` rename
  and path/config changes.

## Reviews

- [Documentation, CLI, and naming audit (2026-10-21)](audit-2026-10-21.md) —
  findings and fixes from that review; the naming migration it proposed is now
  applied.

## Historical records

These document a specific frozen release or measurement session. They
intentionally use the old `xmatch` package name and must not be read as current
usage instructions.

- [0.5.0 release notes](release-0.5.0.md)
- [0.5.0 release audit (2026-10-06)](release-audit-2026-10-06.md)
- [CANFAR scaling report (2026-10-07)](canfar-scaling-2026-10-07.md)
