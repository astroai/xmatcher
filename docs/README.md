# xmatcher Documentation

Welcome to the `xmatcher` documentation. This index organizes user manuals, public Python and CLI references, algorithm specifications, and architecture guides.

---

## Core Documentation & References

- **[CLI Reference](cli.md)**
  Complete manual for the `xmatcher` command-line interface: all subcommands (`match`, `list`, `describe`, `discover`, `search`, `adopt`, `sync`, `project-hats`, `completion`, `doctor`), flags, and recipes.
- **[Public Python API Reference](api.md)**
  Full API documentation for `CrossMatch`, `MatchRequest`, `MatchSpec`, engine choices, relational join modes, and exception types.
- **[User Guide & Cookbooks](usage.md)**
  End-to-end user manual covering installation, local and remote crossmatching, explicit memory budget controls, and distributed cluster execution.
- **[Algorithms & Mathematical Models](algorithms.md)**
  Mathematical formulations for angular separation, Gaussian error ellipses (Mahalanobis distance), Sutherland & Saunders Likelihood Ratio, proper-motion drift, and Bayesian posteriors.

---

## Surveys & Catalogue Pipelines

- **[Catalogue Naming & Resolution](naming.md)**
  Grammar for canonical survey names, aliases, and explicit identifiers (`vizier:<id>`, `tap:<endpoint>/<table>`, `hats:<url>`).
- **[HATS Catalogue Sources](hats-sources.md)**
  IVOA registry lookups, verified LSDB/IRSA datasets, and remote read capabilities across HTTP, S3, and Hugging Face.
- **[Anchor-Centric Crossmatching (`ray-anchor`)](ray-anchor.md)**
  One-row-per-anchor-source master catalog generator with multi-survey counterpart ranking, confusion metrics, and HATS partitioning.

---

## Data Contracts & Architecture

- **[Source & Measurement Releases](observations.md)**
  Lazy measurement ledgers, photometric unit and limit normalization, property evidence, and checksummed Parquet releases.
- **[Catalogue-to-Model Pipeline](catalogue-pipeline.md)**
  End-to-end data flow from survey observations to sparse candidate sets and target association hypotheses.
- **[Association Contract v1](association-v1.md)**
  Normative specification for versioned association tables, directory layouts, component sidecars, and member equivalence.

---

## Releases & Historical Archive

- **[0.6.0 Release Notes](archive/release-0.6.0.md)** — Named catalogues, S3/HF HATS streaming, `skyerr` radius bounds, and upgrade guide.
- **[0.5.1 Migration Notes](archive/release-0.5.1.md)** — Namespace migration from `xmatch` to `xmatcher`.
- **[0.5.0 Release Notes](archive/release-0.5.0.md)** — Initial 0.5.0 release features.
- **[Documentation & CLI Audit (2026-10-21)](archive/audit-2026-10-21.md)** — Verification findings and naming migration records.
- **[0.5.0 Release Audit (2026-10-06)](archive/release-audit-2026-10-06.md)** — Pre-release verification logs.
- **[CANFAR Scaling Report (2026-10-07)](archive/canfar-scaling-2026-10-07.md)** — Benchmark results and scaling curves.
