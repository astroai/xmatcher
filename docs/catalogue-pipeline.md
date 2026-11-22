# Source-preserving catalogue and latent-model pipeline

The catalogue foundation publishes measurements and sparse candidate pairs.
Source association, scientific acceptance, and property-label selection remain
explicit. An outer union preserves observations; it does not prove their identity.

## Ingest pinned survey products

Use existing CatalogueSource/config resolution with an explicit
`release_namespace`, `photometry`, `property_evidence`, and `release_metadata`.
[Observation mappings](observations.md) retain native rows, passbands, signed
flux, uncertainties, limit conventions, times, methods, and raw property evidence.
`required_observation_columns(source)` returns the columns to acquire for these
products; matching defaults are not a complete observation selection.

```python
from xmatcher import CrossMatch, write_observation_release, write_candidate_release

cm = CrossMatch()
sources = [
    cm.resolve_source(
        "optical.parquet",
        {
            "id_column": "id",
            "ra_column": "ra",
            "dec_column": "dec",
            "release_namespace": "example/optical/release1",
            "photometry": [
                {
                    "passband": "example:g:v1",
                    "value_column": "g_flux",
                    "error_column": "g_error",
                    "unit": "uJy",
                    "method": "psf",
                }
            ],
        },
    ),
    cm.resolve_source(
        "infrared.parquet",
        {
            "id_column": "id",
            "ra_column": "ra",
            "dec_column": "dec",
            "release_namespace": "example/infrared/release1",
        },
    ),
]
observations = write_observation_release(
    "releases/observations-1",
    sources,
    release_id="example-observations-1",
    provenance={"software_version": "local-development", "acquisition": "pinned local inputs"},
)
candidates = write_candidate_release(
    "releases/candidates-1",
    sources,
    radius_arcsec=3.0,
    memory_budget_bytes=64 * 1024 * 1024,
    provenance={"observation_release_id": observations["release_id"]},
)
```

Both writers refuse to replace existing releases, stage in a sibling temporary
directory, and publish Parquet files with checksum manifests. Candidate releases
require local, ICRS inputs with unique `release_namespace` values. A candidate
release directory holds:

- `sources.parquet`: every native source, including zero-candidate sources, with
  `source_id`, `release_namespace`, `native_id`, `native_id_type`, `ra_deg`,
  `dec_deg`, `epoch_jyear` and `candidate_count`, sorted by `source_id`;
- `candidates.parquet`: every pair in the requested survey-pair/radius scope, with
  `source_id`, `candidate_id`, `source_namespace`, `candidate_namespace`,
  `sep_arcsec` and `evaluation_epoch_jyear`, sorted by (`source_id`,
  `candidate_id`); within each survey pair the source side is the
  lexicographically smaller namespace;
- `manifest.json`: schema `xmatch.candidates.release.v1`, counts, file SHA-256
  checksums, `input_release_ids`, the search parameters, `score_semantics:
  "ranking_score"`, `association_state: "unevaluated_candidates"`, the supplied
  provenance, execution statistics, and a `release_id` (the schema name plus the
  SHA-256 of the manifest without its execution statistics).

Namespaced source IDs distinguish identical native IDs in different releases.
`candidate_count` is graph degree, not confidence or the number of physical objects.
`verify_candidate_release` rechecks checksums, counts, endpoint references and
the release ID.

## Candidate scope and association alternatives

Candidate generation runs `matcher="sky"`, `find="all"` and
`fallback_policy="error"` on the local partitioned spill engine. The matching
budget controls its batch memory proxy; it does not guarantee resident memory
for Polars scans, sorting, or graph inference. Pair outputs avoid the N-survey
Cartesian product. `survey_pairs` can explicitly restrict the searched release
pairs; omitted pairs are outside the release's candidate scope.

`target_epoch` aligns coordinates before repartitioning. Rows at a different
reference epoch need known finite proper motion. Unknown epochs or missing
motion fail rather than silently assuming stationary sources. The search radius
still needs to cover the caller's declared astrometric/physical uncertainty.
Raw astrometry remains in the inventory; pair records carry the evaluation epoch.

Covariance transport for target-epoch matching outside candidate releases is
described under [epoch propagation](algorithms.md#a-epoch-propagation-target_epoch);
the missing-motion prior is an assumed population model and is not supported
by spill execution. Epoch and memory behaviour of `ray-union` and the native
HATS path are in the
[user guide](usage.md#distributed-full-sky-union-ray-union). Every entry point
validates declared frames instead of comparing incompatible coordinates.

```python
import polars as pl
from xmatcher import normalize_candidate_hypotheses

sources = pl.scan_parquet("releases/candidates-1/sources.parquet")
pairs = pl.scan_parquet("releases/candidates-1/candidates.parquet")
# Supply log weights from an explicit likelihood/prior model. Separation or
# cosine similarity alone is not a calibrated Bayes factor.
scored_pairs = pairs.with_columns(pl.lit(0.0).alias("log_weight"))  # illustrative equal weights
hypotheses = normalize_candidate_hypotheses(
    sources,
    scored_pairs,
    candidate_namespace="example/infrared/release1",
    no_match_log_weight=0.0,
)
```

The resulting alternatives compete within the named target survey. Stored pairs
are oriented so the target survey is the candidate side, and every source outside
that survey receives an explicit no-match alternative. The output has
`source_id`, `candidate_id` (null for no match), `log_weight`, `probability`
(the softmax of the log weights per source), `hypothesis_kind`
(`counterpart` or `no_match`), `score_semantics` and `candidate_namespace`;
`log_weight_column` names a different input column. Log weights must include the declared priors and
selection/coverage assumptions. Output semantics are `assumed_prior_posterior`;
normalization does not establish calibration, global one-to-one consistency,
or deblending. Joint multi-survey/blend hypotheses need a scientific inference
model, rather than pooling all detections into one list of competing candidates.

For namespaced pair tables, both endpoint labels are checked against the
inventory. A nonempty pair table must also contain evidence for the requested
target namespace, in either the inventory or pair labels. An empty pair table
can name a target with no inventory rows; inventoried sources then receive only
the explicit no-match alternative.

Existing association.v1 releases remain authoritative for evaluated pair
records. `construct_association_components(..., additional_members=...)`, given
`AssociationComponentMember` values built from the inventory, retains isolated
sources under an explicit included-decision policy. Its
union-find state is O(number of members); components are graph work units,
not automatically adopted celestial objects.
