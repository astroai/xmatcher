"""xmatcher: cross-match astronomical catalogues (local files, remote archives, HATS).

Key high-level operations:

* :meth:`CrossMatch.crossmatch` — classic two-catalogue match.
* :meth:`CrossMatch.crossmatch_multi` — N-catalogue pairwise intersection.
* :meth:`CrossMatch.union_match` — build a master union catalogue (full outer joins).
* :meth:`CrossMatch.nway_match` — Bayesian N-way simultaneous crossmatching.
* :meth:`CrossMatch.fof_match` — Friends-of-Friends transitive closure merging
  multi-survey detections into object bundles.
* :meth:`CrossMatch.crossmatch_request` — typed entry point for new code.

The public names below are imported on first use, so ``import xmatcher`` (and
``xmatcher --help``) does not load Polars, NumPy or Astropy.
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING, Any

__version__ = "0.6.0"

# public name -> defining submodule
_EXPORTS = {
    "CrossMatch": "crossmatch",
    "CatalogueSource": "sources",
    "MatchSpec": "matchers",
    "MatchRequest": "request",
    "SideOverrides": "request",
    "source_inventory": "observations",
    "namespaced_source_id": "observations",
    "normalize_photometry": "observations",
    "normalize_property_evidence": "observations",
    "required_observation_columns": "observations",
    "validate_source_inventory": "observations",
    "write_observation_release": "observations",
    "verify_observation_release": "observations",
    "write_candidate_release": "candidates",
    "verify_candidate_release": "candidates",
    "normalize_candidate_hypotheses": "candidates",
    "CrossMatchError": "exceptions",
    "ConfigError": "exceptions",
    "InputError": "exceptions",
    "TapError": "exceptions",
    "StiltsError": "exceptions",
    "FrameLike": "io_utils",
    "is_hats_dir": "io_utils",
    "project_hats": "hats_project",
    "scan_frame": "io_utils",
    "write_frame": "io_utils",
    "astropy_table_to_polars": "io_utils",
    "polars_to_astropy": "io_utils",
}

__all__ = [
    "CrossMatch",
    "CatalogueSource",
    "MatchSpec",
    "MatchRequest",
    "SideOverrides",
    "source_inventory",
    "namespaced_source_id",
    "normalize_photometry",
    "normalize_property_evidence",
    "required_observation_columns",
    "validate_source_inventory",
    "write_observation_release",
    "verify_observation_release",
    "write_candidate_release",
    "verify_candidate_release",
    "normalize_candidate_hypotheses",
    "CrossMatchError",
    "ConfigError",
    "InputError",
    "TapError",
    "StiltsError",
    "FrameLike",
    "is_hats_dir",
    "project_hats",
    "scan_frame",
    "write_frame",
    "astropy_table_to_polars",
    "polars_to_astropy",
    "__version__",
]


def __getattr__(name: str) -> Any:
    module = _EXPORTS.get(name)
    if module is None:
        raise AttributeError(f"module 'xmatcher' has no attribute {name!r}")
    value = getattr(importlib.import_module(f".{module}", __name__), name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted({*globals(), *_EXPORTS})


if TYPE_CHECKING:
    from .candidates import (
        normalize_candidate_hypotheses,
        verify_candidate_release,
        write_candidate_release,
    )
    from .crossmatch import CrossMatch
    from .exceptions import ConfigError, CrossMatchError, InputError, StiltsError, TapError
    from .hats_project import project_hats
    from .io_utils import (
        FrameLike,
        astropy_table_to_polars,
        is_hats_dir,
        polars_to_astropy,
        scan_frame,
        write_frame,
    )
    from .matchers import MatchSpec
    from .observations import (
        namespaced_source_id,
        normalize_photometry,
        normalize_property_evidence,
        required_observation_columns,
        source_inventory,
        validate_source_inventory,
        verify_observation_release,
        write_observation_release,
    )
    from .request import MatchRequest, SideOverrides
    from .sources import CatalogueSource
