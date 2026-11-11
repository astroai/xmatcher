"""Locked pensus path layout (public hats vs k-pop work vs scratch)."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

SCRATCH_BUDGET_GIB = 290
SCRATCH_HEADROOM_GIB = 250  # leave ~40 GiB free for session OS / other jobs


@dataclass(frozen=True)
class PensusPaths:
    """Filesystem roots for one pensus campaign session."""

    public_root: Path  # /arc/projects/hats/pensus
    work_root: Path  # /arc/projects/k-pop/pensus
    hats_survey_root: Path  # /arc/projects/hats
    scratch_root: Path  # /scratch/pensus/<run>


def default_paths(run_id: str | None = None) -> PensusPaths:
    run = run_id or os.environ.get("PENSUS_RUN_ID", "default")
    return PensusPaths(
        public_root=Path(os.environ.get("PENSUS_PUBLIC_ROOT", "/arc/projects/hats/pensus")),
        work_root=Path(os.environ.get("PENSUS_WORK_ROOT", "/arc/projects/k-pop/pensus")),
        hats_survey_root=Path(os.environ.get("PENSUS_HATS_ROOT", "/arc/projects/hats")),
        scratch_root=Path(os.environ.get("PENSUS_SCRATCH_ROOT", f"/scratch/pensus/{run}")),
    )


def work_layout(work_root: Path) -> dict[str, Path]:
    """Canonical k-pop work subdirectories."""
    return {
        "staging": work_root / "staging",
        "inputs": work_root / "inputs",
        "hats_build": work_root / "hats_build",
        "tiles": work_root / "tiles",
        "drafts": work_root / "drafts" / "views",
        "logs": work_root / "logs",
        "docs": work_root / "docs",
    }


def ensure_work_layout(work_root: Path) -> dict[str, Path]:
    layout = work_layout(work_root)
    for path in layout.values():
        path.mkdir(parents=True, exist_ok=True)
    return layout


def public_layout(public_root: Path) -> dict[str, Path]:
    return {
        "views": public_root / "views",
        "sidecars": public_root / "sidecars",
        "docs": public_root / "docs",
    }
