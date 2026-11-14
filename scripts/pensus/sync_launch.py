#!/usr/bin/env python3
"""Skaha entrypoint for pensus LSDB pin sync.

Installs nothing itself beyond path wiring; ``canfar_sync_job`` bootstraps
deps into ``/scratch/pensus-pydeps`` because Skaha images set
``PYTHONNOUSERSITE=1``.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

deps = Path(os.environ.get("PENSUS_PYDEPS", "/scratch/pensus-pydeps"))
deps.mkdir(parents=True, exist_ok=True)
code = Path(os.environ.get("PENSUS_CODE_ROOT", "/arc/projects/k-pop/pensus/code"))
vendor_src = Path(os.environ.get("PENSUS_XMATCHER_SRC", str(code / "vendor" / "xmatcher" / "src")))

for path in (deps, vendor_src, code):
    if path.exists() and str(path) not in sys.path:
        sys.path.insert(0, str(path))

os.environ["PYTHONPATH"] = os.pathsep.join(
    [str(p) for p in (deps, vendor_src, code) if p.exists()]
    + ([os.environ["PYTHONPATH"]] if os.environ.get("PYTHONPATH") else [])
)
os.environ.setdefault("PENSUS_PYDEPS", str(deps))
os.environ.setdefault("XMATCHER_CACHE_ROOT", "/arc/projects/k-pop/pensus/inputs/_cache")
os.environ.setdefault("PENSUS_JOB_MODE", "sync")

from pensus.canfar_sync_job import main  # noqa: E402

raise SystemExit(main(sys.argv[1:]))
