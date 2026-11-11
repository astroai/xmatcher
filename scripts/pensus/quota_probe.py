#!/usr/bin/env python3
"""Probe hats + k-pop + scratch quotas on a CANFAR/Skaha session.

Anonymous VOSpace probes (this agent VM) only confirmed vos:hats ACL existence
and vos:k-pop NotFound.  Run this script inside an authenticated session:

    python scripts/pensus/quota_probe.py --json

Writes a ledger stub under k-pop docs when --write-ledger is set and the work
root is writable.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve().parent
_SCRIPTS_ROOT = str(_SCRIPT_DIR.parent)
if _SCRIPTS_ROOT not in sys.path:
    sys.path.insert(0, _SCRIPTS_ROOT)

from pensus.paths import SCRATCH_BUDGET_GIB, default_paths  # noqa: E402


def _df(path: Path) -> dict:
    if not path.exists():
        return {"path": str(path), "mounted": False}
    usage = shutil.disk_usage(path)
    return {
        "path": str(path),
        "mounted": True,
        "total_bytes": usage.total,
        "used_bytes": usage.used,
        "free_bytes": usage.free,
        "total_gib": round(usage.total / (1024**3), 3),
        "free_gib": round(usage.free / (1024**3), 3),
    }


def _vls(uri: str) -> dict:
    try:
        proc = subprocess.run(
            ["vls", "-l", uri],
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
    except FileNotFoundError:
        return {"uri": uri, "available": False, "error": "vls not installed"}
    return {
        "uri": uri,
        "available": proc.returncode == 0,
        "returncode": proc.returncode,
        "stdout": (proc.stdout or "")[:4000],
        "stderr": (proc.stderr or "")[:2000],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--write-ledger", action="store_true")
    args = parser.parse_args(argv)
    paths = default_paths()
    report = {
        "probed_at_utc": datetime.now(UTC).isoformat(),
        "scratch_budget_gib": SCRATCH_BUDGET_GIB,
        "df": {
            "hats": _df(paths.hats_survey_root),
            "k_pop_work": _df(paths.work_root),
            "scratch": _df(Path("/scratch")),
        },
        "vospace": {
            "hats": _vls("vos:hats"),
            "k_pop": _vls("vos:k-pop"),
        },
        "env": {
            "PENSUS_PUBLIC_ROOT": os.environ.get("PENSUS_PUBLIC_ROOT"),
            "PENSUS_WORK_ROOT": os.environ.get("PENSUS_WORK_ROOT"),
            "PENSUS_SCRATCH_ROOT": os.environ.get("PENSUS_SCRATCH_ROOT"),
        },
        "status": "ok" if paths.work_root.exists() or Path("/scratch").exists() else "tbd_offline",
        "note": (
            "Quotas remain TBD until authenticated CANFAR df/getfacl. "
            "Anonymous vos:hats exists; vos:k-pop NotFound without auth."
        ),
    }
    text = json.dumps(report, indent=2, sort_keys=True)
    if args.json or not args.write_ledger:
        print(text)
    if args.write_ledger:
        docs = paths.work_root / "docs"
        try:
            docs.mkdir(parents=True, exist_ok=True)
            (docs / "quota-probe.json").write_text(text + "\n")
            print(f"wrote {docs / 'quota-probe.json'}", file=sys.stderr)
        except OSError as exc:
            print(f"ledger write skipped: {exc}", file=sys.stderr)
            return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
