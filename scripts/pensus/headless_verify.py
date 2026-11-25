"""Verify the PENSUS checkout in CANFAR headless using synthetic catalogues."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import tarfile
import tempfile
from pathlib import Path

CHECKS = {
    "install": ["install", "--locked"],
    "lint": ["run", "ruff", "check", "src", "scripts", "tests"],
    "format": ["run", "ruff", "format", "--check", "src", "scripts", "tests"],
    "types": ["run", "mypy", "src/xmatcher", "scripts", "tests"],
    "regressions": [
        "run",
        "python",
        "-m",
        "pytest",
        "-q",
        "--tb=short",
        "tests/test_pensus_release.py",
        "tests/test_pensus_tile.py",
        "tests/test_pensus_arc_ingest.py",
        "tests/test_pensus_hats_audit_repair.py",
        "tests/test_observations.py",
        "tests/test_r2_observations.py",
        "tests/test_hats_project.py",
        "tests/test_ray_union.py",
        "tests/test_ray_anchor.py",
        "tests/test_ray_anchor_parity.py",
    ],
}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--sha256", required=True)
    parser.add_argument("--results", type=Path, required=True)
    parser.add_argument("--scratch", type=Path, default=Path("/scratch"))
    args = parser.parse_args(argv)
    if args.results.resolve().is_relative_to(Path("/arc/projects/hats")):
        parser.error("Choose a results directory outside the released survey tree")
    pixi = shutil.which("pixi")
    if not pixi:
        parser.error("The CANFAR image must provide pixi")
    with args.archive.open("rb") as handle:
        actual = hashlib.file_digest(handle, "sha256").hexdigest()
    if actual != args.sha256:
        parser.error("Checkout archive does not match the requested SHA-256")
    args.results.mkdir(parents=True, exist_ok=False)
    result_file = args.results / "qa-result.json"
    report: dict = {"archive_sha256": actual, "checks": {}}
    with tempfile.TemporaryDirectory(prefix="pensus-qa-", dir=args.scratch) as directory:
        scratch = Path(directory)
        repo = scratch / "repo"
        with tarfile.open(args.archive, "r:gz") as archive:
            archive.extractall(repo, filter="data")
        env = dict(
            os.environ,
            PIXI_HOME=str(scratch / "pixi-home"),
            PIXI_CACHE_DIR=str(scratch / "pixi-cache"),
            UV_CACHE_DIR=str(scratch / "uv-cache"),
            RAY_TMPDIR=str(scratch),
            MPLCONFIGDIR=str(scratch / "matplotlib"),
            PYTHONNOUSERSITE="1",
        )
        env.pop("PYTHONPATH", None)
        for step, command in CHECKS.items():
            print(f"STEP {step}", flush=True)
            log_path = args.results / f"{step}.log"
            with log_path.open("w") as log:
                try:
                    code = subprocess.run(
                        [pixi, *command],
                        cwd=repo,
                        env=env,
                        stdout=log,
                        stderr=subprocess.STDOUT,
                        check=False,
                        timeout=1800,
                    ).returncode
                except subprocess.TimeoutExpired:
                    code = 124
                except OSError as exc:
                    log.write(f"{type(exc).__name__}: {exc}\n")
                    code = 1
            report["checks"][step] = code
            if code or step == "regressions":
                with log_path.open("rb") as log:
                    length = log.seek(0, os.SEEK_END)
                    log.seek(max(0, length - 8192))
                    tail = log.read().decode("utf-8", errors="replace")
                if code:
                    report.setdefault("failures", {})[step] = tail
                else:
                    report["regressions_summary"] = tail.strip().rsplit("\n", 1)[-1]
            work = result_file.with_suffix(".tmp")
            work.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
            work.replace(result_file)
            print(f"RESULT {step} {code}", flush=True)
            if step == "install" and code:
                break
    return int(any(report["checks"].values()))


if __name__ == "__main__":
    raise SystemExit(main())
