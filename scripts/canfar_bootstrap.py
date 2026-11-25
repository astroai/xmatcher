#!/usr/bin/env python3
"""Bootstrap one xmatcher scenario inside a CANFAR headless session.

CANFAR splits ``create ... -- CMD ARGS`` on whitespace and does not honour
shell quoting, so the session command must be one space-free token.  This
bootstrap is small enough to embed that way::

    python3 -c exec(__import__('base64').b64decode('<this file>').decode()) <scenario>

It unpacks a source tarball into ``/scratch`` and hands off to
``scripts/canfar_headless_run.py``, which installs the pixi environment and
runs the scenario. Set ``XMATCHER_ARCHIVE_PATH`` to a mounted tarball or
``XMATCHER_ARCHIVE`` to a ``vos:`` tarball. ``XMATCHER_RUN_ID`` groups results.
"""

from __future__ import annotations

import os
import subprocess
import sys
import tarfile
import time
from pathlib import Path

VCP = "/opt/canfar/bin/vcp"
RUN_ID = os.environ.get("XMATCHER_RUN_ID") or time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
ARCHIVE = os.environ.get("XMATCHER_ARCHIVE")
SCENARIO = sys.argv[1] if len(sys.argv) > 1 else "help"
WORK = Path(os.environ.get("XMATCHER_WORK", f"/scratch/xmatcher-{SCENARIO}-{RUN_ID}"))


def step(cmd: list[str]) -> int:
    print("+", " ".join(cmd), flush=True)
    return subprocess.run(cmd, text=True, check=False).returncode


def main() -> int:
    if SCENARIO == "help":
        print("usage: canfar_bootstrap.py SCENARIO")
        return 0

    WORK.mkdir(parents=True, exist_ok=True)
    tarball = WORK / "src.tar.gz"
    if not (WORK / "scripts" / "canfar_headless_run.py").is_file():
        path = os.environ.get("XMATCHER_ARCHIVE_PATH")
        if path:
            tarball = Path(path)
        elif ARCHIVE:
            code = step([VCP, ARCHIVE, str(tarball)])
            if code:
                return code
        else:
            print("Set XMATCHER_ARCHIVE_PATH or XMATCHER_ARCHIVE.", file=sys.stderr)
            return 2
        print(f"unpacking {tarball} into {WORK}", flush=True)
        with tarfile.open(tarball) as archive:
            archive.extractall(WORK, filter="data")

    runner = WORK / "scripts" / "canfar_headless_run.py"
    env = dict(
        os.environ,
        XMATCHER_REPO=str(WORK),
        XMATCHER_RUN_ID=RUN_ID,
        XMATCHER_WORK=str(WORK),
    )
    print(f"scenario={SCENARIO} run={RUN_ID} work={WORK}", flush=True)
    return subprocess.run(
        [sys.executable, str(runner), SCENARIO],
        cwd=str(WORK),
        env=env,
        check=False,
    ).returncode


if __name__ == "__main__":
    sys.exit(main())
