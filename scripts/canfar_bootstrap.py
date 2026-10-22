#!/usr/bin/env python3
"""Fetch the xmatcher test harness into a CANFAR headless session.

CANFAR splits ``create ... -- CMD ARGS`` on whitespace and does not honour
shell quoting, so the session command has to be one space-free token.  This
bootstrap is fetched with ``urllib`` from a pinned GitHub commit and then does
the ordinary work: clone that commit into ``/scratch`` and hand off to
``scripts/canfar_headless_run.py``:

    python3 -c exec(__import__('urllib.request',fromlist=['x']).urlopen(
        'https://raw.githubusercontent.com/sfabbro/xmatcher/<sha>/scripts/canfar_bootstrap.py'
    ).read()) <scenario>

Every scenario runs in its own session; ``XMATCHER_RUN_ID`` groups the results.
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

REPO_URL = "https://github.com/sfabbro/xmatcher.git"
REF = os.environ.get("XMATCHER_REF", "main")
RUN_ID = os.environ.get("XMATCHER_RUN_ID") or time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
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
    repo = WORK / "xmatcher"
    if not (repo / ".git").is_dir():
        code = step(["git", "clone", "--depth", "1", REPO_URL, str(repo)])
        if code:
            return code
    if REF != "main":
        for cmd in (
            ["git", "-C", str(repo), "fetch", "--depth", "1", "origin", REF],
            ["git", "-C", str(repo), "checkout", "--detach", "FETCH_HEAD"],
        ):
            code = step(cmd)
            if code:
                return code

    runner = repo / "scripts" / "canfar_headless_run.py"
    env = dict(
        os.environ,
        XMATCHER_REPO=str(repo),
        XMATCHER_RUN_ID=RUN_ID,
        XMATCHER_WORK=str(WORK),
    )
    print(f"scenario={SCENARIO} ref={REF} run={RUN_ID} work={WORK}", flush=True)
    return subprocess.run(
        [sys.executable, str(runner), SCENARIO],
        cwd=str(repo),
        env=env,
        check=False,
    ).returncode


if __name__ == "__main__":
    sys.exit(main())
