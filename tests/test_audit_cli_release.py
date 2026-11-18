"""Release-audit regressions: the source archive is self-consistent."""

import re
import tomllib
from pathlib import Path

from scripts.check_release import package_files, required_sdist_files

ROOT = Path(__file__).resolve().parents[1]


def _shipped() -> set[str]:
    with (ROOT / "pyproject.toml").open("rb") as stream:
        project = tomllib.load(stream)["project"]
    files = required_sdist_files(ROOT, package_files(ROOT), project)
    return {path.relative_to(ROOT).as_posix() for path in files}


def test_shipped_tests_only_use_shipped_script_directories():
    shipped = _shipped()
    for name in sorted(shipped):
        if not name.startswith("tests/") or not name.endswith(".py"):
            continue
        text = (ROOT / name).read_text()
        for directory in re.findall(r'"scripts"\s*/\s*"([\w-]+)"', text):
            assert any(path.startswith(f"scripts/{directory}/") for path in shipped), (
                f"{name} needs scripts/{directory}/, which the source archive omits"
            )


def test_shipped_pixi_tasks_reference_shipped_scripts():
    shipped = _shipped()
    referenced = set(re.findall(r"scripts/[\w./-]+", (ROOT / "pixi.toml").read_text()))
    if "scripts/install-git-hooks.sh" in referenced:
        referenced.add("scripts/git-hooks/pre-push")
    missing = sorted(path for path in referenced if path not in shipped)
    assert not missing, f"pixi tasks reference files the source archive omits: {missing}"
