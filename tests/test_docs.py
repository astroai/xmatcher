"""Documentation drift guards.

These tests catch the copy-paste failures prose review misses: a documented
flag that no longer exists, a relative link that moved, or an anchor that no
longer matches its heading. Historical records and the dated audit are
excluded on purpose — they quote the old spelling as a finding.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

import xmatcher.cli as cli

ROOT = Path(__file__).resolve().parents[1]
CURRENT_DOCS = [ROOT / "README.md", *sorted((ROOT / "docs").glob("*.md"))]
HISTORICAL_GLOBS = ("release-*", "canfar-*", "audit-*")


def _current_docs() -> list[Path]:
    return [
        path
        for path in CURRENT_DOCS
        if not any(path.name.startswith(prefix.rstrip("*")) for prefix in HISTORICAL_GLOBS)
    ]


def _cli_flags() -> set[str]:
    flags = {"--help", "-h", "--version", "-v"}
    for name in dir(cli):
        if not (name.startswith("_build_") and name.endswith("_subparser")):
            continue
        builder = getattr(cli, name)
        if not callable(builder):
            continue
        try:
            parser = builder()
        except TypeError:  # pragma: no cover - future builders may take args
            continue
        for action in parser._actions:
            flags.update(action.option_strings)
    return flags


def test_documented_flags_exist_in_the_cli():
    flags = _cli_flags()
    unknown: list[str] = []
    for path in _current_docs():
        for lineno, line in enumerate(path.read_text().splitlines(), start=1):
            for match in re.finditer(r"(?<![\w-])--[a-z][a-z0-9-]+", line):
                if match.group(0) not in flags:
                    unknown.append(f"{path.relative_to(ROOT)}:{lineno}: {match.group(0)}")
    assert not unknown, "documented but undefined CLI flags:\n  " + "\n  ".join(unknown)


def test_naming_migration_table_matches_the_registry():
    """The documented former→canonical table is the actual alias contract."""
    from xmatcher import CrossMatch

    text = (ROOT / "docs" / "naming.md").read_text()
    section = text.split("## Migration (applied)", 1)[1].split("###", 1)[0]
    rows = re.findall(r"^\| `([a-z0-9_]+)` \| `([a-z0-9_]+)` \|", section, re.MULTILINE)
    assert rows, "naming.md migration table not found"

    cm = CrossMatch()
    for former, canonical in rows:
        assert canonical in cm.catalogues_config, f"{canonical} is not a catalogue key"
        assert cm.resolve_name(former) == canonical, f"{former} no longer resolves to {canonical}"


def _slug(heading: str) -> str:
    # GitHub keeps one hyphen per space, so "A & B" becomes "a--b".
    slug = re.sub(r"[^\w\s-]", "", heading.strip().lower())
    return re.sub(r"\s", "-", slug)


def _anchors(path: Path) -> set[str]:
    anchors: set[str] = set()
    for line in path.read_text().splitlines():
        if line.startswith("#"):
            anchors.add(_slug(line.lstrip("#").strip()))
    return anchors


@pytest.mark.parametrize("path", CURRENT_DOCS, ids=lambda p: p.name)
def test_relative_markdown_links_resolve(path: Path):
    text = path.read_text()
    broken: list[str] = []
    for match in re.finditer(r"\[[^\]]*\]\(([^)\s]+)\)", text):
        href = match.group(1)
        if href.startswith(("http://", "https://", "mailto:")):
            continue
        target, _, anchor = href.partition("#")
        resolved = (path.parent / target).resolve() if target else path
        if not resolved.exists():
            broken.append(href)
        elif anchor and resolved.suffix == ".md" and anchor not in _anchors(resolved):
            broken.append(f"{href} (missing anchor)")
    assert not broken, f"broken links in {path.relative_to(ROOT)}: {broken}"
