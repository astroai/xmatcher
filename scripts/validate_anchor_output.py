#!/usr/bin/env python3
"""Stream a ``ray-anchor`` HATS master and report its coverage.

The validator reads only the anchor ID and the per-survey ambiguity columns,
so it can check a multi-billion-row master without materializing the full
catalogue.  It reports:

* total rows, null IDs, and duplicate IDs within each partition;
* per survey: matched rows, crowded rows, PM warnings, and quality counts;
* the output ``hats_nrows`` from ``properties``.

Use ``--expected-rows`` to fail when the master does not match the anchor row
count (the defining contract of ``engine=ray-anchor``).

Example:
    python scripts/validate_anchor_output.py gaia_master.hats \\
        --expected-rows 1811709771 --json validation.json
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import polars as pl

_META_RE = re.compile(r"^(?P<survey>.+)_n_within$")


def _property_row_count(root: Path) -> int | None:
    for name in ("properties", "hats.properties"):
        path = root / name
        if not path.is_file():
            continue
        for line in path.read_text().splitlines():
            if "=" not in line:
                continue
            key, _, value = line.partition("=")
            if key.strip() == "hats_nrows":
                try:
                    return int(value.strip())
                except ValueError:
                    return None
    return None


def _partitions(root: Path) -> list[Path]:
    dataset = root / "dataset"
    base = dataset if dataset.is_dir() else root
    return sorted(base.glob("Norder=*/*/*.parquet"))


def _discover_surveys(schema: list[str]) -> list[str]:
    surveys: list[str] = []
    for name in schema:
        match = _META_RE.match(name)
        if match is not None:
            surveys.append(match.group("survey"))
    return sorted(surveys)


def validate(
    root: Path,
    *,
    id_column: str = "source_id",
    expected_rows: int | None = None,
    progress_every: int = 100,
) -> dict[str, Any]:
    partitions = _partitions(root)
    if not partitions:
        raise SystemExit(f"no HATS partitions found under {root}")
    schema = pl.read_parquet_schema(partitions[0])
    names = list(schema)
    if id_column not in names:
        raise SystemExit(f"{id_column!r} not found in {partitions[0]} (columns: {names[:20]}…)")
    surveys = _discover_surveys(names)
    if not surveys:
        raise SystemExit("no <survey>_n_within columns found; is this a ray-anchor output?")

    columns = [id_column]
    for survey in surveys:
        for suffix in ("_n_within", "_crowded", "_pm_warn", "_quality"):
            name = f"{survey}{suffix}"
            if name not in names:
                raise SystemExit(f"missing {name!r} for survey {survey!r}")
            columns.append(name)

    total = 0
    null_ids = 0
    duplicate_ids = 0
    per_survey: dict[str, Counter[str]] = defaultdict(Counter)
    quality: dict[str, Counter[str]] = defaultdict(Counter)

    for index, path in enumerate(partitions, start=1):
        frame = pl.read_parquet(path, columns=columns)
        total += frame.height
        if frame.height:
            ids = frame[id_column]
            null_ids += int(ids.null_count())
            duplicate_ids += int(ids.is_duplicated().sum())
            for survey in surveys:
                counts = frame[f"{survey}_n_within"]
                per_survey[survey]["rows"] += frame.height
                per_survey[survey]["matched"] += int((counts > 0).sum())
                per_survey[survey]["crowded"] += int(
                    frame[f"{survey}_crowded"].fill_null(False).sum()
                )
                per_survey[survey]["pm_warn"] += int(
                    frame[f"{survey}_pm_warn"].fill_null(False).sum()
                )
                for value, count in frame[f"{survey}_quality"].value_counts().iter_rows():
                    quality[survey][str(value)] += int(count)
        if progress_every and (index % progress_every == 0 or index == len(partitions)):
            print(
                f"validated {index}/{len(partitions)} partitions, {total:,} rows",
                file=sys.stderr,
            )

    advertised = _property_row_count(root)
    report: dict[str, Any] = {
        "path": str(root),
        "partitions": len(partitions),
        "rows": total,
        "advertised_rows": advertised,
        "id_column": id_column,
        "null_ids": null_ids,
        "duplicate_ids_within_partition": duplicate_ids,
        "surveys": {},
    }
    for survey in surveys:
        report["surveys"][survey] = {
            **dict(per_survey[survey]),
            "quality": dict(quality[survey]),
        }
    if advertised is not None and advertised != total:
        report["error"] = f"properties hats_nrows={advertised} but partitions sum to {total}"
    if null_ids:
        report["error"] = f"{null_ids} anchor rows have a null {id_column}"
    if expected_rows is not None and total != expected_rows:
        report["error"] = f"expected {expected_rows} rows, found {total}"
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path, help="ray-anchor HATS output directory")
    parser.add_argument("--id-column", default="source_id")
    parser.add_argument("--expected-rows", type=int)
    parser.add_argument("--json", type=Path, help="write the full report as JSON")
    parser.add_argument("--progress-every", type=int, default=100)
    args = parser.parse_args(argv)

    report = validate(
        args.output,
        id_column=args.id_column,
        expected_rows=args.expected_rows,
        progress_every=args.progress_every,
    )
    if args.json:
        args.json.parent.mkdir(parents=True, exist_ok=True)
        args.json.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps(report, indent=2, sort_keys=True))
    return 1 if "error" in report else 0


if __name__ == "__main__":
    raise SystemExit(main())
