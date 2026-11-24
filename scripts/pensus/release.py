#!/usr/bin/env python3
"""Plan, run and verify a PENSUS full-sky union from mounted HATS catalogues."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import math
import os
import re
import shutil
import tempfile
import time
from importlib.metadata import version
from pathlib import Path

import polars as pl
import yaml

from xmatcher import astro_utils, community, matchers, ray_union
from xmatcher.community import PROPERTIES_NAMES, read_properties
from xmatcher.exceptions import CrossMatchError
from xmatcher.ray_union import _hats_dir, _write_hats_tree

from . import release_match
from .paths import default_paths
from .release_match import (
    COMPOSITE_ID,
    CORE_SCHEMA,
    INDEX,
    NATIVE_INDEX,
    Survey,
    match_tile,
    output_columns,
    signature,
)
from .tile_geometry import tile_ids
from .tile_markers import file_sha256

PLAN = "plan.json"
READY = "PENSUS_RELEASE.json"
SLUG = re.compile(r"[a-z][a-z0-9_]*")
VERSION = 1


def _json(data: dict) -> str:
    return json.dumps(data, sort_keys=True, allow_nan=False, separators=(",", ":")) + "\n"


def _digest(data: dict) -> str:
    return hashlib.sha256(_json(data).encode()).hexdigest()


def _write_json(path: Path, data: dict, *, create: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, prefix=".pensus-", delete=False) as handle:
        tmp = Path(handle.name)
        try:
            handle.write(_json(data).encode())
            handle.close()
            if create:
                os.link(tmp, path)
            else:
                os.replace(tmp, path)
        finally:
            tmp.unlink(missing_ok=True)


def _runtime(use_motion: bool) -> dict:
    packages = ["polars", "pyarrow", "numpy", "scipy", "cdshealpix", "xmatcher"]
    if use_motion:
        packages.extend(["torchsky", "torch"])
    return {
        "code": {"release": file_sha256(Path(__file__))}
        | {
            module.__name__: file_sha256(Path(module.__file__))
            for module in (release_match, astro_utils, community, matchers, ray_union)
        },
        "packages": {name: version(name) for name in packages},
    }


def inventory(hats_root: Path) -> dict:
    """Inventory catalogue roots without opening data partitions."""
    trees, inaccessible = {}, []
    # Preserve a mounted project root as a dependency. A missing mount must
    # never turn into an apparently complete empty inventory.
    if not hats_root.is_dir():
        raise ValueError(f"HATS root is missing: {hats_root}")

    def walk(path: Path) -> None:
        rel = path.relative_to(hats_root).as_posix()
        try:
            entries = {entry.name: entry for entry in path.iterdir()}
        except PermissionError:
            inaccessible.append(rel)
            return
        if any(name in entries for name in PROPERTIES_NAMES):
            props = read_properties(path)
            if props is None:
                inaccessible.append(rel)
            elif props.get("dataproduct_type", "object").lower() == "object":
                trees[rel] = {
                    key: props.get(key)
                    for key in ("obs_collection", "hats_nrows", "hats_col_ra", "hats_col_dec")
                }
            # Collections can contain primary, margin, index and other tables.
            if not props or props.get("dataproduct_type", "object").lower() == "object":
                return
        for name, entry in sorted(entries.items()):
            if name.startswith((".", "_", "Norder=", "Dir=")) or name == "dataset":
                continue
            if path == hats_root and name == "pensus":
                continue
            if entry.is_symlink():
                # Within-root aliases resolve to the same inventory product.
                # Record external or broken links for an explicit review.
                if not entry.resolve().is_relative_to(hats_root) or not entry.exists():
                    inaccessible.append(entry.relative_to(hats_root).as_posix())
            elif entry.is_dir():
                walk(entry)

    walk(hats_root)
    return {"catalogues": trees, "inaccessible": inaccessible}


def _path(root: Path, name: str) -> Path:
    relative = Path(name)
    if relative.is_absolute() or ".." in relative.parts:
        raise ValueError(f"HATS path must be relative to {root}: {name}")
    path = (root / relative).resolve()
    if not path.is_relative_to(root):
        raise ValueError(f"HATS path escapes {root}: {path}")
    return path


def _number(value: object, name: str, *, positive: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (float, int)) or not math.isfinite(value):
        raise ValueError(f"{name} must be a finite number")
    if positive and value <= 0:
        raise ValueError(f"{name} must be positive")
    return float(value)


def _partition_files(root: Path, depth: int) -> list:
    base = root / "dataset" if (root / "dataset").is_dir() else root
    grouped = {}
    for path in sorted(base.glob("Norder=*/Dir=*/Npix=*")):
        order = int(path.parent.parent.name.split("=")[1])
        pixel = int(path.name.split("=")[1].removesuffix(".parquet"))
        if not 0 <= order <= depth or not 0 <= pixel < 12 * 4**order:
            raise ValueError(f"Invalid partition: {path}")
        files = sorted(path.glob("*.parquet")) if path.is_dir() else [path]
        if not files or any(f.suffix != ".parquet" for f in files) or (order, pixel) in grouped:
            raise ValueError(f"Invalid or duplicate HATS partition: {path}")
        grouped[order, pixel] = [[f.relative_to(root).as_posix(), signature(f)] for f in files]
    if not grouped:
        raise ValueError(f"No HATS Parquet files in {root}")
    for name in (
        "partition_info.csv",
        "partition_info.parquet",
        "dataset/partition_info.parquet",
        "dataset/partition_info.csv",
    ):
        info = root / name
        if info.is_file():
            index = (pl.read_parquet if info.suffix == ".parquet" else pl.read_csv)(info)
            pixels = index.select("Norder", "Npix").rows()
            if len(pixels) != len(grouped) or set(pixels) != grouped.keys():
                raise ValueError(f"Partition index and files disagree in {root}")
            break
    parts = sorted(grouped, key=lambda p: p[1] << (2 * (depth - p[0])))
    end = 0
    result = []
    for order, pixel in parts:
        shift = 2 * (depth - order)
        if pixel << shift < end:
            raise ValueError(f"Overlapping HATS partitions in {root}")
        end = (pixel + 1) << shift
        result.append([order, pixel, grouped[order, pixel]])
    return result


def _survey(spec: dict, hats_root: Path, target: float | None) -> dict:
    tag, release = spec["tag"], spec["release"]
    if not SLUG.fullmatch(tag) or not SLUG.fullmatch(release):
        raise ValueError(
            "Survey tag and release must use lowercase letters, numbers and underscores"
        )
    root = _path(hats_root, spec["path"])
    props = read_properties(root)
    if not props or props.get("dataproduct_type", "object").lower() != "object":
        raise ValueError(f"Expected a HATS object catalogue: {root}")
    if props.get("hats_ordering", "NESTED").upper() != "NESTED":
        raise ValueError(f"Expected NESTED HATS partitions: {root}")
    depth = int(props.get("hats_max_depth", 29))
    rows = int(props["hats_nrows"])
    if rows <= 0 or not 0 <= depth <= 29:
        raise ValueError(f"Invalid row count or max depth in {root}")
    parts = _partition_files(root, depth)
    schema = pl.read_parquet_schema(root / parts[0][2][0][0])
    ra = spec.get("ra", props.get("hats_col_ra"))
    dec = spec.get("dec", props.get("hats_col_dec"))
    keys = [spec["id"]] if isinstance(spec["id"], str) else spec["id"]
    if not isinstance(keys, list) or not keys or len({ra, dec, *keys}) != 2 + len(keys):
        raise ValueError(f"{release}: distinct RA, Dec and ID columns are required")
    if (
        not ra
        or not dec
        or any(
            key not in schema
            or not (schema[key].is_integer() or schema[key] in (pl.String, pl.Binary))
            for key in keys
        )
    ):
        raise ValueError(f"{release}: native ID columns must be integers, strings or binary")
    native_id = keys[0] if len(keys) == 1 else COMPOSITE_ID
    if reserved := {COMPOSITE_ID, NATIVE_INDEX} & schema.keys():
        raise ValueError(f"{release}: reserved input columns {sorted(reserved)}")
    columns = spec.get("columns", [])
    if not isinstance(columns, list) or any(not isinstance(c, str) or not c for c in columns):
        raise ValueError(f"{release}: columns must list concrete column names")
    bands = spec.get("bands", {})
    for name, band in bands.items():
        if not isinstance(band, dict) or any(
            not isinstance(band.get(key), str) or not band[key]
            for key in ("value", "unit", "filter")
        ):
            raise ValueError(f"{release}/{name}: value, unit and filter are required")
        columns = [*columns, band["value"], *band.get("flags", [])]
        if band.get("uncertainty"):
            columns.append(band["uncertainty"])
        if band.get("uncertainty") and not band.get("uncertainty_kind"):
            raise ValueError(f"{release}/{name}: uncertainty_kind is required")
    if not columns:
        raise ValueError(f"{release}: select photometry columns or declare bands")
    columns = list(dict.fromkeys([*keys, ra, dec, *columns]))
    epoch = spec.get("epoch_jyear")
    if epoch is not None:
        epoch = _number(epoch, f"{release}.epoch_jyear")
    pm = spec.get("pm", [])
    motion = _number(spec.get("motion_bound_arcsec", 0), f"{release}.motion_bound_arcsec")
    if pm:
        if (
            not isinstance(pm, list)
            or len(pm) != 2
            or len(set(pm)) != 2
            or target is None
            or epoch is None
            or motion <= 0
        ):
            raise ValueError(f"{release}: PM requires two columns, epochs and a positive bound")
    elif motion != 0:
        raise ValueError(f"{release}: motion bound requires PM columns")
    filter_index = spec.get("filter_index", schema.get(INDEX) in (pl.Int64, pl.UInt64))
    if not isinstance(filter_index, bool) or (
        filter_index and schema.get(INDEX) not in (pl.Int64, pl.UInt64)
    ):
        raise ValueError(f"{release}: filter_index requires a 64-bit {INDEX} column")
    read_columns = list(dict.fromkeys([*columns, *pm, *([INDEX] if filter_index else [])]))
    if missing := set(read_columns) - schema.keys():
        raise ValueError(f"{release}: projected columns are missing: {sorted(missing)}")
    for column in [ra, dec, *pm]:
        if not schema[column].is_numeric():
            raise ValueError(f"{release}: {column} must be numeric")
    survey = {
        "tag": tag,
        "release": release,
        "path": spec["path"],
        "root": str(root),
        "rows": rows,
        "depth": depth,
        "parts": parts,
        "id": native_id,
        "id_columns": keys,
        "ra": ra,
        "dec": dec,
        "columns": columns if len(keys) == 1 else [COMPOSITE_ID, *columns],
        "read_columns": read_columns,
        "bands": bands,
        "epoch": epoch,
        "pm": pm,
        "motion_bound_arcsec": motion,
        "filter_index": filter_index,
        "schema": {c: str(schema[c]) for c in read_columns},
        "metadata_files": {
            name: file_sha256(root / name)
            for name in (
                *PROPERTIES_NAMES,
                "partition_info.csv",
                "partition_info.parquet",
                "dataset/partition_info.csv",
                "dataset/partition_info.parquet",
            )
            if (root / name).is_file()
        },
    }
    survey["column_map"] = output_columns(survey)
    return survey


def build_plan(specification: Path, hats_root: Path, *, pilot: bool = False) -> dict:
    spec = yaml.safe_load(specification.read_text())
    order = spec["order"]
    if type(order) is not int or not 0 <= order <= 29:
        raise ValueError("order must be a HEALPix order from 0 to 29")
    target = spec.get("target_epoch_jyear")
    if target is not None:
        target = _number(target, "target_epoch_jyear")
    radius = _number(spec["radius_arcsec"], "radius_arcsec", positive=True)
    hats_root = hats_root.resolve()
    candidates = inventory(hats_root)
    surveys = [_survey(s, hats_root, target) for s in spec["surveys"]]
    if not surveys:
        raise ValueError("Select at least one photometry catalogue")
    tags = [s["tag"] for s in surveys]
    releases = [s["release"] for s in surveys]
    roots = [s["root"] for s in surveys]
    if any(len(set(values)) != len(surveys) for values in (tags, releases, roots)):
        raise ValueError("A survey tag, release or HATS tree was selected more than once")
    selected = {Path(s["root"]).relative_to(hats_root).as_posix() for s in surveys}
    excluded = spec.get("exclude", {})
    if not isinstance(excluded, dict) or any(
        not isinstance(reason, str) or not reason.strip() for reason in excluded.values()
    ):
        raise ValueError("Each excluded inventory path requires a reason")
    exclusions = [_path(hats_root, name).relative_to(hats_root) for name in excluded]
    if any(Path(name).is_relative_to(exclusion) for name in selected for exclusion in exclusions):
        raise ValueError("A selected survey also falls under an exclusion")
    unreviewed = [
        name
        for name in sorted(set(candidates["catalogues"]) | set(candidates["inaccessible"]))
        if name not in selected and not any(Path(name).is_relative_to(p) for p in exclusions)
    ]
    if unreviewed and not pilot:
        raise ValueError("Review and classify these HATS inventory paths: " + ", ".join(unreviewed))
    output = list(CORE_SCHEMA)
    for survey in surveys:
        output.extend(output_columns(survey).values())
        output.append(f"sep_arcsec_{survey['tag']}")
    if len(output) != len(set(output)):
        raise ValueError("Survey projections produce colliding output names")
    overrides = dict(spec.get("pair_radii_arcsec", {}))
    radii = [
        [
            _number(overrides.pop(f"{a}/{b}", radius), f"{a}/{b}", positive=True)
            for b in tags[i + 1 :]
        ]
        for i, a in enumerate(tags)
    ]
    if overrides:
        raise ValueError(f"Unknown or reversed survey pairs: {sorted(overrides)}")
    if max((r for row in radii for r in row), default=radius) > 648_000:
        raise ValueError("Match radius cannot exceed 180 degrees")
    return {
        "version": VERSION,
        "pilot": pilot,
        "hats_root": str(hats_root),
        "order": order,
        "target_epoch_jyear": target,
        "radii": radii,
        "surveys": surveys,
        "excluded": excluded,
        "unreviewed": unreviewed,
        "inventory": candidates,
        "runtime": _runtime(any(s["pm"] for s in surveys)),
    }


def _load(root: Path, *, check_runtime: bool = True) -> tuple[dict, str]:
    payload = json.loads((root / PLAN).read_text())
    plan, digest = payload["plan"], payload["sha256"]
    if plan["version"] != VERSION or _digest(plan) != digest:
        raise ValueError("Plan contents or version differ from the pinned plan")
    if check_runtime and _runtime(any(s["pm"] for s in plan["surveys"])) != plan["runtime"]:
        raise ValueError("Worker code or runtime differs from the pinned plan")
    return plan, digest


def _loc(order: int, pixel: int) -> str:
    return f"Norder={order}/Dir={_hats_dir(pixel)}/Npix={pixel}"


def _dest(root: Path, order: int, pixel: int) -> Path:
    return root / "catalog" / "dataset" / f"{_loc(order, pixel)}.parquet"


def _marker(root: Path, pixel: int) -> Path:
    return root / "tiles" / f"Dir={_hats_dir(pixel)}" / f"{pixel}.json"


def _done(root: Path, digest: str, plan: dict, pixel: int) -> dict | None:
    try:
        marker = json.loads(_marker(root, pixel).read_text())
        if marker["plan"] != digest or marker["pixel"] != pixel:
            return None
        output = _dest(root, plan["order"], pixel)
        if type(marker["rows"]) is not int or marker["rows"] < 0:
            return None
        if marker["rows"] > 0:
            if output.stat().st_size != marker["bytes"] or not marker["sha256"]:
                return None
        elif marker["bytes"] or marker["sha256"] or output.exists():
            return None
        tags = {s["tag"] for s in plan["surveys"]}
        for field in ("members", "inputs"):
            tallies = marker[field]
            if set(tallies) != tags or any(
                len(values) != 3 or any(type(n) is not int or not 0 <= n < 2**64 for n in values)
                for values in tallies.values()
            ):
                return None
        if any(values[0] > marker["rows"] for values in marker["members"].values()):
            return None
        return marker
    except (OSError, ValueError, KeyError, TypeError):
        return None


def _run(root: Path, args: argparse.Namespace) -> None:
    plan, digest = _load(root)
    if root.resolve().is_relative_to(Path(plan["hats_root"])):
        raise ValueError("A draft cannot be written under the HATS survey root")
    depth = max(
        min(14, plan["order"] + 4),
        plan["order"],
        max(order for s in plan["surveys"] for order, _, _ in s["parts"]),
    )
    surveys = [Survey(s, depth) for s in plan["surveys"]]
    sky = tile_ids(plan["order"])
    if args.pixel is not None:
        pixels = args.pixel
        if args.start != 0 or args.stop is not None:
            raise ValueError("Use --pixel or --start/--stop")
    else:
        stop = sky.stop if args.stop is None else args.stop
        if not 0 <= args.start < stop <= sky.stop:
            raise ValueError("The tile range must lie inside the full sky")
        pixels = range(args.start, stop)
    memory_bytes = int(_number(args.pool_gib, "pool-gib", positive=True) * 1024**3)
    if args.scratch is not None:
        if args.scratch.resolve().is_relative_to(Path(plan["hats_root"])):
            raise ValueError("Scratch cannot be placed under the HATS survey root")
        args.scratch.mkdir(parents=True, exist_ok=True)
    for pixel in pixels:
        if pixel not in sky:
            raise ValueError(f"Tile {pixel} is outside order {plan['order']}")
        if _done(root, digest, plan, pixel) and not args.repair:
            continue
        marker = _marker(root, pixel)
        marker.parent.mkdir(parents=True, exist_ok=True)
        with marker.with_suffix(".lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            if (root / "catalog" / READY).exists():
                raise ValueError("Finalized releases are immutable")
            if _done(root, digest, plan, pixel) and not args.repair:
                continue
            dest = _dest(root, plan["order"], pixel)
            dest.parent.mkdir(parents=True, exist_ok=True)
            started = time.monotonic()
            with tempfile.TemporaryDirectory(
                dir=args.scratch or dest.parent, prefix=".pensus-tile-"
            ) as directory:
                scratch_file = Path(directory) / "tile.parquet"
                count, members, inputs, loaded = match_tile(
                    surveys, plan, pixel, scratch_file, memory_bytes
                )
                result = {
                    "plan": digest,
                    "pixel": pixel,
                    "rows": count,
                    "members": members,
                    "inputs": inputs,
                    "bytes": 0,
                    "sha256": None,
                    "pool_bytes": loaded,
                    "seconds": round(time.monotonic() - started, 3),
                }
                marker.unlink(missing_ok=True)
                if count:
                    result["bytes"] = scratch_file.stat().st_size
                    result["sha256"] = file_sha256(scratch_file)
                    if args.scratch is None:
                        os.replace(scratch_file, dest)
                    else:
                        with tempfile.TemporaryDirectory(
                            dir=dest.parent, prefix=".pensus-copy-"
                        ) as copy_dir:
                            copied = Path(copy_dir) / "tile.parquet"
                            shutil.copyfile(scratch_file, copied)
                            os.replace(copied, dest)
                else:
                    dest.unlink(missing_ok=True)
                _write_json(marker, result)
                print(_json(result), end="", flush=True)


def _status(
    root: Path, finalize: bool, *, deep: bool = False, aggregate_metadata: bool = False
) -> dict:
    plan, digest = _load(root, check_runtime=finalize)
    if finalize and root.resolve().is_relative_to(Path(plan["hats_root"])):
        raise ValueError("A draft cannot be written under the HATS survey root")
    if finalize and plan["pilot"]:
        raise ValueError("A pilot plan cannot be finalized as a full release")
    covered = tile_ids(plan["order"])
    missing_count = 0
    missing_sample: list[int] = []
    totals = {
        field: {s["tag"]: [0, 0, 0] for s in plan["surveys"]} for field in ("members", "inputs")
    }
    expected = {s["tag"]: s["rows"] for s in plan["surveys"]}
    parts = []
    rows = 0
    for pixel in covered:
        marker = _done(root, digest, plan, pixel)
        if marker is None:
            missing_count += 1
            if len(missing_sample) < 32:
                missing_sample.append(pixel)
            continue
        count = marker["rows"]
        rows += count
        for field, tallies in totals.items():
            for tag, values in marker[field].items():
                tallies[tag][0] += values[0]
                for i in (1, 2):
                    tallies[tag][i] = (tallies[tag][i] + values[i]) % 2**64
        if count and finalize:
            path = _dest(root, plan["order"], pixel)
            if deep and file_sha256(path) != marker["sha256"]:
                raise ValueError(f"SHA-256 mismatch for tile {pixel}")
            parts.append(
                {
                    "Norder": plan["order"],
                    "Dir": _hats_dir(pixel),
                    "Npix": pixel,
                    "Nfiles": 1,
                    "file_loc": _loc(plan["order"], pixel),
                    "file_size": marker["bytes"],
                    "count": count,
                }
            )
    member_counts = {tag: values[0] for tag, values in totals["members"].items()}
    report = {
        "plan_sha256": digest,
        "tiles": len(covered),
        "missing": missing_count,
        "missing_sample": missing_sample,
        "rows": rows,
        "members": member_counts,
        "expected": expected,
        "membership_checks": totals,
        "complete": missing_count == 0
        and member_counts == expected
        and totals["members"] == totals["inputs"],
    }
    if finalize:
        if not report["complete"]:
            raise ValueError(_json(report))
        for survey in plan["surveys"]:
            path = Path(survey["root"])
            if _partition_files(path, survey["depth"]) != survey["parts"] or any(
                file_sha256(path / name) != checksum
                for name, checksum in survey["metadata_files"].items()
            ):
                raise ValueError(f"Input changed after planning: {path}")
        catalog = root / "catalog"
        dataset = catalog / "dataset"
        files = {
            p.relative_to(dataset).as_posix() for p in dataset.glob("Norder=*/Dir=*/Npix=*.parquet")
        }
        if files != {f"{part['file_loc']}.parquet" for part in parts}:
            raise ValueError("Draft Parquet files differ from the completed tile inventory")
        props = {
            "obs_collection": root.name,
            "dataproduct_type": "object",
            "hats_col_ra": "ra",
            "hats_col_dec": "dec",
            "hats_ordering": "NESTED",
            "hats_col_healpix": INDEX,
            "hats_max_depth": str(plan["order"]),
            "hats_epoch": "J2000",
            "pensus_position_epoch_column": "position_epoch_jyear",
        }
        _write_hats_tree(
            catalog, parts, props, verify_counts=True, create_metadata=aggregate_metadata
        )
        _write_json(
            catalog / READY,
            {
                **report,
                "verified_sha256": deep,
                "aggregate_metadata": aggregate_metadata,
                "inputs": [{k: v for k, v in s.items() if k != "parts"} for s in plan["surveys"]],
                "excluded": plan["excluded"],
                "radii_arcsec": plan["radii"],
                "target_epoch_jyear": plan["target_epoch_jyear"],
                "runtime": plan["runtime"],
            },
            create=True,
        )
    return report


def _pending(root: Path, batch_size: int) -> dict:
    if batch_size < 1:
        raise ValueError("tiles-per-job must be positive")
    plan, digest = _load(root, check_runtime=False)
    end = tile_ids(plan["order"]).stop
    ranges = []
    for start in range(0, end, batch_size):
        stop = min(start + batch_size, end)
        if any(_done(root, digest, plan, pixel) is None for pixel in range(start, stop)):
            ranges.append({"start": start, "stop": stop})
    return {"plan_sha256": digest, "ranges": ranges}


def main(argv: list[str] | None = None) -> int:
    paths = default_paths()
    work_root = Path(os.environ.get("PENSUS_WORK_ROOT", "/arc/projects/astroai/pensus"))
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(required=True, dest="command")
    inv = commands.add_parser("inventory", help="List mounted HATS object products")
    inv.add_argument("--hats-root", type=Path, default=paths.hats_survey_root)
    plan = commands.add_parser(
        "plan", help="Pin inventory, projections, inputs and matching policy"
    )
    plan.add_argument("--spec", type=Path, required=True)
    plan.add_argument("--hats-root", type=Path, default=paths.hats_survey_root)
    plan.add_argument("--pilot", action="store_true")
    run = commands.add_parser("run", help="Match a tile or a contiguous range of tiles")
    run.add_argument("--pixel", type=int, action="append")
    run.add_argument("--start", type=int, default=0)
    run.add_argument("--stop", type=int)
    run.add_argument("--scratch", type=Path)
    run.add_argument("--pool-gib", type=float, default=4)
    run.add_argument(
        "--repair", action="store_true", help="Recompute selected tiles before finalization"
    )
    status = commands.add_parser("status", help="Check tile completion and survey row conservation")
    pending = commands.add_parser("pending", help="List tile ranges to submit or resume")
    pending.add_argument("--tiles-per-job", type=int, default=64)
    finalize = commands.add_parser("finalize", help="Seal a complete draft as a HATS catalogue")
    finalize.add_argument("--deep", action="store_true", help="Verify every output SHA-256")
    finalize.add_argument(
        "--aggregate-metadata",
        action="store_true",
        help="Also write the combined Parquet row-group footers for smaller releases",
    )
    for command in (plan, run, status, pending, finalize):
        command.add_argument(
            "--draft", type=Path, required=True, help=f"Draft under {work_root}/drafts/views"
        )
    args = parser.parse_args(argv)
    try:
        if args.command == "inventory":
            result = inventory(args.hats_root)
        elif args.command == "plan":
            draft = args.draft.resolve()
            if draft.is_relative_to(args.hats_root.resolve()):
                raise ValueError("A draft cannot be written under the HATS survey root")
            if any((draft / item).exists() for item in (PLAN, "catalog", "tiles")):
                raise ValueError(f"Choose a new draft directory: {draft}")
            data = build_plan(args.spec, args.hats_root, pilot=args.pilot)
            result = {"sha256": _digest(data), "plan": data}
            _write_json(draft / PLAN, result, create=True)
            result = {
                "sha256": result["sha256"],
                "draft": str(draft),
                "surveys": len(data["surveys"]),
            }
        elif args.command == "run":
            with (args.draft / PLAN).open("rb") as lock:
                fcntl.flock(lock, fcntl.LOCK_SH)
                _run(args.draft, args)
            return 0
        elif args.command == "finalize":
            with (args.draft / PLAN).open("rb") as lock:
                fcntl.flock(lock, fcntl.LOCK_EX)
                if (args.draft / "catalog" / READY).exists():
                    raise ValueError("Draft already finalized")
                result = _status(
                    args.draft, True, deep=args.deep, aggregate_metadata=args.aggregate_metadata
                )
        elif args.command == "pending":
            result = _pending(args.draft, args.tiles_per_job)
        else:
            result = _status(args.draft, False)
    except (
        CrossMatchError,
        ValueError,
        OSError,
        KeyError,
        TypeError,
        pl.exceptions.PolarsError,
    ) as exc:
        parser.exit(1, f"pensus: {exc}\n")
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
