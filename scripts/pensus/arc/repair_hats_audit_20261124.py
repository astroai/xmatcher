#!/usr/bin/env python3
"""Stage or apply verified, catalogue-specific repairs from the 2026-11-24 audit."""

from __future__ import annotations

import argparse
import csv
import fcntl
import hashlib
import json
import os
import shutil
import stat
import tempfile
import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Any

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

if TYPE_CHECKING:
    from collections.abc import Iterable

AUDIT = Path(__file__).parents[1] / "audits" / "hats_survey_audit_20261124"
HATS = Path("/arc/projects/hats")
WORK = Path("/arc/projects/astroai/pensus")
COORDS = {
    "delve/dr3_gold": ("RA", "DEC"),
    "ps1/otmo": ("raMean", "decMean"),
    "des/y6_gold_prera_backup": ("RA", "DEC"),
    "uhs/dr3_prera_backup": ("ra", "dec"),
}
ATLAS = "atlas/dr5"
PHOTOZ = "lsst/dp1/object_photoz"
GAAP = "unions/gaap_sp_ugriz_photoz_ext"
INDEX = "_healpix_29"


def read_json(path: Path) -> Any:
    return json.loads(path.read_text())


def write_json(path: Path, value: object) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(value, indent=2) + "\n")
    tmp.replace(path)


def signature(path: Path) -> tuple[int, int, int, int] | None:
    if path.is_symlink():
        raise ValueError(f"Refusing symlink {path}")
    try:
        info = path.stat()
    except FileNotFoundError:
        return None
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns)


def digest(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


class Repair:
    """Track every input and stage complete replacements outside the live tree."""

    def __init__(self, root: Path, run: Path):
        self.root = root
        self.run = run
        self.inputs: dict[Path, tuple[int, int, int, int] | None] = {}
        self.outputs: dict[Path, Path] = {}
        self.details: dict[str, Any] = {}

    def track(self, relative: Path) -> Path:
        live = self.root / relative
        if not live.resolve().is_relative_to(self.root):
            raise ValueError(f"Path escapes approved catalogue root: {live}")
        self.inputs.setdefault(relative, signature(live))
        return live

    def stage(self, relative: Path) -> Path:
        self.track(relative)
        staged = self.run / "staged" / relative
        staged.parent.mkdir(parents=True, exist_ok=True)
        self.outputs[relative] = staged
        return staged

    def verify_inputs(self) -> None:
        for relative, expected in self.inputs.items():
            if signature(self.root / relative) != expected:
                raise ValueError(f"Live input changed during preparation: {relative}")

    def snapshot(self) -> dict[str, Any]:
        self.verify_inputs()
        return {
            "root": str(self.root),
            "inputs": {str(name): state for name, state in self.inputs.items()},
            "changes": {str(name): digest(path) for name, path in self.outputs.items()},
            "details": self.details,
        }

    def publish(self, plan: dict[str, Any]) -> None:
        self.verify_inputs()
        for relative in self.outputs:
            if self.inputs[relative] is not None:
                backup = self.run / "original" / relative
                backup.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(self.root / relative, backup)
        installed = []
        try:
            for relative, staged in self.outputs.items():
                target = self.root / relative
                if signature(target) != self.inputs[relative]:
                    raise ValueError(f"Live target changed before publication: {target}")
                if digest(staged) != plan["changes"][str(relative)]:
                    raise ValueError(f"Staged file changed: {staged}")
                install_file(staged, target)
                installed.append(relative)
            for relative in self.outputs:
                if digest(self.root / relative) != plan["changes"][str(relative)]:
                    raise ValueError(f"Published file differs from staged file: {relative}")
        except BaseException:
            for relative in reversed(installed):
                target = self.root / relative
                if digest(target) != plan["changes"][str(relative)]:
                    raise ValueError(f"Concurrent edit prevents rollback: {target}") from None
                restore_file(self.run, relative, target)
            raise


def install_file(source: Path, target: Path) -> None:
    fd, name = tempfile.mkstemp(prefix=f".{target.name}.audit-", dir=target.parent)
    os.close(fd)
    tmp = Path(name)
    try:
        shutil.copyfile(source, tmp)
        if target.is_file():
            # Inherit the live file's group and mode, including ACLs.
            info = target.stat()
            os.chown(tmp, -1, info.st_gid)
            shutil.copystat(target, tmp)
        else:
            # Keep the directory's inherited ACL and grant its group write access.
            tmp.chmod(stat.S_IMODE(target.parent.stat().st_mode) & 0o666)
        os.utime(tmp, None)
        with tmp.open("rb") as stream:
            os.fsync(stream.fileno())
        tmp.replace(target)
    finally:
        tmp.unlink(missing_ok=True)


def restore_file(run: Path, relative: Path, target: Path) -> None:
    backup = run / "original" / relative
    if backup.is_file():
        install_file(backup, target)
    else:
        target.unlink()


@contextmanager
def catalogue_lock(catalogue: str) -> Iterator[None]:
    directory = WORK / "repairs" / "locks"
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / catalogue.replace("/", "__")).open("a") as stream:
        fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield


def partitions(repair: Repair) -> dict[tuple[int, int], Path]:
    with repair.track(Path("partition_info.csv")).open() as stream:
        keys = [(int(r["Norder"]), int(r["Npix"])) for r in csv.DictReader(stream)]
    if not keys or len(keys) != len(set(keys)):
        raise ValueError("Empty or duplicate partition_info.csv")
    if any(order < 0 or order > 29 or not 0 <= pix < 12 * 4**order for order, pix in keys):
        raise ValueError("Invalid HEALPix partition")
    result = {
        (order, pix): Path(f"dataset/Norder={order}/Dir={pix // 10000 * 10000}/Npix={pix}.parquet")
        for order, pix in sorted(keys)
    }
    actual = {p.relative_to(repair.root) for p in repair.root.glob("dataset/Norder=*/Dir=*/Npix*")}
    if actual != set(result.values()):
        raise ValueError("Live partitions differ from partition_info.csv or expected flat layout")
    return result


def read_properties(path: Path) -> dict[str, str]:
    return dict(
        line.split("=", 1)
        for line in path.read_text().splitlines()
        if "=" in line and not line.lstrip().startswith(("#", "!"))
    )


def update_properties(repair: Repair, changes: dict[str, str]) -> int:
    changed = 0
    for name in ("hats.properties", "properties"):
        relative = Path(name)
        original = repair.track(relative)
        if not original.is_file():
            continue
        props = read_properties(original)
        add = {k: v for k, v in changes.items() if k not in props}
        conflicts = {k for k, v in changes.items() if k in props and props[k] != v}
        if conflicts:
            raise ValueError(f"Unexpected existing properties in {original}: {conflicts}")
        if add:
            text = original.read_text().rstrip("\n") + "\n"
            text += "".join(f"{k}={v}\n" for k, v in add.items())
            repair.stage(relative).write_text(text)
            changed += 1
    return changed


def audit_pixels(catalogue: str) -> dict[tuple[int, int], dict[str, Any]]:
    name = catalogue.replace("/", "__")
    fullscan = AUDIT / "fullscan" / (name + ".json")
    if fullscan.is_file():
        issues = read_json(fullscan)["bad_partitions"]
    else:
        report = read_json(AUDIT / "structure" / (name + ".json"))
        issues = report["coord_stats_out_of_range"]
    pixels = {}
    for issue in issues:
        order, pix = map(int, issue["partition"].split("/"))
        pixels[order, pix] = issue
    return pixels


def healpix29(ra: np.ndarray, dec: np.ndarray) -> np.ndarray:
    from astropy import units as u
    from cdshealpix import lonlat_to_healpix

    return lonlat_to_healpix(ra * u.deg, dec * u.deg, depth=29, num_threads=1)


def corrected_ra(
    batch: pa.Table, ra_name: str, dec_name: str, pixel: tuple[int, int]
) -> tuple[pa.Table, int]:
    ra = batch[ra_name].to_numpy()
    dec = batch[dec_name].to_numpy()
    if not np.isfinite(ra).all() or not (np.isfinite(dec) & (np.abs(dec) <= 90)).all():
        raise ValueError(f"Invalid coordinates in {pixel}")
    affected = (ra < 0) | (ra >= 360)
    count = int(affected.sum())
    if not count:
        return batch, 0
    old = ra[affected]
    if np.any((old < -1e-4) | (old > 360.0001)):
        raise ValueError(f"RA is not a rounding-sized wrap in {pixel}")
    wrapped = np.remainder(old, 360)
    index = healpix29(wrapped, dec[affected])
    order, pix = pixel
    shift = 2 * (29 - order)
    native = batch[INDEX].to_numpy()[affected]
    if not ((index >> shift == pix) & (native >> shift == pix)).all():
        raise ValueError(f"Repair would move a source across partition {pixel}")
    # Leave the stored order-29 key intact. PS1 may index another centroid.
    fixed = ra.copy()
    fixed[affected] = wrapped
    field = batch.schema.field(ra_name)
    return batch.set_column(batch.schema.get_field_index(ra_name), field, pa.array(fixed)), count


def check_readable(path: Path, columns: list[str] | None = None) -> int:
    reader = pq.ParquetFile(path, page_checksum_verification=True)
    rows = sum(
        batch.num_rows
        for batch in reader.iter_batches(batch_size=65_536, columns=columns, use_threads=False)
    )
    if rows != reader.metadata.num_rows:
        raise ValueError(f"Decoded row count differs from footer: {path}")
    return rows


def rewrite(
    source: pq.ParquetFile,
    sink: Path,
    schema: pa.Schema,
    transform: Any,
) -> int:
    changed = 0
    with pq.ParquetWriter(sink, schema, compression="zstd", write_page_checksum=True) as writer:
        # Retain row-group boundaries and row order.
        for i in range(source.num_row_groups):
            table, count = transform(source.read_row_group(i, use_threads=False))
            writer.write_table(table, row_group_size=max(1, table.num_rows))
            changed += count
    check_readable(sink)
    return changed


def metadata(repair: Repair, files: Iterable[Path], expected_rows: int) -> None:
    data = None
    schema = None
    for relative in files:
        live = repair.track(relative)
        footer = pq.ParquetFile(repair.outputs.get(relative, live))
        if schema is None:
            schema = footer.schema_arrow
        elif not schema.equals(footer.schema_arrow):
            raise ValueError(f"Incompatible schema at {relative}")
        part = footer.metadata
        part.set_file_path(relative.relative_to("dataset").as_posix())
        if data is None:
            data = part
        else:
            data.append_row_groups(part)
    if data is None or schema is None or data.num_rows != expected_rows:
        raise ValueError(f"Aggregate row count is not {expected_rows}")
    dataset = Path("dataset")
    aggregate = repair.stage(dataset / "_metadata")
    data.write_metadata_file(aggregate)
    if pq.read_metadata(aggregate).num_rows != expected_rows:
        raise ValueError("Aggregate metadata failed round trip")
    common = dataset / "_common_metadata"
    existing = repair.track(common)
    if existing.is_file():
        if not pq.read_schema(existing).equals(schema):
            raise ValueError("Existing _common_metadata differs from repaired schema")
    else:
        pq.write_metadata(schema, repair.stage(common))


def catalogue_rows(repair: Repair) -> int:
    totals = {
        int(read_properties(p)["hats_nrows"])
        for name in ("hats.properties", "properties")
        if (p := repair.track(Path(name))).is_file()
    }
    if len(totals) != 1:
        raise ValueError("Properties do not agree on the catalogue row count")
    return totals.pop()


def repair_coords(repair: Repair, catalogue: str) -> None:
    ra, dec = COORDS[catalogue]
    files = partitions(repair)
    changes = {}
    for pixel, issue in audit_pixels(catalogue).items():
        relative = files[pixel]
        reader = pq.ParquetFile(repair.track(relative), page_checksum_verification=True)
        schema = reader.schema_arrow
        if not {ra, dec, INDEX}.issubset(schema.names):
            raise ValueError(f"Missing audited fields in {relative}")
        count = sum(
            corrected_ra(pa.Table.from_batches([batch]), ra, dec, pixel)[1]
            for batch in reader.iter_batches(columns=[ra, dec, INDEX], use_threads=False)
        )
        if not count:
            continue
        max_count = issue.get("out_of_range", 1)
        if count > max_count:
            raise ValueError(f"New coordinate defects in {relative}: {count} > {max_count}")
        rewritten = rewrite(
            reader,
            repair.stage(relative),
            schema,
            partial(corrected_ra, ra_name=ra, dec_name=dec, pixel=pixel),
        )
        if rewritten != count:
            raise ValueError(f"Unexpected number of coordinate replacements in {relative}")
        changes[str(relative)] = count
    if changes:
        metadata(repair, files.values(), catalogue_rows(repair))
    repair.details["corrected_ra"] = changes


def repair_atlas(repair: Repair, source: Path) -> None:
    files = partitions(repair)
    other = Repair(source, repair.run)
    if partitions(other) != files:
        raise ValueError("ATLAS recovery source has a different partition tree")
    aggregate = pq.read_metadata(repair.track(Path("dataset/_metadata")))
    expected: dict[Path, int] = {}
    for i in range(aggregate.num_row_groups):
        group = aggregate.row_group(i)
        relative = Path("dataset") / group.column(0).file_path
        expected[relative] = expected.get(relative, 0) + group.num_rows
    if set(expected) != set(files.values()) or sum(expected.values()) != catalogue_rows(repair):
        raise ValueError("ATLAS aggregate does not describe the audited input")
    schema = pq.read_schema(repair.track(Path("dataset/_common_metadata")))
    bad = audit_pixels(ATLAS)
    for pixel, relative in files.items():
        path = other.track(relative)
        footer = pq.ParquetFile(path)
        if footer.metadata.num_rows != expected[relative] or not footer.schema_arrow.equals(schema):
            raise ValueError(f"ATLAS source does not reproduce {relative}")
        if pixel not in bad:
            continue
        live = repair.track(relative)
        try:
            if check_readable(live) == expected[relative]:
                continue
        except (OSError, ValueError):
            pass
        replacement = repair.stage(relative)
        shutil.copyfile(path, replacement)
        if check_readable(replacement) != expected[relative]:
            raise ValueError(f"ATLAS recovery does not decode {relative}")
    other.verify_inputs()
    if repair.outputs:
        metadata(repair, files.values(), catalogue_rows(repair))
    repair.details["recovery_source"] = str(source)


def repair_gaap(repair: Repair) -> None:
    files = partitions(repair)
    if (len(files), max(order for order, _ in files)) != (1650, 10):
        raise ValueError("GAAP tree changed since the audit")
    ra, dec = "ALPHA_J2000", "DELTA_J2000"
    full = None
    missing = []
    for pixel, relative in files.items():
        path = repair.track(relative)
        reader = pq.ParquetFile(path, page_checksum_verification=True)
        schema = reader.schema_arrow
        if not {ra, dec, INDEX}.issubset(schema.names):
            raise ValueError(f"No validated coordinate columns in {path}")
        if "N_EPOCH" in schema.names:
            if full is None:
                full = schema
            elif not full.equals(schema):
                raise ValueError(f"Unexpected GAAP schema at {path}")
        else:
            missing.append((relative, schema))
        order, pix = pixel
        for batch in reader.iter_batches(columns=[ra, dec, INDEX], use_threads=False):
            lon, lat, native = (batch[name].to_numpy() for name in (ra, dec, INDEX))
            if not np.all(
                np.isfinite(lon) & (lon >= 0) & (lon < 360) & np.isfinite(lat) & (np.abs(lat) <= 90)
            ):
                raise ValueError(f"Invalid GAAP coordinates in {path}")
            index = healpix29(lon, lat)
            if not np.array_equal(native, index) or not (index >> (2 * (29 - order)) == pix).all():
                raise ValueError(f"GAAP coordinates differ from the on-disk partition: {path}")
    if full is None or not full.field("N_EPOCH").nullable or len(missing) not in {0, 20}:
        raise ValueError("GAAP missing N_EPOCH subset has changed")
    field = full.field("N_EPOCH")
    position = full.get_field_index(field.name)
    for relative, schema in missing:
        if not full.remove(position).equals(schema):
            raise ValueError(f"Unexpected GAAP schema delta at {relative}")
        rewrite(
            pq.ParquetFile(repair.track(relative)),
            repair.stage(relative),
            full,
            lambda table: (
                table.add_column(position, field, pa.nulls(table.num_rows, field.type)),
                0,
            ),
        )
    rows = 637129635
    metadata(repair, files.values(), rows)
    relative = Path("hats.properties")
    props = {
        "obs_collection": "gaap_sp_ugriz_photoz_ext",
        "dataproduct_type": "object",
        "hats_nrows": str(rows),
        "hats_col_ra": ra,
        "hats_col_dec": dec,
        "hats_col_healpix": INDEX,
        "hats_col_healpix_order": "29",
        "hats_npix_suffix": ".parquet",
        "hats_order": "10",
        "hats_version": "v1.0",
        "hats_builder": "xmatcher audit recovery of existing UNIONS partitions",
        "hats_creation_date": datetime.now(UTC).strftime("%Y-%m-%dT%H:%MUTC"),
    }
    if repair.track(relative).exists():
        actual = read_properties(repair.track(relative))
        stable = set(props) - {"hats_creation_date"}
        if any(actual.get(k) != props[k] for k in stable):
            raise ValueError("GAAP properties were created by another workflow")
    else:
        repair.stage(relative).write_text(
            "".join(f"{name}={value}\n" for name, value in props.items())
        )
    repair.details["filled_N_EPOCH_partitions"] = len(missing)


def apply_repair(cat: str, run: Path, source: Path | None, *, apply: bool) -> None:
    root = HATS / cat
    if root.resolve() != root or not root.is_dir():
        raise ValueError(f"Expected a live catalogue directory at {root}")
    repair = Repair(root, run)
    if cat in COORDS:
        repair_coords(repair, cat)
    elif cat == PHOTOZ:
        order = max(order for order, _ in partitions(repair))
        if not update_properties(repair, {"hats_order": str(order)}) and not any(
            (root / name).is_file() for name in ("properties", "hats.properties")
        ):
            raise ValueError("LSST catalogue properties are missing")
    elif cat == ATLAS:
        if source is None:
            raise ValueError("Pass --atlas-source from the verified staged rebuild")
        repair_atlas(repair, source.resolve())
    elif cat == GAAP:
        repair_gaap(repair)
    plan = repair.snapshot()
    write_json(run / "plan.json", plan)
    if apply:
        repair.publish(plan)
    write_json(
        run / "result.json",
        {"catalogue": cat, "status": "applied" if apply else "staged", **plan},
    )


def rollback(cat: str, run: Path) -> None:
    plan = read_json(run / "plan.json")
    root = HATS / cat
    if plan["root"] != str(root) or root.resolve() != root:
        raise ValueError("Rollback plan does not match the requested catalogue")
    restore = []
    for name, new_hash in plan["changes"].items():
        relative = Path(name)
        target = root / relative
        if not target.resolve().is_relative_to(root):
            raise ValueError(f"Rollback path escapes catalogue: {target}")
        state = signature(target)
        if state is not None and digest(target) == new_hash:
            if plan["inputs"][name] and not (run / "original" / relative).is_file():
                raise ValueError(f"Missing original backup for {target}")
            restore.append(relative)
        elif plan["inputs"][name] != (list(state) if state else None):
            backup = run / "original" / relative
            if not backup.is_file() or not target.is_file() or digest(backup) != digest(target):
                raise ValueError(f"Concurrent edit prevents rollback: {target}")
    for relative in reversed(restore):
        restore_file(run, relative, root / relative)
    write_json(run / "rollback.json", {"catalogue": cat, "restored": list(map(str, restore))})


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("catalogue", choices=sorted([*COORDS, ATLAS, PHOTOZ, GAAP]))
    parser.add_argument("--run", type=Path, required=True)
    parser.add_argument("--atlas-source", type=Path)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--apply", action="store_true")
    mode.add_argument("--rollback", action="store_true")
    args = parser.parse_args()
    run = args.run.resolve()
    if not run.is_relative_to(WORK / "repairs"):
        parser.error("--run must be underneath /arc/projects/astroai/pensus/repairs")
    with catalogue_lock(args.catalogue):
        if args.rollback:
            rollback(args.catalogue, run)
            return
        run.mkdir(parents=True)
        start = time.monotonic()
        try:
            apply_repair(args.catalogue, run, args.atlas_source, apply=args.apply)
        except BaseException as exc:
            write_json(
                run / "error.json", {"error": repr(exc), "seconds": time.monotonic() - start}
            )
            raise


if __name__ == "__main__":
    main()
