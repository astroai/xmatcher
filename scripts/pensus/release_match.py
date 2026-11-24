"""Projected HATS reads and the source-preserving PENSUS cascade."""

from __future__ import annotations

import math
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path

import cdshealpix as hp
import numpy as np
import polars as pl
import pyarrow.parquet as pq
from scipy.spatial import cKDTree

from xmatcher.astro_utils import propagate_proper_motion
from xmatcher.matchers import _arcsec_to_chord, _chord_to_arcsec, _pixellate, _radec_to_xyz
from xmatcher.ray_union import _cone_ranges, _pixel_center_deg

INDEX = "_healpix_29"
COMPOSITE_ID = "_pensus_native_key"
NATIVE_INDEX = "_pensus_native_index"
CORE_SCHEMA = {
    "component_id": pl.String,
    "spine_release": pl.String,
    "ra": pl.Float64,
    "dec": pl.Float64,
    "position_epoch_jyear": pl.Float64,
    INDEX: pl.Int64,
}


def signature(path: Path) -> list[int]:
    info = path.stat()
    return [info.st_size, info.st_mtime_ns]


def output_columns(survey: dict) -> dict[str, str]:
    tag = survey["tag"]
    return {
        name: f"id_{tag}" if name == survey["id"] else f"{name}_{tag}" for name in survey["columns"]
    }


def key_text(ids: pl.Series) -> pl.Series:
    if ids.dtype == pl.Binary:
        return ids.bin.encode("hex")
    return ids.cast(pl.String)


def coverage(order: int, pixel: int, margin: float, depth: int) -> np.ndarray:
    # Bound parent-to-point distance by parent-to-child-center distance plus
    # the conservative 2/nside angular bound for each descendant cell.
    suborder = min(order + 3, 29)
    subshift = 2 * (suborder - order)
    children = np.arange(pixel << subshift, (pixel + 1) << subshift, dtype=np.uint64)
    lon, lat = hp.healpix_to_lonlat(children, suborder, num_threads=1)
    center_ra, center_dec = _pixel_center_deg(order, pixel)
    center = _radec_to_xyz(np.array([center_ra]), np.array([center_dec]))
    chords = np.linalg.norm(_radec_to_xyz(lon.deg, lat.deg) - center, axis=1)
    radius = (float(_chord_to_arcsec(chords.max())) + margin) / 3600.0
    radius += math.degrees(2.0) / 2**suborder
    if radius >= 180:
        return np.array([[0, 12 * 4**depth]], dtype=np.int64)
    cover_depth = min(depth, order + 5)
    shift = 2 * (depth - cover_depth)
    ranges: list[list[int]] = []
    for lo, hi in sorted(_cone_ranges(order, pixel, radius, cover_depth)):
        if ranges and lo <= ranges[-1][1]:
            ranges[-1][1] = max(hi, ranges[-1][1])
        else:
            ranges.append([lo, hi])
    return np.array(ranges, dtype=np.int64) << shift


@dataclass
class Survey:
    config: dict
    depth: int

    @cached_property
    def intervals(self) -> np.ndarray:
        return np.array(
            [
                (pix << (2 * (self.depth - order)), (pix + 1) << (2 * (self.depth - order)))
                for order, pix, _ in self.config["parts"]
            ],
            dtype=np.int64,
        )

    @cached_property
    def schema(self) -> dict[str, pl.DataType]:
        path = Path(self.config["root"]) / self.config["parts"][0][2][0][0]
        return pl.read_parquet_schema(path)

    def read(self, ranges: np.ndarray, memory_bytes: int) -> pl.DataFrame:
        cat = self.config
        chosen: set[int] = set()
        for lo, hi in ranges:
            first = np.searchsorted(self.intervals[:, 1], lo, side="right")
            last = np.searchsorted(self.intervals[:, 0], hi, side="left")
            chosen.update(range(first, last))
        columns = cat["read_columns"]
        dtype = {c: self.schema[c] for c in columns}
        loaded_dtype = {**dtype, NATIVE_INDEX: pl.Int64}
        if not chosen:
            return pl.DataFrame(schema=loaded_dtype)

        frames = []
        used = 0.0
        for idx in sorted(chosen):
            order, pixel, files = cat["parts"][idx]
            shift = 2 * (self.depth - order)
            start, end = pixel << shift, (pixel + 1) << shift
            for name, expected in files:
                path = Path(cat["root"]) / name
                if signature(path) != expected:
                    raise ValueError(f"Input changed after planning: {path}")
                scan = pl.scan_parquet(path, hive_partitioning=False, low_memory=True)
                if cat["filter_index"]:
                    # Limit predicates to ranges crossing this HATS partition.
                    # Fall back to its bounding interval for fragmented coverage.
                    bounds = ranges[(ranges[:, 0] < end) & (ranges[:, 1] > start)].copy()
                    bounds[:, 0] = np.maximum(bounds[:, 0], start)
                    bounds[:, 1] = np.minimum(bounds[:, 1], end)
                    if len(bounds) > 32:
                        bounds = np.array([[bounds[0, 0], bounds[-1, 1]]])
                    hp_shift = 2 * (29 - self.depth)
                    index = pl.col(INDEX)
                    scan = scan.filter(
                        pl.any_horizontal(
                            [
                                (index >= int(lo) << hp_shift) & (index < int(hi) << hp_shift)
                                for lo, hi in bounds
                            ]
                        )
                    )
                for frame in scan.select(columns).collect_batches(
                    chunk_size=65_536, engine="streaming"
                ):
                    if dict(frame.schema) != dtype:
                        raise ValueError(f"Projected schema differs at {path}")
                    ra = frame[cat["ra"]].cast(pl.Float64).to_numpy()
                    dec = frame[cat["dec"]].cast(pl.Float64).to_numpy()
                    if not (
                        np.isfinite(ra).all()
                        and np.isfinite(dec).all()
                        and (np.abs(dec) <= 90).all()
                    ):
                        raise ValueError(f"Invalid coordinates in {path}")
                    native = _pixellate(hp, ra % 360.0, dec, 29, label=str(path))
                    if cat["filter_index"] and not np.array_equal(native, frame[INDEX].to_numpy()):
                        raise ValueError(f"Coordinates and {INDEX} differ in {path}")
                    coarse = native >> (2 * (29 - self.depth))
                    if not ((coarse >= start) & (coarse < end)).all():
                        raise ValueError(f"Coordinates outside HATS partition in {path}")
                    i = np.searchsorted(ranges[:, 1], coarse, side="right")
                    keep = (i < len(ranges)) & (coarse >= ranges[np.minimum(i, len(ranges) - 1), 0])
                    if not keep.any():
                        continue
                    frame = frame.filter(keep).with_columns(pl.Series(NATIVE_INDEX, native[keep]))
                    used += frame.estimated_size() + frame.height * 120
                    if used > memory_bytes:
                        raise ValueError(f"Projected tile pool exceeds its memory budget at {path}")
                    frames.append(frame)
                if signature(path) != expected:
                    raise ValueError(f"Input changed while reading: {path}")
        return pl.concat(frames) if frames else pl.DataFrame(schema=loaded_dtype)


@dataclass
class Pool:
    frame: pl.DataFrame
    ids: np.ndarray
    xyz: np.ndarray
    ra: np.ndarray
    dec: np.ndarray
    index: np.ndarray
    native_index: np.ndarray
    epoch: np.ndarray
    alive: np.ndarray


def read_pool(
    survey: Survey, ranges: np.ndarray, target_epoch: float | None, memory_bytes: int
) -> Pool:
    cat = survey.config
    frame = survey.read(ranges, memory_bytes)
    ra = frame[cat["ra"]].cast(pl.Float64).to_numpy() % 360.0
    dec = frame[cat["dec"]].cast(pl.Float64).to_numpy()
    native_index = frame[NATIVE_INDEX].to_numpy()
    keys = cat["id_columns"]
    if any(frame[c].null_count() for c in keys):
        raise ValueError(f"Null native ID in {cat['release']} tile pool")
    if cat["id"] == COMPOSITE_ID:
        fields = [
            pl.col(name).bin.encode("hex") if frame.schema[name] == pl.Binary else pl.col(name)
            for name in keys
        ]
        frame = frame.with_columns(pl.struct(fields).struct.json_encode().alias(COMPOSITE_ID))
    ids = key_text(frame[cat["id"]])
    if ids.null_count() or (ids.str.len_bytes() == 0).any() or ids.n_unique() != len(ids):
        raise ValueError(f"Null, empty or duplicate native ID in {cat['release']} tile pool")
    xyz = _radec_to_xyz(ra, dec)
    epoch = np.full(len(ra), np.nan if cat["epoch"] is None else cat["epoch"])
    if source := cat["epoch_source"]:
        raw_epoch = frame[source["column"]].cast(pl.Float64).to_numpy()
        measured = np.isfinite(raw_epoch) & ~np.isin(raw_epoch, source["missing_values"])
        values = raw_epoch[measured]
        fmt = source["format"]
        if fmt == "jd":
            values = 2000.0 + (values - 2_451_545.0) / 365.25
        elif fmt == "mjd":
            values = 2000.0 + (values - 51_544.5) / 365.25
        epoch[measured] = values
    index = native_index
    if cat["pm"] and len(ra):
        pm_ra, pm_dec = (frame[c].cast(pl.Float64).to_numpy() for c in cat["pm"])
        valid = np.isfinite(pm_ra) & np.isfinite(pm_dec) & np.isfinite(epoch)
        if valid.any():
            assert target_epoch is not None
            dec = dec.copy()
            ra[valid], dec[valid] = propagate_proper_motion(
                ra[valid], dec[valid], pm_ra[valid], pm_dec[valid], epoch[valid], target_epoch
            )
            moved_xyz = _radec_to_xyz(ra[valid], dec[valid])
            steps = _chord_to_arcsec(np.linalg.norm(moved_xyz - xyz[valid], axis=1))
            if not np.isfinite(steps).all() or steps.max() > cat["motion_bound_arcsec"]:
                raise ValueError(f"Proper motion exceeds the pinned bound for {cat['release']}")
            xyz[valid] = moved_xyz
            epoch[valid] = target_epoch
            index = native_index.copy()
            index[valid] = _pixellate(hp, ra[valid], dec[valid], 29, label=cat["release"])
    return Pool(
        frame, ids.to_numpy(), xyz, ra, dec, index, native_index, epoch, np.ones(len(ra), bool)
    )


def _nearest(query: np.ndarray, pool: np.ndarray, tree: cKDTree, limit: float) -> np.ndarray:
    # Pools are ordered by native ID. Resolve exact distance ties in that order,
    # including triples and identical coordinates.
    distances, indices = tree.query(query, k=2, distance_upper_bound=np.nextafter(limit, np.inf))
    nearest = indices[:, 0].copy()
    ties = np.flatnonzero((distances[:, 0] == distances[:, 1]) & (distances[:, 0] <= limit))
    for row in ties:
        nearby = tree.query_ball_point(query[row], np.nextafter(distances[row, 0], np.inf))
        nearest[row] = min(nearby, key=lambda index: (math.dist(query[row], pool[index]), index))
    return nearest


@dataclass
class Search:
    rows: np.ndarray
    xyz: np.ndarray

    @classmethod
    def build(cls, pool: Pool, rows: np.ndarray) -> Search:
        rows = rows[np.argsort(pool.ids[rows], kind="stable")]
        return cls(rows, pool.xyz[rows])

    @cached_property
    def tree(self) -> cKDTree:
        return cKDTree(self.xyz)


def reciprocal(
    left: Search, right: Pool, radius: float
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    r_indices = np.flatnonzero(right.alive)
    if not len(left.rows) or not len(r_indices):
        empty = np.empty(0, dtype=np.int64)
        return empty, empty, np.empty(0, dtype=float)
    other = Search.build(right, r_indices)
    l_xyz, r_xyz = left.xyz, other.xyz
    limit = _arcsec_to_chord(radius)
    lr = _nearest(l_xyz, r_xyz, other.tree, limit)
    called = np.flatnonzero(lr < len(r_indices))
    if not len(called):
        return called, called, np.empty(0, dtype=float)
    # A right source participates only if it was chosen by some left source.
    candidates, inverse = np.unique(lr[called], return_inverse=True)
    rl = _nearest(r_xyz[candidates], l_xyz, left.tree, limit)
    left_rows = called[rl[inverse] == called]
    right_rows = lr[left_rows]
    chords = np.linalg.norm(l_xyz[left_rows] - r_xyz[right_rows], axis=1)
    keep = chords <= limit
    return left.rows[left_rows[keep]], other.rows[right_rows[keep]], _chord_to_arcsec(chords[keep])


def calibration_candidates(
    pair: list[dict],
    plan: dict,
    pixel: int,
    radius: float,
    memory_bytes: int,
    max_rows: int,
    extra_columns: tuple[list[str], list[str]],
) -> pl.DataFrame:
    """Read pilot columns and retain the full cone plus unmatched primaries."""
    order = plan["order"]
    depth = max(order, min(29, order + 4), max(p[0] for cat in pair for p in cat["parts"]))
    pools = []
    for i, cat in enumerate(pair):
        columns = [*cat["id_columns"], cat["ra"], cat["dec"], *cat["pm"]]
        if source := cat["epoch_source"]:
            columns.append(source["column"])
        columns.extend(extra_columns[i])
        if cat["filter_index"]:
            columns.append(INDEX)
        survey = Survey({**cat, "read_columns": list(dict.fromkeys(columns))}, depth)
        halo = cat["motion_bound_arcsec"] + (radius if i else 0.0)
        pool = read_pool(
            survey, coverage(order, pixel, halo, depth), plan["target_epoch_jyear"], memory_bytes
        )
        memory_bytes -= int(pool.frame.estimated_size() + 120 * len(pool.ids))
        pools.append(pool)
    left, right = pools
    owned = np.flatnonzero(left.index >> (2 * (29 - order)) == pixel)
    owned = owned[np.argsort(left.ids[owned], kind="stable")]
    right_sorted = np.argsort(right.ids, kind="stable")
    tree = cKDTree(right.xyz[right_sorted])
    query = left.xyz[owned]
    chord = np.nextafter(_arcsec_to_chord(radius), np.inf)
    numbers = tree.query_ball_point(query, chord, return_length=True)
    if np.maximum(1, numbers).sum() > max_rows:
        raise ValueError(f"Pilot candidate tile {pixel} exceeds {max_rows} rows")
    neighbours = tree.query_ball_point(query, chord, return_sorted=True)
    primary = np.repeat(owned, np.maximum(1, numbers))
    partner = np.array(
        [int(right_sorted[j]) if j >= 0 else -1 for group in neighbours for j in (group or [-1])],
        dtype=np.int64,
    )
    valid = partner >= 0
    seps = np.full(len(primary), np.nan)
    seps[valid] = _chord_to_arcsec(
        np.linalg.norm(left.xyz[primary[valid]] - right.xyz[partner[valid]], axis=1)
    )
    select = pl.Series(partner).set(pl.Series(~valid), None)
    return pl.DataFrame(
        {
            "left_id": key_text(left.frame[pair[0]["id"]]).gather(primary),
            "right_id": key_text(right.frame[pair[1]["id"]]).gather(select),
            "left_ra": left.ra[primary],
            "left_dec": left.dec[primary],
            "right_ra": pl.Series(right.ra).gather(select),
            "right_dec": pl.Series(right.dec).gather(select),
            "left_epoch_jyear": pl.Series(left.epoch[primary]).fill_nan(None),
            "right_epoch_jyear": pl.Series(right.epoch).gather(select).fill_nan(None),
            "sep_arcsec": pl.Series(seps).fill_nan(None),
            **{f"left_native_{c}": left.frame[c].gather(primary) for c in extra_columns[0]},
            **{f"right_native_{c}": right.frame[c].gather(select) for c in extra_columns[1]},
        }
    )


def match_tile(
    surveys: list[Survey], plan: dict, pixel: int, dest: Path, memory_bytes: int
) -> tuple[int, dict[str, list[int]], dict[str, list[int]], int]:
    order = plan["order"]
    shift = 2 * (29 - order)
    # Two edges can affect a reciprocal decision. Walk the earlier decisions
    # backwards, adding two radii for each level in the cascade.
    radii = plan["radii"]
    halo = 2 * sum(max(row, default=0) for row in radii)
    depth = surveys[0].depth
    covers = {}
    pools = []
    loaded = 0.0
    for survey in surveys:
        motion = survey.config["motion_bound_arcsec"]
        if motion not in covers:
            covers[motion] = coverage(order, pixel, halo + motion, depth)
        pool = read_pool(
            survey, covers[motion], plan["target_epoch_jyear"], int(memory_bytes - loaded)
        )
        loaded += pool.frame.estimated_size() + pool.xyz.nbytes + len(pool.ids) * 96
        if loaded > memory_bytes:
            raise ValueError(f"Tile {pixel} exceeds its projected pool memory budget")
        pools.append(pool)

    seed_frames = []
    memberships: list[list[tuple[np.ndarray, np.ndarray, np.ndarray]]] = [[] for _ in pools]
    offset = 0
    for level, (survey, pool) in enumerate(zip(surveys, pools, strict=True)):
        active = np.flatnonzero(pool.alive)
        owned = active[(pool.index[active] >> shift) == pixel]
        cat = survey.config
        ids = key_text(pool.frame[cat["id"]].gather(owned))
        if len(owned):
            seed_frames.append(
                pl.DataFrame(
                    {
                        "component_id": cat["release"] + ":" + ids,
                        "spine_release": [cat["release"]] * len(owned),
                        "ra": pool.ra[owned],
                        "dec": pool.dec[owned],
                        "position_epoch_jyear": pl.Series(pool.epoch[owned]).fill_nan(None),
                        INDEX: pool.index[owned],
                    },
                    schema=CORE_SCHEMA,
                )
            )
            memberships[level].append((offset + np.arange(len(owned)), owned, np.zeros(len(owned))))
        search = Search.build(pool, active)
        for j in range(level + 1, len(pools)):
            other = pools[j]
            left, right, sep = reciprocal(search, other, radii[level][j - level - 1])
            other.alive[right] = False
            core = (pool.index[left] >> shift) == pixel
            output_rows = offset + np.searchsorted(owned, left[core])
            memberships[j].append((output_rows, right[core], sep[core]))
        offset += len(owned)

    def tally(ids: pl.Series) -> list[int]:
        # Order-independent membership check that can be reduced over tiles.
        # The published SHA-256 is a separate checksum over each output file.
        return [len(ids), int(ids.hash(seed=17).sum()), int(ids.hash(seed=31).sum())]

    members, inputs = {}, {}
    for survey, pool, groups in zip(surveys, pools, memberships, strict=True):
        tag = survey.config["tag"]
        keys = key_text(pool.frame[survey.config["id"]])
        rows = np.concatenate([group[1] for group in groups]) if groups else np.array([], dtype=int)
        members[tag] = tally(keys.gather(rows))
        inputs[tag] = tally(keys.filter((pool.native_index >> shift) == pixel))
    if not offset:
        return 0, members, inputs, int(loaded)
    seeds = pl.concat(seed_frames)
    # The component ID also makes coincident seeds deterministic.
    sorted_rows = seeds.select(pl.arg_sort_by([INDEX, "component_id"])).to_series().to_numpy()
    inverse = np.empty(offset, dtype=np.int64)
    inverse[sorted_rows] = np.arange(offset)
    member_arrays = []
    width = 80.0
    for groups, survey, pool in zip(memberships, surveys, pools, strict=True):
        rows, source_rows, seps = (
            [np.concatenate(arrays) for arrays in zip(*groups, strict=True)]
            if groups
            else [np.empty(0, dtype=np.int64), np.empty(0, dtype=np.int64), np.empty(0)]
        )
        positions = inverse[rows]
        take = positions.argsort()
        member_arrays.append((positions[take], source_rows[take], seps[take]))
        width += 8 + sum(
            max(8, pool.frame[col].estimated_size() / max(1, pool.frame.height))
            for col in output_columns(survey.config)
        )

    # Write directly in HATS order. Each batch materializes only a bounded
    # slice of the wide result, including its typed nulls.
    batch_rows = max(1, min(131_072, int(min(64 << 20, memory_bytes // 8) / width)))
    writer = None
    try:
        for start in range(0, offset, batch_rows):
            end = min(start + batch_rows, offset)
            result = seeds[sorted_rows[start:end]]
            for survey, pool, (positions, source_rows, seps) in zip(
                surveys, pools, member_arrays, strict=True
            ):
                lo, hi = np.searchsorted(positions, [start, end])
                batch_index = positions[lo:hi] - start
                # Arrow-backed null indices allow a vectorized gather without
                # converting native identifiers or measurements into Python objects.
                indices = np.zeros(end - start, dtype=np.int64)
                included = np.zeros(end - start, dtype=bool)
                indices[batch_index] = source_rows[lo:hi]
                included[batch_index] = True
                gather = pl.Series(indices).set(pl.Series(~included), None)
                sep = np.full(end - start, np.nan)
                sep[batch_index] = seps[lo:hi]
                columns = [
                    pool.frame[raw].gather(gather).rename(name)
                    for raw, name in output_columns(survey.config).items()
                ]
                columns.append(pl.Series(f"sep_arcsec_{survey.config['tag']}", sep).fill_nan(None))
                result.hstack(columns, in_place=True)
            table = result.to_arrow()
            if writer is None:
                writer = pq.ParquetWriter(dest, table.schema, compression="zstd")
            writer.write_table(table)
    finally:
        if writer is not None:
            writer.close()
    return offset, members, inputs, int(loaded)
