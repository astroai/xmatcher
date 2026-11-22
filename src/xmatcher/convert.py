"""Streaming conversion between FITS, Parquet, HATS and CSV catalogues.

``convert(src, dst)`` reads ``src`` in Arrow record batches and writes ``dst``
in the format its suffix names. Every pair of formats works in both
directions, and memory stays bounded by the batch size:

- **FITS** (``.fits``, ``.fit``) is read with torchfits (streaming
  ``table.scan``; FITS nulls become Arrow nulls). It is written by a
  streaming binary-table writer: one pass over the rows, with ``NAXIS2``
  patched at the end. String columns need their width up front, which costs
  one extra pass over string columns when the source is not FITS.
- **Parquet** is a file or a directory of Parquet files (a plain dataset),
  read and written with pyarrow.
- **HATS** (an output ending in ``.hats``, or any directory with a HATS
  ``properties`` file) is read partition by partition, in (order, pixel)
  order. It is written in three passes:
  1. count rows per HEALPix cell at ``max_order``;
  2. choose an adaptive tiling (split cells holding more than ``max_rows``
     rows) and spill each batch's rows to their partition;
  3. write each partition sorted by ``_healpix_29``, then the spec-conformant
     metadata (``properties``, ``partition_info.csv``, ``_metadata`` with
     every footer, ``_common_metadata``).
- **CSV** (``.csv``, ``.tsv``) is a convenience in both directions.

Reading a HATS tree drops its ``_healpix_29`` index column unless
``keep_healpix29=True``; writing one adds it. Row order follows the source,
except that a HATS output is ordered spatially.
"""

from __future__ import annotations

import logging
import math
import os
import shutil
import tempfile
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .exceptions import InputError

logger = logging.getLogger(__name__)

HEALPIX29 = "_healpix_29"
_FITS_SUFFIXES = (".fits", ".fit", ".fts")
_CSV_SUFFIXES = (".csv", ".tsv", ".tab")
_RA_NAMES = ("ra", "RA", "ra_deg", "RAJ2000", "RA_ICRS", "raMean", "ALPHA_J2000", "ra_pm")
_DEC_NAMES = (
    "dec",
    "DEC",
    "Dec",
    "dec_deg",
    "DEJ2000",
    "DE_ICRS",
    "decMean",
    "DELTA_J2000",
    "dec_pm",
)


@dataclass
class ConvertReport:
    """What :func:`convert` wrote."""

    src_format: str
    dst_format: str
    rows: int
    columns: list[str]
    partitions: int = 0


# ---------------------------------------------------------------- formats


def detect_format(path: str | Path, *, output: bool = False) -> str:
    """``"fits"``, ``"parquet"``, ``"hats"`` or ``"csv"`` for a path.

    Outputs are named by suffix (``.hats`` for a HATS tree). Inputs may also
    be directories: a HATS tree (with ``properties``) or a Parquet dataset.
    """
    p = Path(path)
    name = p.name.lower()
    if name.endswith(".fits.gz") or p.suffix.lower() in _FITS_SUFFIXES:
        return "fits"
    if p.suffix.lower() == ".parquet":
        return "parquet"
    if p.suffix.lower() in _CSV_SUFFIXES:
        return "csv"
    if p.suffix.lower() == ".hats":
        return "hats"
    if not output and p.is_dir():
        from .io_utils import is_hats_dir

        if is_hats_dir(p):
            return "hats"
        if any(p.rglob("*.parquet")):
            return "parquet"
    raise InputError(
        f"Cannot tell the format of '{path}': use .fits, .parquet, .csv or .hats "
        "(or a HATS / Parquet directory as input)."
    )


# ---------------------------------------------------------------- readers


@dataclass
class _Source:
    """A re-iterable batch source: ``batches()`` restarts from the first row."""

    fmt: str
    schema: pa.Schema
    batches: Callable[[], Iterator[pa.RecordBatch]]
    hats_ra: str | None = None
    hats_dec: str | None = None


def _hats_partition_files(root: Path) -> list[Path]:
    info = root / "partition_info.csv"
    if info.is_file():
        import polars as pl

        tab = pl.read_csv(info)
        if "file_loc" in tab.columns:
            files = [root / "dataset" / f"{loc}.parquet" for loc in tab["file_loc"]]
        else:
            files = [
                root
                / "dataset"
                / f"Norder={o}"
                / f"Dir={(p // 10000) * 10000}"
                / f"Npix={p}.parquet"
                for o, p in zip(tab["Norder"], tab["Npix"], strict=True)
            ]
        keyed = sorted(zip(tab["Norder"].to_list(), tab["Npix"].to_list(), files, strict=True))
        return [f for _, _, f in keyed]

    def key(f: Path) -> tuple[int, int]:
        parts = {k: v for k, _, v in (s.partition("=") for s in f.with_suffix("").parts) if v}
        return int(parts.get("Norder", 0)), int(parts.get("Npix", 0))

    return sorted((root / "dataset").glob("Norder=*/Dir=*/Npix=*.parquet"), key=key)


def _open_source(
    src: Path,
    fmt: str,
    *,
    columns: Sequence[str] | None,
    batch_rows: int,
    hdu: int | str | None,
    keep_healpix29: bool,
) -> _Source:
    cols = list(columns) if columns else None
    if fmt == "fits":
        from torchfits import table as fits_table

        h = 1 if hdu is None else hdu

        def fits_batches() -> Iterator[pa.RecordBatch]:
            yield from fits_table.scan(
                str(src), hdu=h, columns=cols, batch_size=batch_rows, include_fits_metadata=True
            )

        schema = fits_table.read_arrow(
            str(src), hdu=h, columns=cols, row_slice=(0, 0), include_fits_metadata=True
        ).schema
        return _Source("fits", schema, fits_batches)
    if fmt == "parquet":
        import pyarrow.dataset as ds

        dataset = ds.dataset(str(src), format="parquet")
        schema = (
            dataset.schema if cols is None else pa.schema([dataset.schema.field(c) for c in cols])
        )

        def parquet_batches() -> Iterator[pa.RecordBatch]:
            yield from dataset.to_batches(columns=cols, batch_size=batch_rows)

        return _Source("parquet", schema, parquet_batches)
    if fmt == "hats":
        from .hats_native import _read_properties
        from .io_utils import resolve_local_hats_root

        root = resolve_local_hats_root(src)
        props = _read_properties(root)
        files = _hats_partition_files(root)
        if not files:
            schema = pq.read_schema(root / "dataset" / "_common_metadata")
        else:
            schema = pq.read_schema(files[0])
        # Hive columns are not data; the spatial index is dropped unless asked.
        drop = {"Norder", "Dir", "Npix"} | (set() if keep_healpix29 else {HEALPIX29})
        names = cols if cols is not None else [n for n in schema.names if n not in drop]
        missing = [c for c in names if c not in schema.names]
        if missing:
            raise InputError(f"Columns {missing} are not in the HATS catalogue {root}.")
        schema = pa.schema([schema.field(n) for n in names])

        def hats_batches() -> Iterator[pa.RecordBatch]:
            for f in files:
                yield from pq.ParquetFile(f).iter_batches(batch_size=batch_rows, columns=names)

        return _Source(
            "hats", schema, hats_batches, props.get("hats_col_ra"), props.get("hats_col_dec")
        )
    if fmt == "csv":
        import pyarrow.csv as pacsv

        delim = "\t" if src.suffix.lower() in (".tsv", ".tab") else ","

        def csv_batches() -> Iterator[pa.RecordBatch]:
            reader = pacsv.open_csv(
                str(src),
                read_options=pacsv.ReadOptions(block_size=1 << 24),
                parse_options=pacsv.ParseOptions(delimiter=delim),
                convert_options=pacsv.ConvertOptions(include_columns=cols),
            )
            yield from reader

        first = next(csv_batches(), None)
        schema = first.schema if first is not None else pa.schema([])
        return _Source("csv", schema, csv_batches)
    raise InputError(f"Unsupported input format {fmt!r}.")


def _conform(batch: pa.RecordBatch, schema: pa.Schema) -> pa.RecordBatch:
    """Cast a batch to the source's first schema (null-typed batches, int widths)."""
    if batch.schema.equals(schema, check_metadata=False):
        return batch
    arrays = [batch.column(n).cast(schema.field(n).type) for n in schema.names]
    return pa.RecordBatch.from_arrays(arrays, schema=schema)


# ------------------------------------------------------------ FITS writer

_FITS_BLOCK = 2880


def _card(key: str, value: Any = None, comment: str = "") -> bytes:
    if value is None:
        text = key.ljust(80)
    else:
        if isinstance(value, bool):
            v = ("T" if value else "F").rjust(20)
        elif isinstance(value, int):
            v = str(value).rjust(20)
        elif isinstance(value, float):
            v = repr(value).upper().rjust(20)
        else:
            s = str(value).replace("'", "''")
            v = f"'{s.ljust(8)}'".ljust(20)
        text = f"{key[:8].ljust(8)}= {v}" + (f" / {comment}" if comment else "")
    if len(text) > 80:
        raise InputError(f"FITS card too long for {key}: {text!r}")
    return text.ljust(80).encode("ascii")


def _pad(data: bytes, fill: bytes) -> bytes:
    return data + fill * ((-len(data)) % _FITS_BLOCK)


# (TFORM code, numpy big-endian dtype, TZERO or None, TNULL for nullable ints)
_FITS_TYPES: dict[pa.DataType, tuple[str, str, int | None, int | None]] = {
    pa.bool_(): ("L", "S1", None, None),
    pa.uint8(): ("B", ">u1", None, None),
    pa.int8(): ("B", ">u1", -128, None),
    pa.int16(): ("I", ">i2", None, -(2**15)),
    pa.uint16(): ("I", ">i2", 2**15, None),
    pa.int32(): ("J", ">i4", None, -(2**31)),
    pa.uint32(): ("J", ">i4", 2**31, None),
    pa.int64(): ("K", ">i8", None, -(2**63)),
    pa.uint64(): ("K", ">i8", 2**63, None),
    pa.float32(): ("E", ">f4", None, None),
    pa.float64(): ("D", ">f8", None, None),
}


def _string_width(field: pa.Field) -> int | None:
    meta = field.metadata or {}
    tform = meta.get(b"fits_tform", b"").decode()
    if tform.endswith("A"):
        return int(tform[:-1] or 1)
    return None


class _FitsTableWriter:
    """Streaming FITS binary-table writer (primary HDU + one BINTABLE)."""

    def __init__(self, path: Path, schema: pa.Schema, widths: dict[str, int], extname: str):
        self.path = path
        self.schema = schema
        self.rows = 0
        self.cols: list[tuple[str, str, str, int, int | None, int | None]] = []
        cards = [
            _card("XTENSION", "BINTABLE", "binary table extension"),
            _card("BITPIX", 8),
            _card("NAXIS", 2),
        ]
        row_bytes = 0
        body: list[bytes] = []
        for i, field in enumerate(schema, start=1):
            t = field.type
            repeat = 1
            if pa.types.is_fixed_size_list(t):
                repeat, t = t.list_size, t.value_type
            if pa.types.is_string(t) or pa.types.is_large_string(t):
                if repeat != 1:
                    raise InputError(
                        f"Column {field.name!r}: string arrays cannot be written to FITS."
                    )
                w = max(1, widths.get(field.name, 1))
                code, dtype, zero, null = "A", f"S{w}", None, None
                tform, nbytes = f"{w}A", w
            elif t in _FITS_TYPES:
                code, dtype, zero, null = _FITS_TYPES[t]
                tform = f"{repeat}{code}"
                nbytes = repeat * np.dtype(dtype).itemsize
            else:
                raise InputError(
                    f"Column {field.name!r} has type {field.type}, which has no FITS binary-table "
                    "form here; drop it with columns=."
                )
            self.cols.append((field.name, code, dtype, repeat, zero, null))
            row_bytes += nbytes
            body.append(_card(f"TTYPE{i}", field.name))
            body.append(_card(f"TFORM{i}", tform))
            if zero is not None:
                body.append(_card(f"TSCAL{i}", 1))
                body.append(_card(f"TZERO{i}", zero))
            if null is not None and field.nullable:
                body.append(_card(f"TNULL{i}", null))
            unit = (field.metadata or {}).get(b"unit") or (field.metadata or {}).get(b"fits_tunit")
            if unit:
                body.append(_card(f"TUNIT{i}", unit.decode()))
        cards.append(_card("NAXIS1", row_bytes, "bytes per row"))
        self._naxis2_card = len(cards)
        cards.append(_card("NAXIS2", 0, "rows"))
        cards += [_card("PCOUNT", 0), _card("GCOUNT", 1), _card("TFIELDS", len(schema))]
        cards += body
        cards += [_card("EXTNAME", extname), _card("END")]
        primary = _pad(
            b"".join(
                [
                    _card("SIMPLE", True),
                    _card("BITPIX", 8),
                    _card("NAXIS", 0),
                    _card("EXTEND", True),
                    _card("END"),
                ]
            ),
            b" ",
        )
        self._header = b"".join(cards)
        self.row_bytes = row_bytes
        # Held open across write() calls; close() patches NAXIS2 and closes it.
        self.fh = open(path, "wb")  # noqa: SIM115
        self.fh.write(primary)
        self._header_at = self.fh.tell()
        self.fh.write(_pad(self._header, b" "))

    def write(self, batch: pa.RecordBatch) -> None:
        n = batch.num_rows
        if n == 0:
            return
        layout = [
            (f"f{i}", (d, r) if r > 1 and c != "A" else d)
            for i, (_, c, d, r, _, _) in enumerate(self.cols)
        ]
        rec = np.zeros(n, dtype=layout)
        for i, (name, code, dtype, repeat, zero, null) in enumerate(self.cols):
            col = batch.column(name)
            key = f"f{i}"
            if repeat > 1:
                if col.null_count:
                    raise InputError(f"Column {name!r} has null arrays; FITS has no null for them.")
                v = pc.list_flatten(col).to_numpy(zero_copy_only=False).reshape(n, repeat)
            elif code == "A":
                enc = [x.encode("utf-8") for x in col.fill_null("").to_pylist()]
                w = np.dtype(dtype).itemsize
                if any(len(e) > w for e in enc):
                    raise InputError(f"Column {name!r} has strings longer than its FITS width {w}.")
                rec[key] = np.array(enc, dtype=dtype)
                continue
            elif code == "L":
                rec[key] = np.array(
                    [b"\0" if x is None else (b"T" if x else b"F") for x in col.to_pylist()],
                    dtype="S1",
                )
                continue
            elif code in "ED":
                v = col.fill_null(math.nan).to_numpy(zero_copy_only=False)
            else:
                if col.null_count and null is None:
                    raise InputError(f"Column {name!r} has nulls but no FITS null value.")
                v = col.fill_null(0 if null is None else null).to_numpy(zero_copy_only=False)
            if code == "L":  # bool arrays
                v = np.where(v, b"T", b"F")
            elif zero is not None:
                # Stored = physical - TZERO, in the signed (or unsigned byte) storage type.
                if zero == 2**63:
                    v = (v.astype(np.uint64) ^ np.uint64(1 << 63)).view(np.int64)
                else:
                    v = (v.astype(np.int64) - zero).astype(np.dtype(dtype).newbyteorder("="))
            rec[key] = v
        self.fh.write(rec.tobytes())
        self.rows += n

    def close(self) -> None:
        nbytes = self.rows * self.row_bytes
        self.fh.write(b"\0" * ((-nbytes) % _FITS_BLOCK))
        self.fh.seek(self._header_at + 80 * self._naxis2_card)
        self.fh.write(_card("NAXIS2", self.rows, "rows"))
        self.fh.close()


# ------------------------------------------------------------ HATS writer


def _healpix29(ra: np.ndarray, dec: np.ndarray) -> np.ndarray:
    import torch
    from torchsky.catalogs import healpix29

    def tensor(a: np.ndarray) -> torch.Tensor:
        return torch.from_numpy(a if a.flags.writeable else a.copy())

    return healpix29(tensor(ra), tensor(dec)).numpy()


def _radec(batch: pa.RecordBatch, ra: str, dec: str) -> tuple[np.ndarray, np.ndarray]:
    r = batch.column(ra).cast(pa.float64()).to_numpy(zero_copy_only=False)
    d = batch.column(dec).cast(pa.float64()).to_numpy(zero_copy_only=False)
    bad = ~(np.isfinite(r) & np.isfinite(d) & (np.abs(d) <= 90.0))
    if bad.any():
        raise InputError(
            f"{int(bad.sum())} rows have missing or invalid coordinates ({ra}, {dec}); "
            "a HATS catalogue needs a position for every row. Filter them first."
        )
    return r, d


def _tiling(counts: dict[int, int], max_order: int, max_rows: int) -> list[tuple[int, int]]:
    """Adaptive (order, pixel) leaves covering every non-empty cell at ``max_order``."""
    cells = np.array(sorted(counts), dtype=np.int64)
    cum = np.concatenate(([0], np.cumsum([counts[c] for c in cells])))
    leaves: list[tuple[int, int]] = []
    stack = [(0, p) for p in range(11, -1, -1)]
    while stack:
        order, pix = stack.pop()
        shift = 2 * (max_order - order)
        lo, hi = np.searchsorted(cells, [pix << shift, (pix + 1) << shift])
        n = int(cum[hi] - cum[lo])
        if n == 0:
            continue
        if n <= max_rows or order >= max_order:
            leaves.append((order, pix))
            continue
        stack.extend((order + 1, 4 * pix + k) for k in range(3, -1, -1))
    return leaves


def _write_hats(
    source: _Source,
    out: Path,
    *,
    ra: str,
    dec: str,
    max_rows: int,
    max_order: int,
    spill_rows: int,
    name: str,
) -> tuple[int, int]:
    import pyarrow.ipc as ipc

    if not 0 <= max_order <= 29:
        raise InputError("max_order must lie in [0, 29]")
    if HEALPIX29 in source.schema.names:
        inner = source.batches

        def without_index() -> Iterator[pa.RecordBatch]:
            for x in inner():
                yield x.drop_columns([HEALPIX29])

        source = _Source(
            source.fmt, pa.schema([f for f in source.schema if f.name != HEALPIX29]), without_index
        )
    shift29 = 2 * (29 - max_order)
    # Pass 1: rows per cell at max_order.
    counts: dict[int, int] = {}
    for batch in source.batches():
        if batch.num_rows == 0:
            continue
        h = _healpix29(*_radec(batch, ra, dec)) >> shift29
        cells, n = np.unique(h, return_counts=True)
        for c, k in zip(cells.tolist(), n.tolist(), strict=True):
            counts[c] = counts.get(c, 0) + k
    leaves = _tiling(counts, max_order, max_rows)
    starts = np.array([p << (2 * (max_order - o)) for o, p in leaves], dtype=np.int64)
    order_idx = np.argsort(starts)
    starts = starts[order_idx]
    leaves = [leaves[i] for i in order_idx]
    out_schema = source.schema.append(pa.field(HEALPIX29, pa.int64()))
    dataset = out / "dataset"
    dataset.mkdir(parents=True)
    spill = Path(tempfile.mkdtemp(prefix=".spill-", dir=out))
    try:
        # Pass 2: route rows to their leaf, spilling to Arrow IPC files.
        buffers: dict[int, list[pa.RecordBatch]] = {}
        buffered = 0
        nflush = 0

        def flush() -> None:
            nonlocal buffered, nflush
            for leaf, parts in buffers.items():
                with ipc.new_file(spill / f"{leaf}.{nflush}.arrow", out_schema) as w:
                    for part in parts:
                        w.write_batch(part)
            buffers.clear()
            buffered = 0
            nflush += 1

        for batch in source.batches():
            if batch.num_rows == 0:
                continue
            batch = _conform(batch, source.schema)
            h29 = _healpix29(*_radec(batch, ra, dec))
            leaf = np.searchsorted(starts, h29 >> shift29, side="right") - 1
            full = pa.RecordBatch.from_arrays(
                [*batch.columns, pa.array(h29, pa.int64())], schema=out_schema
            )
            perm = np.argsort(leaf, kind="stable")
            bounds = np.flatnonzero(np.diff(leaf[perm])) + 1
            for chunk in np.split(perm, bounds):
                lf = int(leaf[chunk[0]])
                buffers.setdefault(lf, []).append(full.take(pa.array(chunk)))
            buffered += batch.num_rows
            if buffered >= spill_rows:
                flush()
        # Pass 3: one sorted Parquet file per leaf, then the metadata.
        info: list[dict[str, Any]] = []
        total = 0
        for i, (order, pix) in enumerate(leaves):
            parts = list(buffers.get(i, []))
            for f in sorted(spill.glob(f"{i}.*.arrow"), key=lambda q: int(q.suffixes[0][1:])):
                with ipc.open_file(f) as r:
                    parts.extend(r.get_batch(k) for k in range(r.num_record_batches))
            table = pa.Table.from_batches(parts, schema=out_schema)
            table = table.take(pc.sort_indices(table, [(HEALPIX29, "ascending")]))
            dirtree = (pix // 10000) * 10000
            rel = f"Norder={order}/Dir={dirtree}/Npix={pix}.parquet"
            dest = dataset / rel
            dest.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(table, dest)
            info.append(
                {
                    "Norder": order,
                    "Dir": dirtree,
                    "Npix": pix,
                    "Nfiles": 1,
                    "file_loc": rel.removesuffix(".parquet"),
                    "file_size": dest.stat().st_size,
                    "count": table.num_rows,
                }
            )
            total += table.num_rows
    finally:
        shutil.rmtree(spill, ignore_errors=True)
    props = {
        "obs_collection": name,
        "dataproduct_type": "object",
        "hats_col_ra": ra,
        "hats_col_dec": dec,
        "hats_ordering": "NESTED",
        "hats_max_rows": str(max_rows),
        "hats_order": str(max((o for o, _ in leaves), default=0)),
    }
    from .ray_union import _write_hats_tree

    _write_hats_tree(out, info, props)
    if not info:
        pq.write_metadata(out_schema, dataset / "_common_metadata")
    return total, len(info)


# ---------------------------------------------------------------- driver


def _pick(
    schema: pa.Schema, given: str | None, hint: str | None, names: Sequence[str], what: str
) -> str:
    if given:
        if given not in schema.names:
            raise InputError(f"{what} column {given!r} is not in the source.")
        return given
    if hint and hint in schema.names:
        return hint
    found = [n for n in names if n in schema.names]
    if len(found) == 1 or (found and found[0].lower() == what.lower()):
        return found[0]
    raise InputError(
        f"Cannot tell the {what} column for a HATS output (candidates: {found or 'none'}); "
        f"pass {'ra_column' if what == 'RA' else 'dec_column'}=."
    )


def _string_widths(source: _Source) -> dict[str, int]:
    widths: dict[str, int] = {}
    pending = []
    for f in source.schema:
        if pa.types.is_string(f.type) or pa.types.is_large_string(f.type):
            w = _string_width(f)
            if w is None:
                pending.append(f.name)
            else:
                widths[f.name] = w
    if pending:
        for batch in source.batches():
            for name in pending:
                lens = pc.binary_length(batch.column(name).cast(pa.binary()))
                m = pc.max(lens).as_py()
                if m is not None:
                    widths[name] = max(widths.get(name, 1), int(m))
    return widths


def convert(
    src: str | Path,
    dst: str | Path,
    *,
    columns: Sequence[str] | None = None,
    hdu: int | str | None = None,
    ra_column: str | None = None,
    dec_column: str | None = None,
    hats_max_rows: int = 1_000_000,
    hats_max_order: int = 10,
    keep_healpix29: bool = False,
    batch_rows: int = 262_144,
    spill_rows: int = 4_000_000,
    overwrite: bool = False,
) -> ConvertReport:
    """Convert a catalogue between FITS, Parquet, HATS and CSV (see module docs).

    ``columns`` selects (and orders) the columns; ``hdu`` names the FITS
    table HDU (default 1). For a HATS output, ``ra_column``/``dec_column``
    default to the source's HATS columns or the one obvious name.
    ``hats_max_rows`` is the partition size at which a HEALPix cell is split,
    down to ``hats_max_order``. ``batch_rows`` bounds the rows in memory per
    batch; ``spill_rows`` bounds the rows buffered before a HATS spill.
    """
    src_p, dst_p = Path(src), Path(dst)
    if not src_p.exists():
        raise InputError(f"Input '{src}' does not exist.")
    src_fmt = detect_format(src_p)
    dst_fmt = detect_format(dst_p, output=True)
    if dst_p.exists():
        if not overwrite:
            raise InputError(f"Output '{dst}' exists; pass overwrite=True to replace it.")
        if dst_p.resolve() == src_p.resolve():
            raise InputError("Input and output are the same path.")
        shutil.rmtree(dst_p) if dst_p.is_dir() else dst_p.unlink()
    source = _open_source(
        src_p,
        src_fmt,
        columns=columns,
        batch_rows=batch_rows,
        hdu=hdu,
        keep_healpix29=keep_healpix29,
    )
    names = list(source.schema.names)
    dst_p.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst_p.with_name(f".{dst_p.name}.{os.getpid()}.tmp")
    if tmp.exists():
        shutil.rmtree(tmp) if tmp.is_dir() else tmp.unlink()
    rows = 0
    partitions = 0
    try:
        if dst_fmt == "hats":
            ra = _pick(source.schema, ra_column, source.hats_ra, _RA_NAMES, "RA")
            dec = _pick(source.schema, dec_column, source.hats_dec, _DEC_NAMES, "Dec")
            tmp.mkdir(parents=True)
            rows, partitions = _write_hats(
                source,
                tmp,
                ra=ra,
                dec=dec,
                max_rows=hats_max_rows,
                max_order=hats_max_order,
                spill_rows=spill_rows,
                name=dst_p.stem,
            )
        elif dst_fmt == "parquet":
            with pq.ParquetWriter(tmp, source.schema, compression="zstd") as w:
                for batch in source.batches():
                    w.write_batch(_conform(batch, source.schema))
                    rows += batch.num_rows
        elif dst_fmt == "csv":
            import pyarrow.csv as pacsv

            opts = pacsv.WriteOptions(
                delimiter="\t" if dst_p.suffix.lower() in (".tsv", ".tab") else ","
            )
            with pacsv.CSVWriter(tmp, source.schema, write_options=opts) as w:
                for batch in source.batches():
                    w.write_batch(_conform(batch, source.schema))
                    rows += batch.num_rows
        elif dst_fmt == "fits":
            writer = _FitsTableWriter(tmp, source.schema, _string_widths(source), "CATALOG")
            try:
                for batch in source.batches():
                    writer.write(_conform(batch, source.schema))
            finally:
                writer.close()
            rows = writer.rows
        else:  # pragma: no cover - detect_format covers every value
            raise InputError(f"Unsupported output format {dst_fmt!r}.")
        os.replace(tmp, dst_p)
    except BaseException:
        if tmp.exists():
            shutil.rmtree(tmp, ignore_errors=True) if tmp.is_dir() else tmp.unlink()
        raise
    logger.info("Converted %s (%s) to %s (%s): %d rows", src, src_fmt, dst, dst_fmt, rows)
    return ConvertReport(src_fmt, dst_fmt, rows, names, partitions)


__all__ = ["ConvertReport", "convert", "detect_format"]
