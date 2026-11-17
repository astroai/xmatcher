"""Convert a Gaia DR3 XpContinuousMeanSpectrum CSV(.gz) to Parquet with native array dtypes.

Archive dtypes are kept exactly: *_coefficients float64[], *_coefficient_errors and
*_coefficient_correlations float32[]; scalars as declared in the ECSV header.
Usage: xp_convert.py <in.csv.gz> <out.parquet>
"""

from __future__ import annotations

import gzip
import json
import resource
import sys
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

ARRAY_TYPES = {
    "coefficients": pa.float64(),
    "coefficient_errors": pa.float32(),
    "coefficient_correlations": pa.float32(),
}
SCALARS = {
    "source_id": pa.int64(),
    "solution_id": pa.int64(),
    "basis_function_id": pa.int16(),
    "degrees_of_freedom": pa.int16(),
    "n_parameters": pa.int16(),
    "n_measurements": pa.int16(),
    "n_rejected_measurements": pa.int16(),
    "standard_deviation": pa.float32(),
    "chi_squared": pa.float32(),
    "n_relevant_bases": pa.int16(),
    "relative_shrinking": pa.float32(),
}


def _column_types(names: list[str]) -> dict[str, pa.DataType]:
    out: dict[str, pa.DataType] = {}
    for n in names:
        key = n.removeprefix("bp_").removeprefix("rp_")
        if key in ARRAY_TYPES:
            out[n] = pa.string()
        elif key in SCALARS:
            out[n] = SCALARS[key]
        else:
            raise ValueError(f"unexpected XP column {n}")
    return out


def _header_lines(path: Path) -> int:
    n = 0
    with gzip.open(path, "rt") as fh:
        for line in fh:
            if not line.startswith("#"):
                return n
            n += 1
    return n


def _parse_array(col: pa.ChunkedArray, typ: pa.DataType) -> pa.ChunkedArray:
    inner = pc.utf8_slice_codeunits(col, 1, -1)
    parts = pc.split_pattern(inner, ",")
    return parts.cast(pa.list_(typ))


def convert(src: Path) -> pa.Table:
    skip = _header_lines(src)
    with gzip.open(src, "rb") as fh:
        head = fh.read(1 << 16).decode()
    names = [ln for ln in head.splitlines() if not ln.startswith("#")][0].split(",")
    table = pacsv.read_csv(
        src,
        read_options=pacsv.ReadOptions(skip_rows=skip, block_size=1 << 26),
        convert_options=pacsv.ConvertOptions(
            column_types=_column_types(names), strings_can_be_null=True
        ),
    )
    cols = []
    for n in table.column_names:
        key = n.removeprefix("bp_").removeprefix("rp_")
        cols.append(_parse_array(table[n], ARRAY_TYPES[key]) if key in ARRAY_TYPES else table[n])
    return pa.table(cols, names=table.column_names)


def write(table: pa.Table, dst: str | Path) -> None:
    # Byte-stream-split makes float mantissas compressible; ~35% smaller than plain zstd on XP arrays.
    floats = [
        f.name for f in table.schema if pa.types.is_list(f.type) or pa.types.is_floating(f.type)
    ]
    pq.write_table(
        table,
        dst,
        compression="zstd",
        compression_level=3,
        row_group_size=16384,
        use_byte_stream_split=floats,
        use_dictionary=[n for n in table.column_names if n not in floats],
    )


def main(src: str, dst: str) -> int:
    t0 = time.time()
    table = convert(Path(src))
    t1 = time.time()
    write(table, dst)
    t2 = time.time()
    bp = table["bp_coefficients"].combine_chunks()
    lens = pc.list_value_length(bp)
    print(
        json.dumps(
            {
                "rows": table.num_rows,
                "parse_s": round(t1 - t0, 1),
                "write_s": round(t2 - t1, 1),
                "in_bytes": Path(src).stat().st_size,
                "out_bytes": Path(dst).stat().st_size,
                "bp_null": bp.null_count,
                "peak_rss_gb": round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 2**20, 2),
                "bp_len_minmax": [pc.min(lens).as_py(), pc.max(lens).as_py()],
                "schema": {f.name: str(f.type) for f in table.schema},
            },
            indent=1,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1], sys.argv[2]))
