"""Convert a CatWISE2020 IPAC table (.tbl.gz) to Parquet.

Columns are fixed-width between the '|' delimiters of the IPAC header. `double` columns and
FLOAT64_REALS are float64; other `r`/`real` columns are float32, which is exact because they publish
at most 6 significant digits (FLT_DIG), checked on every file. Ints are int64; `null` and blank cells
become nulls.
Usage: catwise_convert.py <in.tbl.gz> <out.parquet> [expected_rows]
"""

from __future__ import annotations

import gzip
import json
import re
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

TYPES = {
    "char": pa.string(),
    "c": pa.string(),
    "double": pa.float64(),
    "d": pa.float64(),
    "real": pa.float32(),
    "r": pa.float32(),
    "float": pa.float32(),
    "int": pa.int64(),
    "i": pa.int64(),
    "long": pa.int64(),
    "l": pa.int64(),
}
# Reals reaching 6+ significant digits in a 1,500-tile sample; the rest peak at <= 5.
FLOAT64_REALS = {
    "DelatSNR",
    "DelonSNR",
    "PMDec",
    "PMRA",
    "p1",
    "p2",
    "par_pm",
    "sigPMDec",
    "sigPMRA",
    "sigdec_pm",
    "sigra_pm",
    "sigradec_pm",
    "w1Sat",
    "w1conf",
    "w1k",
    "w1sigsk",
    "w1sky",
    "w2Sat",
    "w2conf",
    "w2k",
    "w2sigsk",
    "w2sky",
    "wx",
    "wy",
}
FLT_DIG = 6


def _max_sig_digits(raw: pa.Array) -> int:
    mant = pc.replace_substring_regex(raw, r"[eE].*$", "")
    digits = pc.utf8_ltrim(pc.replace_substring_regex(mant, r"[^0-9]", ""), "0")
    return int(pc.max(pc.utf8_length(digits)).as_py() or 0)


def read_ipac(path: Path) -> tuple[pa.Table, int | None]:
    with gzip.open(path, "rt", encoding="latin1") as fh:
        text = fh.read()
    lines = text.splitlines()
    start = next((i for i, ln in enumerate(lines) if not ln.startswith(("\\", "|"))), len(lines))
    header = [ln for ln in lines[:start] if ln.startswith("|")]
    nsrc = next(
        (int(m.group(1)) for ln in lines[:start] if (m := re.match(r"\\Nsrc\s*=\s*(\d+)", ln))),
        None,
    )
    data = pa.array([ln for ln in lines[start:] if ln.strip()], pa.string())
    del text, lines
    names, types = header[0], header[1]
    pipes = [i for i, ch in enumerate(names) if ch == "|"]
    cols, out_names = [], []
    for a, b in zip(pipes, pipes[1:], strict=False):
        name, typ = names[a + 1 : b].strip(), types[a + 1 : b].strip()
        raw = pc.utf8_trim_whitespace(pc.utf8_slice_codeunits(data, a + 1, b + 1))
        raw = pc.if_else(
            pc.or_(pc.equal(raw, "null"), pc.equal(raw, "")), pa.scalar(None, pa.string()), raw
        )
        dtype = pa.float64() if name in FLOAT64_REALS else TYPES[typ]
        if dtype == pa.float32() and (n := _max_sig_digits(raw)) > FLT_DIG:
            raise ValueError(
                f"{path}: column {name} has {n} significant digits; float32 would be lossy"
            )
        cols.append(raw.cast(dtype))
        out_names.append(name)
    return pa.table(cols, names=out_names), nsrc


def write(table: pa.Table, dst: str | Path) -> None:
    floats = [f.name for f in table.schema if pa.types.is_floating(f.type)]
    pq.write_table(
        table,
        dst,
        compression="zstd",
        compression_level=3,
        row_group_size=65536,
        use_byte_stream_split=floats,
        use_dictionary=[n for n in table.column_names if n not in floats],
    )


def main(src: str, dst: str, expected: str | None = None) -> int:
    table, nsrc = read_ipac(Path(src))
    want = int(expected) if expected else nsrc
    if table.num_rows != want or (nsrc is not None and table.num_rows != nsrc):
        print(
            json.dumps(
                {
                    "file": src,
                    "error": "row count",
                    "rows": table.num_rows,
                    "nsrc": nsrc,
                    "expected": want,
                }
            )
        )
        return 1
    tmp = Path(str(dst) + ".partial")
    write(table, tmp)
    tmp.rename(dst)
    print(json.dumps({"file": src, "rows": table.num_rows, "out_bytes": Path(dst).stat().st_size}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(*sys.argv[1:]))
