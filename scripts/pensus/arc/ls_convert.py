"""Convert one Legacy Surveys DR11 sweep (FITS binary table) to Parquet with the mirror schema.

North sweeps lack the 15 i-band columns of south sweeps; the mirror schema is the south schema plus a
`PHOTSYS` column, so north rows carry typed nulls there. Any other column or dtype difference from the
reference schema fails closed. Reads with torchfits `table.scan` (torchfits 1.3.2 `table.read` of a
whole multi-million-row sweep raises TypeError).
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import torchfits

BATCH = 262_144


def mirror_schema(south_fits_schema: pa.Schema) -> pa.Schema:
    return south_fits_schema.append(pa.field("PHOTSYS", pa.string()))


def write_kwargs(schema: pa.Schema) -> dict:
    floats = [f.name for f in schema if pa.types.is_floating(f.type)]
    return {
        "compression": "zstd",
        "compression_level": 3,
        "column_encoding": dict.fromkeys(floats, "BYTE_STREAM_SPLIT"),
        "use_dictionary": [f.name for f in schema if f.name not in floats],
    }


def conform(batch: pa.RecordBatch, schema: pa.Schema, photsys: str) -> pa.RecordBatch:
    have = {f.name: f.type for f in batch.schema}
    extra = set(have) - set(schema.names)
    if extra:
        raise ValueError(f"columns not in mirror schema: {sorted(extra)}")
    cols = []
    for f in schema:
        if f.name == "PHOTSYS":
            cols.append(pa.array([photsys] * batch.num_rows, pa.string()))
        elif f.name in have:
            if have[f.name] != f.type:
                raise ValueError(f"{f.name}: {have[f.name]} != mirror {f.type}")
            cols.append(batch.column(f.name))
        elif photsys == "N" and f.name.endswith("_I"):
            cols.append(pa.nulls(batch.num_rows, f.type))
        else:
            raise ValueError(f"missing column {f.name} for PHOTSYS={photsys}")
    return pa.RecordBatch.from_arrays(cols, schema=schema)


def convert(fits_path: Path, out_path: Path, photsys: str, schema: pa.Schema) -> int:
    if photsys not in ("N", "S"):
        raise ValueError(f"PHOTSYS must be N or S, got {photsys!r}")
    rows = 0
    with pq.ParquetWriter(out_path, schema, **write_kwargs(schema)) as writer:
        for batch in torchfits.table.scan(str(fits_path), hdu=1, batch_size=BATCH):
            writer.write_batch(conform(batch, schema, photsys), row_group_size=BATCH)
            rows += batch.num_rows
    return rows
