"""Run hats-import for one catalogue (dask workers need a real __main__ module)."""

import argparse
import glob
import json
import sys

from dask.distributed import Client
from hats_import.catalog.arguments import ImportArguments
from hats_import.catalog.file_readers import get_file_reader
from hats_import.pipeline import pipeline_with_client


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--inputs", nargs="+", required=True, help="files or globs")
    p.add_argument(
        "--reader", required=True, help="hats-import file_reader name (fits, csv, parquet)"
    )
    p.add_argument("--name", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--tmp", required=True)
    p.add_argument("--ra", required=True)
    p.add_argument("--dec", required=True)
    p.add_argument("--sort", default=None)
    p.add_argument("--pixel-threshold", type=int, default=1_000_000)
    p.add_argument("--workers", type=int, default=6)
    p.add_argument("--memory-limit", default="auto")
    p.add_argument("--extra", default="{}", help="JSON of extra ImportArguments kwargs")
    p.add_argument("--reader-kwargs", default="{}", help="JSON kwargs for get_file_reader")
    a = p.parse_args()

    files = sorted({f for pat in a.inputs for f in (glob.glob(pat) or [pat])})
    if not files:
        print("no input files", file=sys.stderr)
        return 2
    print(f"{len(files)} input files", flush=True)
    args = ImportArguments(
        output_artifact_name=a.name,
        output_path=a.out,
        input_file_list=files,
        file_reader=get_file_reader(a.reader, **json.loads(a.reader_kwargs)),
        ra_column=a.ra,
        dec_column=a.dec,
        sort_columns=a.sort,
        catalog_type="object",
        pixel_threshold=a.pixel_threshold,
        tmp_dir=a.tmp,
        dask_tmp=a.tmp,
        resume=True,
        simple_progress_bar=True,
        **json.loads(a.extra),
    )
    with Client(
        n_workers=a.workers,
        threads_per_worker=1,
        memory_limit=a.memory_limit,
        local_directory=a.tmp,
    ) as client:
        pipeline_with_client(args, client)
    print("hats-import done", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
