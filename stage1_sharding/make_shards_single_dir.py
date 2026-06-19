#!/usr/bin/env python3
import os
import argparse
from pathlib import Path
import json

import fastparquet as fp
import webdataset as wds


def _empty_list_or_none(x):
    if x is None:
        return True
    if isinstance(x, (list, tuple)):
        return all(v is None for v in x)
    return False


def iter_docs_from_parquet_files(parquet_files):
    doc_id = 0

    columns = ["images", "texts"]

    for parquet_path in parquet_files:
        print(f"[INFO] Reading parquet: {parquet_path}")
        pf = fp.ParquetFile(parquet_path)

        for df in pf.iter_row_groups(columns=columns):
            for _, row in df.iterrows():
                images = row["images"]
                texts = row["texts"]

                if _empty_list_or_none(images) and _empty_list_or_none(texts):
                    continue

                sample = {
                    "images": images,
                    "texts": texts,
                }

                key = f"{doc_id:09d}"
                yield {
                    "__key__": key,
                    "json": json.dumps(sample).encode("utf-8"),
                }

                doc_id += 1


def make_shards_for_dir(
    input_dir: Path,
    output_dir: Path,
    max_parquet_files: int | None,
    max_samples_per_shard: int,
):
    parquet_files = sorted(input_dir.rglob("*.parquet"))
    if not parquet_files:
        print(f"[INFO] No parquet files under {input_dir}, nothing to do.")
        return

    if max_parquet_files is not None:
        parquet_files = parquet_files[:max_parquet_files]

    output_dir.mkdir(parents=True, exist_ok=True)
    shard_pattern = str(output_dir / f"{input_dir.name}-%06d.tar")

    print(
        f"[INFO] Processing {input_dir} -> {output_dir}, "
        f"{len(parquet_files)} parquet files, shard_pattern={shard_pattern}"
    )

    docs_iter = iter_docs_from_parquet_files(parquet_files)

    with wds.ShardWriter(shard_pattern, maxcount=max_samples_per_shard) as sink:
        for sample in docs_iter:
            sink.write(sample)


def parse_args():
    p = argparse.ArgumentParser(
        description="Convert OmniCorpus parquet in one directory to WebDataset shards."
    )
    p.add_argument(
        "--input-dir",
        type=str,
        default=None,
        help="Directory containing parquet files (recursively). If not set, uses $INPUT_DIR.",
    )
    p.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Directory to write WebDataset shards. If not set, uses $OUTPUT_DIR.",
    )
    p.add_argument(
        "--max-parquet",
        type=int,
        default=None,
        help="Limit number of parquet files (default: all).",
    )
    p.add_argument(
        "--max-samples-per-shard",
        type=int,
        default=10000,
        help="Max number of samples per shard.",
    )
    return p.parse_args()


def main():
    args = parse_args()

    input_dir = args.input_dir or os.environ.get("INPUT_DIR")
    output_dir = args.output_dir or os.environ.get("OUTPUT_DIR")

    if not input_dir or not output_dir:
        raise SystemExit(
            "You must specify --input-dir and --output-dir, or set INPUT_DIR and OUTPUT_DIR env vars."
        )

    make_shards_for_dir(
        input_dir=Path(input_dir),
        output_dir=Path(output_dir),
        max_parquet_files=args.max_parquet,
        max_samples_per_shard=args.max_samples_per_shard,
    )


if __name__ == "__main__":
    main()
