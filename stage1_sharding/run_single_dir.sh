#!/usr/bin/env bash
set -euo pipefail

module load python/3.12.11-fasrc02
mamba activate /n/holylfs06/LABS/kempner_shared/Everyone/common_envs/imgdl

export INPUT_DIR=/n/holylfs06/LABS/kempner_shared/Everyone/testbed/multimodal/OmniCorpus/raw/CC-MAIN-2013-20
export OUTPUT_DIR=./webdataset_shards

python make_shards_single_dir.py --max-parquet 2 --max-samples-per-shard 10000
