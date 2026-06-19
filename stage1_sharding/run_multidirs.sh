#!/usr/bin/env bash
set -euo pipefail

module load python/3.12.11-fasrc02
mamba activate /n/holylfs06/LABS/kempner_shared/Everyone/common_envs/imgdl

INPUT_ROOT="/n/holylfs06/LABS/kempner_shared/Everyone/testbed/multimodal/OmniCorpus/raw"
OUTPUT_ROOT="./webdataset_shards" 

SCRIPT="./make_shards_single_dir.py"

# Optional limits
MAX_SUBDIRS=2          # leave empty for all
MAX_PARQUET_PER_SUBDIR=2   # leave empty for all
MAX_SAMPLES_PER_SHARD=40000

mkdir -p "$OUTPUT_ROOT"

count=0
for subdir in "$INPUT_ROOT"/*; do
  [ -d "$subdir" ] || continue

  if [[ -n "${MAX_SUBDIRS:-}" && $count -ge $MAX_SUBDIRS ]]; then
    echo "[DRIVER] Reached MAX_SUBDIRS=$MAX_SUBDIRS, stopping."
    break
  fi

  echo "$subdir"
  name="$(basename "$subdir")"
  out_subdir="$OUTPUT_ROOT/$name"
  mkdir -p "$out_subdir"

  echo "[DRIVER] Launching job for subdir: $subdir -> $out_subdir"

  INPUT_DIR="$subdir" OUTPUT_DIR="$out_subdir" \
    python "$SCRIPT" \
      ${MAX_PARQUET_PER_SUBDIR:+--max-parquet "$MAX_PARQUET_PER_SUBDIR"} \
      ${MAX_SAMPLES_PER_SHARD:+--max-samples-per-shard "$MAX_SAMPLES_PER_SHARD"}

  count=$((count + 1))
done

