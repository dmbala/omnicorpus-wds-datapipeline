#!/bin/bash
# generate_remaining.sh
# Generates json_shards_remaining.txt with .tar paths that have no corresponding .tar.done

SHARD_DIR="/n/holylfs06/LABS/kempner_shared/Everyone/testbed/multimodal/OmniCorpus/webdataset_shards"
OUTPUT="$(dirname "$(realpath "$0")")/../data/json_shards_remaining.txt"

find "$SHARD_DIR" -maxdepth 2 -name "*.tar" -not -name "*.tar.*" | sort | while read -r tar_file; do
  if [ ! -f "${tar_file}.done" ]; then
    echo "$tar_file"
  fi
done > "$OUTPUT"

echo "Generated $OUTPUT with $(wc -l < "$OUTPUT") remaining shards"


