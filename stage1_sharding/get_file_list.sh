#!/usr/bin/env bash
set -euo pipefail

INPUT_ROOT=/n/holylfs06/LABS/kempner_shared/Everyone/testbed/multimodal/OmniCorpus/raw

# List first-level CC-MAIN-* dirs into a file
find "$INPUT_ROOT" -maxdepth 1 -mindepth 1 -type d | sort > ../data/subdirs.txt

