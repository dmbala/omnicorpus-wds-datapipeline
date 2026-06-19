# OmniCorpus Data Collection

A distributed pipeline for converting [OmniCorpus](https://github.com/OpenGVLab/OmniCorpus) WebDataset shards from JSON-only format into image-text pairs, with downloaded images, suitable for multimodal training. OmniCorpus consists of approximately 10 billion image-text pairs, making it one of the largest publicly documented datasets of its kind.

## Overview

The pipeline runs in two stages:

1. **Sharding** (`Sharding/`): Converts raw OmniCorpus parquet files into JSON-only WebDataset shards (one shard per CC-MAIN snapshot directory).
2. **Image downloading** (root): Takes those JSON shards, downloads the referenced images, and writes image-text pair WebDataset archives.

**Scale**: ~98,599 total shards across 94 CC-MAIN snapshots.

## Directory Structure

```
omnicorpus-datacollect/
├── stage1_sharding/
│   ├── get_file_list.sh            # Lists CC-MAIN snapshot dirs into data/subdirs.txt
│   ├── make_shards_single_dir.py   # Converts parquet→JSON WebDataset for one dir
│   ├── run_single_dir.sh           # Manual test run for a single snapshot dir
│   ├── run_multidirs.sh            # Serial run over all snapshot dirs (local testing)
│   └── wds_array.slrm              # SLURM array job (one task per snapshot dir)
├── stage2_image_dl/
│   ├── json_to_pair_wds_ip.py      # Downloads images, writes image-text pair shards
│   ├── array_json_to_pair_wds_ip.slrm  # SLURM worker job script
│   ├── dispatch_chunks.sh          # Long-running dispatcher (rate-limits submissions)
│   ├── dispatch_chunks.slrm        # SLURM script for running the dispatcher as a job
│   ├── submit_chunks.sh            # Submits shard lists as SLURM array jobs
│   ├── pending_downloads_shards.sh # Finds unprocessed shards (missing .done files)
│   └── run_pair.sh                 # Example single-shard invocation
├── data/
│   ├── host_ip_map.csv             # Global DNS cache (51K domain→IP entries)
│   ├── subdirs.txt                 # One CC-MAIN-* path per line (94 entries)
│   ├── json_shards_all.txt         # Complete list of all shards (98,599 paths)
│   └── json_shards_remaining.txt   # Shards still needing processing
└── README.md
```

## Processing Pipeline

```
Stage 1 — Sharding (parquet → JSON WebDataset)
─────────────────────────────────────────────
OmniCorpus raw parquet files
        ↓
Sharding/get_file_list.sh   ← enumerates CC-MAIN-* snapshot dirs → subdirs.txt
        ↓
Sharding/wds_array.slrm     ← one SLURM task per snapshot dir
        ↓
Sharding/make_shards_single_dir.py  ← reads parquet, writes JSON WDS shards
        ↓
Output: CC-MAIN-YYYY-WW-NNNNNN.tar (JSON-only WebDataset shards)

Stage 2 — Image Downloading (JSON WebDataset → image-text pairs)
────────────────────────────────────────────────────────────────
json_shards_remaining.txt (shard paths from Stage 1)
        ↓
dispatch_chunks.sh   ← monitors queue capacity, polls every 120s
        ↓
submit_chunks.sh     ← splits list into ≤10,000-task SLURM array submissions
        ↓
array_json_to_pair_wds_ip.slrm  ← one SLURM task per shard
        ↓
json_to_pair_wds_ip.py   ← downloads images, writes output WebDataset pairs
        ↓
Output: image-text pair WebDataset shards (.tar) + completion markers (.done)
```

## Sharding Scripts

### `Sharding/get_file_list.sh` — Snapshot Discovery

Enumerates all CC-MAIN-* snapshot directories under the raw input root and writes them to `subdirs.txt`, one path per line.

```bash
bash stage1_sharding/get_file_list.sh
```

### `Sharding/make_shards_single_dir.py` — Parquet → JSON WebDataset

Reads all `.parquet` files under one CC-MAIN snapshot directory and writes JSON-only WebDataset shards. Each sample contains `images` (list of URLs) and `texts` (list of captions).

```bash
python stage1_sharding/make_shards_single_dir.py \
  --input-dir /path/to/CC-MAIN-2023-06 \
  --output-dir /path/to/output/CC-MAIN-2023-06 \
  --max-samples-per-shard 10000
```

Accepts `INPUT_DIR` / `OUTPUT_DIR` environment variables as an alternative to CLI flags.

### `Sharding/wds_array.slrm` — SLURM Array Job

Submits one task per line in `subdirs.txt`. Update `--array` to match the number of snapshot dirs, then submit:

```bash
bash stage1_sharding/get_file_list.sh   # regenerate data/subdirs.txt if needed
NUM=$(wc -l < data/subdirs.txt)
cd stage1_sharding && sbatch --array=0-$((NUM - 1)) wds_array.slrm
```

### `Sharding/run_single_dir.sh` / `run_multidirs.sh` — Local Testing

`run_single_dir.sh` runs `make_shards_single_dir.py` against a single hardcoded snapshot directory. `run_multidirs.sh` iterates serially over all snapshot directories — useful for small-scale testing before submitting the array job.

## Key Scripts (Stage 2)

### `json_to_pair_wds_ip.py` — Main Processor

Processes a single JSON WebDataset shard into image-text pair shards.

**Input**: WebDataset tar containing JSON samples with structure:
```json
{
  "images": ["url1", "url2", ...],
  "texts": ["caption1", "caption2", ...]
}
```

**Output**: WebDataset tars with per-image samples:
- `{key}.jpg` / `{key}.png` / `{key}.gif` — downloaded image bytes
- `{key}.txt` — associated text
- `{key}.json` — metadata (shard info, doc index, source URL)

**Features**:
- Multi-tier DNS resolution: local IP cache → campus nameservers → external (Google/Cloudflare)
- Exponential backoff with respect for HTTP 429/503 `Retry-After` headers
- SSL/SNI fallback for HTTPS failures
- Per-shard `.done` files for fault-tolerant resumption
- 10 MB per-image size limit; content-type validation

```bash
python json_to_pair_wds_ip.py \
  --input_shard /path/to/input.tar \
  --output_dir /path/to/output/ \
  --ip_map host_ip_map.csv \
  --max_images_per_doc 40 \
  --maxcount 10000
```

### `submit_chunks.sh` — Batch Submitter

Splits a large shard list into multiple SLURM array job submissions, respecting the SLURM `MaxArraySize` limit (10,000 elements).

```bash
# Submit all remaining shards in 1000-task chunks, 10 running concurrently
bash submit_chunks.sh \
  --input json_shards_remaining.txt \
  --slurm array_json_to_pair_wds_ip.slrm \
  --chunk 1000 \
  --maxrun 10

# Resume from last logged submission
bash submit_chunks.sh --resume
```

A job log (`chunk_jobs.tsv`) tracks all submissions.

### `dispatch_chunks.sh` — Queue-Aware Dispatcher

Wraps `submit_chunks.sh` in a polling loop that only submits new chunks when the queue has capacity. Prevents overloading the scheduler.

```bash
bash dispatch_chunks.sh \
  --input json_shards_remaining.txt \
  --slurm array_json_to_pair_wds_ip.slrm \
  --chunk 1000 \
  --max-active 10000 \
  --interval 120
```

Can also be submitted as a SLURM job via `dispatch_chunks.slrm`.

### `pending_downloads_shards.sh` — Shard Discovery

Regenerates `json_shards_remaining.txt` by scanning for input `.tar` files that lack a corresponding `.done` marker.

```bash
bash pending_downloads_shards.sh
```

## DNS Caching

Image downloads hit millions of distinct hostnames. To avoid repeated DNS lookups:

- **`host_ip_map.csv`**: Global pre-computed cache with 51,170 domain→IP entries, loaded at startup.
- **Per-shard `.ip` files**: New resolutions discovered during processing are saved alongside each output shard and merged into the global cache on future runs.

DNS resolution order: system resolver → campus nameservers (randomized) → Google/Cloudflare fallback.

## Environment

- **Cluster**: Harvard FASRC / Kempner Institute HPC
- **Scheduler**: SLURM
- **Python**: 3.12 (`imgdl` conda environment)
- **Key dependencies**: `webdataset`, `requests`, `dnspython`

## Fault Tolerance

- Each shard writes a `.done` file on completion; failed shards lack this file.
- `pending_downloads_shards.sh` identifies and regenerates the remaining list.
- `submit_chunks.sh --resume` continues submission from the last logged chunk.
- Within a shard, already-downloaded pairs are tracked so processing can resume mid-shard.

## Data Source

Input shards are sourced from the OmniCorpus dataset, organized by Common Crawl snapshot:

```
/n/holylfs06/LABS/kempner_shared/Everyone/testbed/multimodal/OmniCorpus/
└── CC-MAIN-YYYY-WW/
    ├── CC-MAIN-YYYY-WW-000000.tar
    ├── CC-MAIN-YYYY-WW-000001.tar
    └── ...
```

**HuggingFace**:
- Collection: https://huggingface.co/collections/OpenGVLab/omnicorpus
- OmniCorpus-CC (Common Crawl subset used here): https://huggingface.co/datasets/OpenGVLab/OmniCorpus-CC

## Citation

```bibtex
@article{li2024omnicorpus,
  title={OmniCorpus: A Unified Multimodal Corpus of 10 Billion-Level Images Interleaved with Text},
  author={Li, Qingyun and Chen, Zhe and Wang, Weiyun and Wang, Wenhai and Ye, Shenglong and
          Jin, Zhenjiang and Chen, Guanzhou and He, Yinan and Gao, Zhangwei and Cui, Erfei and
          Yu, Jiashuo and Tian, Hao and Zhou, Jiasheng and Xu, Chao and Wang, Bin and
          Wei, Xingjian and Li, Wei and Zhang, Wenjian and Zhang, Bo and Cai, Pinlong and
          Wen, Licheng and Yan, Xiangchao},
  journal={arXiv preprint arXiv:2406.08418},
  year={2024}
}
```
