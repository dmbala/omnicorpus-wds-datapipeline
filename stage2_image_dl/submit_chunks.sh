#!/usr/bin/env bash
set -euo pipefail

# Submit a large list of shard paths as multiple Slurm job arrays, each <= CHUNK tasks.
# Requires the Slurm script to compute TASK_ID using OFFSET + SLURM_ARRAY_TASK_ID.
#
# Example:
#   ./submit_chunks.sh
#   ./submit_chunks.sh --chunk 10000 --maxrun 10
#   ./submit_chunks.sh --dry-run

SCRIPT="array_json_to_pair_wds_ip.slrm"
LIST="../data/json_shards_all.txt"
CHUNK=10000
MAXRUN=10
DRY_RUN=0
JOBLOG="chunk_jobs.tsv"
START_OFFSET=0
MAX_CHUNKS=0
RESUME=0

usage() {
  cat <<EOF
Usage: $0 [options]

Options:
  --script PATH     Slurm script to submit (default: $SCRIPT)
  --list PATH       Shard list file (default: $LIST)
  --chunk N         Tasks per submission (default: $CHUNK; must be <= MaxArraySize)
  --maxrun N        Max simultaneous tasks per array (default: $MAXRUN)
  --joblog PATH     Append job ids + metadata to PATH (default: $JOBLOG)
  --start-offset N  Start submitting at global index N (default: $START_OFFSET)
  --max-chunks N    Submit at most N chunks this run (default: unlimited)
  --resume          Continue from the end of the last logged chunk in --joblog
  --dry-run         Print sbatch commands without submitting
  -h, --help        Show this help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --script) SCRIPT="$2"; shift 2;;
    --list) LIST="$2"; shift 2;;
    --chunk) CHUNK="$2"; shift 2;;
    --maxrun) MAXRUN="$2"; shift 2;;
    --joblog) JOBLOG="$2"; shift 2;;
    --start-offset) START_OFFSET="$2"; shift 2;;
    --max-chunks) MAX_CHUNKS="$2"; shift 2;;
    --resume) RESUME=1; shift;;
    --dry-run) DRY_RUN=1; shift;;
    -h|--help) usage; exit 0;;
    *) echo "Unknown arg: $1"; usage; exit 2;;
  esac
done

if [[ ! -f "$SCRIPT" ]]; then
  echo "[ERROR] Slurm script not found: $SCRIPT" >&2
  exit 1
fi
if [[ ! -f "$LIST" ]]; then
  echo "[ERROR] List file not found: $LIST" >&2
  exit 1
fi
if ! [[ "$CHUNK" =~ ^[0-9]+$ ]] || (( CHUNK <= 0 )); then
  echo "[ERROR] --chunk must be a positive integer" >&2
  exit 1
fi
if ! [[ "$MAXRUN" =~ ^[0-9]+$ ]] || (( MAXRUN <= 0 )); then
  echo "[ERROR] --maxrun must be a positive integer" >&2
  exit 1
fi
if ! [[ "$START_OFFSET" =~ ^[0-9]+$ ]]; then
  echo "[ERROR] --start-offset must be a non-negative integer" >&2
  exit 1
fi
if ! [[ "$MAX_CHUNKS" =~ ^[0-9]+$ ]]; then
  echo "[ERROR] --max-chunks must be an integer (0 means unlimited)" >&2
  exit 1
fi

NUM_LINES=$(wc -l < "$LIST")
if (( NUM_LINES == 0 )); then
  echo "[ERROR] List file is empty: $LIST" >&2
  exit 1
fi

echo "[INFO] Script:     $SCRIPT"
echo "[INFO] List:       $LIST"
echo "[INFO] Num lines:  $NUM_LINES"
echo "[INFO] Chunk size: $CHUNK"
echo "[INFO] Max run:    $MAXRUN"
echo "[INFO] Job log:    $JOBLOG"

if (( RESUME )); then
  if [[ -f "$JOBLOG" ]]; then
    last_end=$(awk -F'\t' 'NR>1 && $2!="" { end=$3+$4 } END { if (end=="") end=0; print end }' "$JOBLOG")
    START_OFFSET=$last_end
    echo "[INFO] Resume:     START_OFFSET=$START_OFFSET (from $JOBLOG)"
  else
    echo "[WARN] --resume set but job log not found; starting from 0" >&2
    START_OFFSET=0
  fi
fi

if (( START_OFFSET >= NUM_LINES )); then
  echo "[INFO] START_OFFSET=$START_OFFSET >= NUM_LINES=$NUM_LINES; nothing to submit."
  exit 0
fi

if (( ! DRY_RUN )); then
  if [[ ! -f "$JOBLOG" ]]; then
    printf "timestamp\tjobid\toffset\tcount\tarray\tmaxrun\tlist\tscript\n" > "$JOBLOG"
  fi
fi

total_submitted=0
for ((OFFSET=START_OFFSET; OFFSET<NUM_LINES; OFFSET+=CHUNK)); do
  REM=$((NUM_LINES - OFFSET))
  NTHIS=$(( REM < CHUNK ? REM : CHUNK ))
  LAST=$(( NTHIS - 1 ))

  cmd=(sbatch
    --array=0-"$LAST"%"$MAXRUN"
    --export=ALL,OFFSET="$OFFSET",JSON_SHARD_LIST="$LIST"
    "$SCRIPT")

  if (( DRY_RUN )); then
    printf '[DRY] %q ' "${cmd[@]}"
    printf '\n'
  else
    # --parsable prints just the numeric job id (e.g. 123456)
    if ! jobid=$(sbatch --parsable \
        --array=0-"$LAST"%"$MAXRUN" \
        --export=ALL,OFFSET="$OFFSET",JSON_SHARD_LIST="$LIST" \
        "$SCRIPT"); then
      echo "[ERROR] sbatch failed at OFFSET=$OFFSET (array 0-$LAST%$MAXRUN)." >&2
      echo "[HINT] This is often MaxSubmitJobs/AssocMaxSubmitJobLimit. Try running fewer chunks at once:" >&2
      echo "       ./submit_chunks.sh --max-chunks 1" >&2
      echo "       then later: ./submit_chunks.sh --resume" >&2
      exit 1
    fi
    ts=$(date -Is)
    array_spec="0-$LAST%$MAXRUN"
    echo "[SUBMIT] OFFSET=$OFFSET ARRAY=$array_spec JOBID=$jobid"
    printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
      "$ts" "$jobid" "$OFFSET" "$NTHIS" "$array_spec" "$MAXRUN" "$LIST" "$SCRIPT" >> "$JOBLOG"
  fi

  total_submitted=$((total_submitted + 1))

  if (( MAX_CHUNKS > 0 && total_submitted >= MAX_CHUNKS )); then
    echo "[INFO] Reached --max-chunks=$MAX_CHUNKS; stopping."
    break
  fi
done

echo "[INFO] Done. Submitted $total_submitted chunk(s)."
