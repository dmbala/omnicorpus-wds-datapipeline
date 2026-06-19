#!/usr/bin/env bash
set -euo pipefail

# Long-running dispatcher: submits chunked job arrays gradually to stay under job-submit limits.
# It waits until there is capacity for another full chunk (based on the count of active array elements)
# and then submits exactly one more chunk using submit_chunks.sh.

SUBMITTER="./submit_chunks.sh"
SCRIPT="array_json_to_pair_wds_ip.slrm"
LIST="../data/json_shards_all.txt"
JOBLOG="chunk_jobs.tsv"

CHUNK=1000
MAXRUN=1000
MAX_ACTIVE=10000
POLL_SECS=120

DRY_RUN=0

usage() {
  cat <<EOF
Usage: $0 [options]

Options:
  --submitter PATH   submit_chunks.sh path (default: $SUBMITTER)
  --script PATH      Slurm worker script (default: $SCRIPT)
  --list PATH        Shard list file (default: $LIST)
  --joblog PATH      Job log (default: $JOBLOG)
  --chunk N          Array elements per chunk submission (default: $CHUNK)
  --maxrun N         % concurrency per chunk array (default: $MAXRUN)
  --max-active N     Maximum active array elements to keep in queue+run (default: $MAX_ACTIVE)
  --poll-secs N      Seconds between checks (default: $POLL_SECS)
  --dry-run          Print what would happen, do not submit
  -h, --help         Show help

Notes:
  Capacity rule: submit a new chunk only if (active_array_elements + CHUNK) <= MAX_ACTIVE.
  With defaults CHUNK=1000 and MAX_ACTIVE=10000, this submits the next chunk when active<=9000.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --submitter) SUBMITTER="$2"; shift 2;;
    --script) SCRIPT="$2"; shift 2;;
    --list) LIST="$2"; shift 2;;
    --joblog) JOBLOG="$2"; shift 2;;
    --chunk) CHUNK="$2"; shift 2;;
    --maxrun) MAXRUN="$2"; shift 2;;
    --max-active) MAX_ACTIVE="$2"; shift 2;;
    --poll-secs) POLL_SECS="$2"; shift 2;;
    --dry-run) DRY_RUN=1; shift;;
    -h|--help) usage; exit 0;;
    *) echo "Unknown arg: $1"; usage; exit 2;;
  esac
done

if [[ ! -x "$SUBMITTER" ]]; then
  echo "[ERROR] submitter not executable: $SUBMITTER" >&2
  exit 1
fi
if [[ ! -f "$SCRIPT" ]]; then
  echo "[ERROR] worker slurm script not found: $SCRIPT" >&2
  exit 1
fi
if [[ ! -f "$LIST" ]]; then
  echo "[ERROR] list file not found: $LIST" >&2
  exit 1
fi

NUM_LINES=$(wc -l < "$LIST")
echo "[INFO] Dispatcher starting"
echo "[INFO] list=$LIST lines=$NUM_LINES chunk=$CHUNK maxrun=$MAXRUN max_active=$MAX_ACTIVE poll=${POLL_SECS}s"

echo "[INFO] Tip: watch active elements with: squeue --array -u $USER -h | wc -l"

while true; do
  # Determine next offset from joblog (same logic as submit_chunks.sh --resume).
  next_offset=0
  if [[ -f "$JOBLOG" ]]; then
    next_offset=$(awk -F'\t' 'NR>1 && $2!="" { end=$3+$4 } END { if (end=="") end=0; print end }' "$JOBLOG")
  fi

  if (( next_offset >= NUM_LINES )); then
    echo "[INFO] All done: next_offset=$next_offset >= NUM_LINES=$NUM_LINES"
    exit 0
  fi

  # Count active (pending+running) array elements for this workflow.
  # Filter by job name from the worker script (#SBATCH --job-name=...); default in your script is js2imgs.
  # If you change the job-name, update JOB_NAME below.
  JOB_NAME=$(grep -m1 -E '^#SBATCH --job-name=' "$SCRIPT" | sed -E 's/^#SBATCH --job-name=//')
  JOB_NAME=${JOB_NAME:-js2imgs}

  active=$(squeue --array -u "$USER" -n "$JOB_NAME" -h -t PD,R | wc -l)

  ts=$(date -Is)
  echo "[$ts] next_offset=$next_offset active=$active (job-name=$JOB_NAME)"

  if (( active + CHUNK <= MAX_ACTIVE )); then
    echo "[$ts] capacity available; submitting next chunk (OFFSET=$next_offset)"
    if (( DRY_RUN )); then
      echo "[DRY] $SUBMITTER --script $SCRIPT --list $LIST --joblog $JOBLOG --chunk $CHUNK --maxrun $MAXRUN --resume --max-chunks 1"
    else
      if ! "$SUBMITTER" --script "$SCRIPT" --list "$LIST" --joblog "$JOBLOG" --chunk "$CHUNK" --maxrun "$MAXRUN" --resume --max-chunks 1; then
        echo "[$ts] submit failed; will retry after sleeping ${POLL_SECS}s" >&2
      fi
    fi
  fi

  sleep "$POLL_SECS"
done
