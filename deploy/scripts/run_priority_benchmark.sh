#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"

MODE_LABEL="decoupled-baseline-priority"
TOTAL_REQUESTS=1000
CONCURRENCY=40
GENERATED_DIR="$ROOT_DIR/experiments/benchmark/generated/priority"
OUTPUT_DIR="$ROOT_DIR/experiments/benchmark/results_priority"
PRIVATE_EXPORT_DIR="$ROOT_DIR/private-data/exports"
SYNC_COMMAND="bash $ROOT_DIR/sync_to_nodes.sh"
RESET_COMMAND="bash $ROOT_DIR/deploy/scripts/reset_benchmark_state.sh"
RESET_SLEEP_SECONDS=3
SIZE_PROFILE="small=4096,medium=8388608,large=67108864"
SIZE_MIX="small:0.6,medium:0.2,large:0.2"
PRIORITY_BY_SIZE="small:2,medium:1,large:0"
READ_RATIO=1.0
READ_TARGET_COUNT=""
READ_REPEAT_MODE="unique"
DOMAIN_ID="default"
DOMAIN_MIX=""
SEED=1
EXTRA_ARGS=()

usage() {
  cat <<EOF
Usage: $0 [options] [-- priority benchmark args...]

Run the paired priority scheduling benchmark:
  1. uniform priority workload
  2. small-first priority workload

Options:
  --mode-label LABEL        Benchmark mode label. Default: $MODE_LABEL
  --total-requests N        Number of generated requests. Default: $TOTAL_REQUESTS
  --concurrency N           Client concurrency. Default: $CONCURRENCY
  --generated-dir DIR       Generated trace/data directory. Default: $GENERATED_DIR
  --output-dir DIR          Benchmark result directory. Default: $OUTPUT_DIR
  --private-export-dir DIR  Local private export directory before sync. Default: $PRIVATE_EXPORT_DIR
  --sync-command CMD        Command used after generating private read files.
                            Default: $SYNC_COMMAND
  --reset-command CMD       Command used before each benchmark run.
                            Default: $RESET_COMMAND
  --reset-sleep N           Seconds to sleep after reset. Default: $RESET_SLEEP_SECONDS
  --size-profile SPEC       Size profile. Default: $SIZE_PROFILE
  --size-mix SPEC           Size mix. Default: $SIZE_MIX
  --priority-by-size SPEC   Priority mapping. Default: $PRIORITY_BY_SIZE
  --read-ratio N            Read ratio. Default: $READ_RATIO
  --read-target-count N     Read target count. Default: total requests
  --read-repeat-mode MODE   unique, uniform, or zipf. Default: $READ_REPEAT_MODE
  --domain-id ID            Domain id written into generated traces. Default: $DOMAIN_ID
  --domain-mix SPEC         Domain ratio mix, e.g. domain-a:0.7,domain-b:0.3.
  --seed N                  Trace generation seed. Default: $SEED
  -h, --help                Show this help message.

Anything after -- is passed through to run_priority_benchmark.py.

Examples:
  $0 --total-requests 300
  $0 --total-requests 300 --sync-command "bash sync_to_nodes.sh" --reset-command "bash deploy/scripts/reset_benchmark_state.sh"
  $0 --total-requests 120 --concurrency 40 -- --timeout 900
EOF
}

python_bin() {
  if [[ -x "$ROOT_DIR/.venv/bin/python" ]]; then
    echo "$ROOT_DIR/.venv/bin/python"
    return
  fi
  if command -v python3 >/dev/null 2>&1; then
    command -v python3
    return
  fi
  echo "python3 is required" >&2
  exit 1
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode-label)
      MODE_LABEL="$2"
      shift
      ;;
    --total-requests)
      TOTAL_REQUESTS="$2"
      shift
      ;;
    --concurrency)
      CONCURRENCY="$2"
      shift
      ;;
    --generated-dir)
      GENERATED_DIR="$2"
      shift
      ;;
    --output-dir)
      OUTPUT_DIR="$2"
      shift
      ;;
    --private-export-dir)
      PRIVATE_EXPORT_DIR="$2"
      shift
      ;;
    --sync-command)
      SYNC_COMMAND="$2"
      shift
      ;;
    --reset-command)
      RESET_COMMAND="$2"
      shift
      ;;
    --reset-sleep)
      RESET_SLEEP_SECONDS="$2"
      shift
      ;;
    --size-profile)
      SIZE_PROFILE="$2"
      shift
      ;;
    --size-mix)
      SIZE_MIX="$2"
      shift
      ;;
    --priority-by-size)
      PRIORITY_BY_SIZE="$2"
      shift
      ;;
    --read-ratio)
      READ_RATIO="$2"
      shift
      ;;
    --read-target-count)
      READ_TARGET_COUNT="$2"
      shift
      ;;
    --read-repeat-mode)
      READ_REPEAT_MODE="$2"
      shift
      ;;
    --domain-id)
      DOMAIN_ID="$2"
      shift
      ;;
    --domain-mix)
      DOMAIN_MIX="$2"
      shift
      ;;
    --seed)
      SEED="$2"
      shift
      ;;
    --)
      shift
      EXTRA_ARGS+=("$@")
      break
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
  shift
done

set -a
source "$ROOT_DIR/inventory.env"
set +a

CONTROL_API="${CONTROL_API:-http://${NODE1_HOST}:${API_PORT}}"
READ_TARGET_COUNT="${READ_TARGET_COUNT:-$TOTAL_REQUESTS}"

echo "[priority] mode=$MODE_LABEL total_requests=$TOTAL_REQUESTS concurrency=$CONCURRENCY"
echo "[priority] api=$CONTROL_API"
echo "[priority] sync_command=$SYNC_COMMAND"
echo "[priority] reset_command=$RESET_COMMAND"
echo "[priority] size_profile=$SIZE_PROFILE size_mix=$SIZE_MIX priority_by_size=$PRIORITY_BY_SIZE domain_id=$DOMAIN_ID domain_mix=$DOMAIN_MIX"

"$(python_bin)" "$ROOT_DIR/experiments/benchmark/run_priority_benchmark.py" \
  --api "$CONTROL_API" \
  --mode "$MODE_LABEL" \
  --generated-dir "$GENERATED_DIR" \
  --output-dir "$OUTPUT_DIR" \
  --private-export-dir "$PRIVATE_EXPORT_DIR" \
  --sync-command "$SYNC_COMMAND" \
  --reset-command "$RESET_COMMAND" \
  --reset-sleep "$RESET_SLEEP_SECONDS" \
  --total-requests "$TOTAL_REQUESTS" \
  --concurrency "$CONCURRENCY" \
  --size-profile "$SIZE_PROFILE" \
  --size-mix "$SIZE_MIX" \
  --priority-by-size "$PRIORITY_BY_SIZE" \
  --domain-id "$DOMAIN_ID" \
  --domain-mix "$DOMAIN_MIX" \
  --read-ratio "$READ_RATIO" \
  --read-target-count "$READ_TARGET_COUNT" \
  --read-repeat-mode "$READ_REPEAT_MODE" \
  --seed "$SEED" \
  "${EXTRA_ARGS[@]}"
