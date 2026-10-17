#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"

MODE_LABEL=""
TOTAL_REQUESTS=""
WORKLOADS=(read-only-no-reuse-medium,read-only-reuse-medium,write-only-medium)
CONCURRENCIES=(10)
RESET_BEFORE_RUN=true
RESET_SLEEP_SECONDS=3
GENERATED_DIR="$ROOT_DIR/experiments/benchmark/generated/compact"
PRIVATE_EXPORT_DIR="$ROOT_DIR/private-data/exports"
SYNC_READ_DATA=true
DOMAIN_ID="default"
DOMAIN_MIX=""
SEED=1
EXTRA_ARGS=()

usage() {
  cat <<EOF
Usage: $0 <mode-label> [options] [-- benchmark args...]

Run the compact benchmark matrix:
  workloads:    ${WORKLOADS[*]}
  concurrency:  ${CONCURRENCIES[*]}

Options:
  --total-requests N       Override request count for each workload. Default: use workload JSON.
  --workloads LIST         Comma-separated workload filenames, stems, or JSON paths.
                           Default: ${WORKLOADS[*]}
  --concurrency LIST       Comma-separated concurrency levels. Default: ${CONCURRENCIES[*]}
  --generated-dir DIR      Directory for generated traces/files. Default: $GENERATED_DIR
  --private-export-dir DIR Local directory for generated read files before sync.
                           Default: $PRIVATE_EXPORT_DIR
  --domain-id ID           Domain id written into generated traces. Default: $DOMAIN_ID
  --domain-mix SPEC        Domain ratio mix, e.g. domain-a:0.7,domain-b:0.3.
  --seed N                 Trace generation seed. Default: $SEED
  --no-sync-read-data      Do not run sync_to_nodes.sh after generating read files.
  --no-reset               Do not run reset_benchmark_state.sh before each point.
  --reset-sleep N          Seconds to sleep after reset. Default: $RESET_SLEEP_SECONDS
  -h, --help               Show this help message.

Anything after -- is passed through to run_benchmark.sh.

Examples:
  $0 decoupled-reuse
  $0 coupled-rpc --total-requests 500
  $0 decoupled-baseline --workloads read_heavy_reuse,mixed,write_heavy_no_reuse --concurrency 1,10,50 -- --timeout 600
EOF
}

split_csv() {
  local value="$1"
  local -n target="$2"
  IFS=',' read -r -a target <<< "$value"
}

normalize_csv_array() {
  local -n source="$1"
  local normalized=()
  local item
  local part
  local -a parts
  for item in "${source[@]}"; do
    IFS=',' read -r -a parts <<< "$item"
    for part in "${parts[@]}"; do
      if [[ -n "$part" ]]; then
        normalized+=("$part")
      fi
    done
  done
  source=("${normalized[@]}")
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

resolve_workload_json() {
  local workload="$1"
  local workload_dir="$ROOT_DIR/experiments/workloads"
  local path=""

  if [[ "$workload" = /* ]]; then
    path="$workload"
  elif [[ "$workload" == *.json && -f "$ROOT_DIR/$workload" ]]; then
    path="$ROOT_DIR/$workload"
  elif [[ "$workload" == *.json && -f "$workload_dir/$workload" ]]; then
    path="$workload_dir/$workload"
  else
    local stem="${workload%.json}"
    local normalized="${stem//-/_}"
    path="$workload_dir/$normalized.json"
  fi

  if [[ -f "$path" ]]; then
    echo "$path"
    return
  fi

  echo "Unknown generated workload: $workload" >&2
  echo "Expected a workload filename under experiments/workloads, e.g.:" >&2
  find "$workload_dir" -maxdepth 1 -type f -name '*.json' -printf '  %f\n' | sort >&2
  exit 1
}

workload_name() {
  local workload_json="$1"
  "$(python_bin)" - "$workload_json" <<'PY'
import sys
from pathlib import Path

path = Path(sys.argv[1])
print(path.stem.replace("_", "-"))
PY
}

prepare_workload() {
  local workload_json="$1"
  local name="$2"
  local output_dir="$GENERATED_DIR/$name"
  local trace_json="$output_dir/$name.trace.json"
  local args=(
    --role admin
    --workload-json "$workload_json"
    --name "$name"
    --output-dir "$output_dir"
    --domain-id "$DOMAIN_ID"
    --domain-mix "$DOMAIN_MIX"
    --seed "$SEED"
  )
  if [[ -n "$TOTAL_REQUESTS" ]]; then
    args+=(--total-requests "$TOTAL_REQUESTS")
  fi

  mkdir -p "$output_dir"
  "$(python_bin)" "$ROOT_DIR/experiments/benchmark/generate_dataset.py" \
    "${args[@]}"

  echo "[compact] generating read files locally for private storage: $PRIVATE_EXPORT_DIR"
  "$(python_bin)" "$ROOT_DIR/experiments/benchmark/generate_dataset.py" \
    --role private \
    --trace-json "$trace_json" \
    --private-export-dir "$PRIVATE_EXPORT_DIR"

  if [[ "$SYNC_READ_DATA" == true ]]; then
    echo "[compact] syncing generated read files to nodes"
    bash "$ROOT_DIR/sync_to_nodes.sh"
  else
    echo "[compact] skipping read data sync"
  fi

  echo "[compact] generating client write files: $output_dir/writes"
  "$(python_bin)" "$ROOT_DIR/experiments/benchmark/generate_dataset.py" \
    --role client \
    --trace-json "$trace_json" \
    --write-input-dir "$output_dir/writes"
}

if [[ $# -eq 0 ]]; then
  usage >&2
  exit 1
fi

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

MODE_LABEL="$1"
shift

while [[ $# -gt 0 ]]; do
  case "$1" in
    --total-requests)
      TOTAL_REQUESTS="$2"
      shift
      ;;
    --workloads)
      split_csv "$2" WORKLOADS
      shift
      ;;
    --concurrency)
      split_csv "$2" CONCURRENCIES
      shift
      ;;
    --generated-dir)
      GENERATED_DIR="$2"
      shift
      ;;
    --private-export-dir)
      PRIVATE_EXPORT_DIR="$2"
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
    --no-sync-read-data)
      SYNC_READ_DATA=false
      ;;
    --no-reset)
      RESET_BEFORE_RUN=false
      ;;
    --reset-sleep)
      RESET_SLEEP_SECONDS="$2"
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

if [[ "${#WORKLOADS[@]}" -eq 0 || "${#CONCURRENCIES[@]}" -eq 0 ]]; then
  echo "workloads and concurrency lists must not be empty" >&2
  exit 1
fi

normalize_csv_array WORKLOADS
normalize_csv_array CONCURRENCIES

if [[ "${#WORKLOADS[@]}" -eq 0 || "${#CONCURRENCIES[@]}" -eq 0 ]]; then
  echo "workloads and concurrency lists must not be empty" >&2
  exit 1
fi

if [[ -n "$TOTAL_REQUESTS" ]]; then
  echo "[compact] mode=$MODE_LABEL total_requests_override=$TOTAL_REQUESTS reset=$RESET_BEFORE_RUN"
else
  echo "[compact] mode=$MODE_LABEL total_requests=workload-json reset=$RESET_BEFORE_RUN"
fi
echo "[compact] workloads=${WORKLOADS[*]}"
echo "[compact] concurrencies=${CONCURRENCIES[*]}"
echo "[compact] generated_dir=$GENERATED_DIR seed=$SEED"
echo "[compact] private_export_dir=$PRIVATE_EXPORT_DIR sync_read_data=$SYNC_READ_DATA domain_id=$DOMAIN_ID domain_mix=$DOMAIN_MIX"

for workload in "${WORKLOADS[@]}"; do
  workload_json="$(resolve_workload_json "$workload")"
  name="$(workload_name "$workload_json")"
  output_dir="$GENERATED_DIR/$name"
  trace_json="$output_dir/$name.trace.json"

  echo "[compact] preparing workload=$name json=$workload_json"
  prepare_workload "$workload_json" "$name"

  for concurrency in "${CONCURRENCIES[@]}"; do
    echo "[compact] running workload=$name concurrency=$concurrency trace=$trace_json"
    if [[ "$RESET_BEFORE_RUN" == true ]]; then
      bash "$ROOT_DIR/deploy/scripts/reset_benchmark_state.sh"
      if [[ "$RESET_SLEEP_SECONDS" != "0" ]]; then
        echo "[compact] sleeping ${RESET_SLEEP_SECONDS}s after reset"
        sleep "$RESET_SLEEP_SECONDS"
      fi
    fi
    bash "$ROOT_DIR/deploy/scripts/run_benchmark.sh" "$MODE_LABEL" \
      --trace-json "$trace_json" \
      --trace-base-dir "$output_dir" \
      --concurrency "$concurrency" \
      "${EXTRA_ARGS[@]}"
  done
done

echo "[compact] completed"
