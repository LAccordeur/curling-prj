#!/usr/bin/env bash
set -euo pipefail

if [ "${1:-}" = "" ]; then
  echo "Usage: $0 <mode-label> [benchmark args...]"
  echo "Example: $0 decoupled-reuse --workload read-heavy --total-requests 200 --concurrency 25"
  exit 1
fi

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
MODE="$1"
shift

set -a
source "$ROOT_DIR/inventory.env"
set +a

source "$ROOT_DIR/.venv/bin/activate"
CONTROL_API="${CONTROL_API:-http://${NODE1_HOST}:${API_PORT}}" \
python "$ROOT_DIR/experiments/benchmark/run_benchmark.py" \
  --mode "$MODE" \
  --api "${CONTROL_API:-http://${NODE1_HOST}:${API_PORT}}" \
  "$@"
