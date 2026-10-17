#!/usr/bin/env bash
set -euo pipefail

if [ "${1:-}" = "" ]; then
  echo "Usage: $0 <coupled-rpc|coupled-storage-api|decoupled-baseline|decoupled-reuse>"
  exit 1
fi

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
MODE="$1"

set -a
source "$ROOT_DIR/inventory.env"
set +a

source "$ROOT_DIR/.venv/bin/activate"
CONTROL_API="${CONTROL_API:-http://${NODE1_HOST}:${API_PORT}}" \
python "$ROOT_DIR/tests/smoke/test_execution_mode.py" --mode "$MODE"
