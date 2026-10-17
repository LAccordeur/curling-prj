#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT_DIR/services/api"
source "$ROOT_DIR/.venv/bin/activate"
set -a
source "$ROOT_DIR/deploy/control-plane/.env"
set +a
uvicorn main:app --host 0.0.0.0 --port "${API_PORT:-8000}"
