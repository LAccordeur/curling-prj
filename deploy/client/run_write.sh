#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT_DIR/services/client"
source "$ROOT_DIR/.venv/bin/activate"
set -a
source "$ROOT_DIR/deploy/client/.env"
set +a
python submit_write.py
