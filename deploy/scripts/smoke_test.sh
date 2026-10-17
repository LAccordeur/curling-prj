#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
set -a
source "$ROOT_DIR/inventory.env"
set +a
QUEUE_HOST="${NODE6_HOST:-$NODE2_HOST}"

echo "[1] API health"
curl -fsS "http://${NODE1_HOST}:${API_PORT}/health" | jq . || curl -fsS "http://${NODE1_HOST}:${API_PORT}/health"

echo "[2] ElasticMQ basic endpoint"
curl -fsS "http://${QUEUE_HOST}:${ELASTICMQ_PORT}" >/dev/null || true

echo "[3] Submit read request"
source "$ROOT_DIR/deploy/client/.env"
source "$ROOT_DIR/.venv/bin/activate"
python "$ROOT_DIR/services/client/submit_read.py"

echo "Smoke test completed."
