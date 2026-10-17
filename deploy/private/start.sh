#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT_DIR/services/executor"
source "$ROOT_DIR/.venv/bin/activate"
DOMAIN_ARG="${1:-${PRIVATE_DOMAIN_ID:-}}"
ENV_FILE="${PRIVATE_ENV_FILE:-$ROOT_DIR/deploy/private/.env}"
if [[ -n "$DOMAIN_ARG" ]]; then
  DOMAIN_ENV_FILE="$ROOT_DIR/deploy/private/${DOMAIN_ARG}.env"
  if [[ ! -f "$DOMAIN_ENV_FILE" ]]; then
    echo "Missing private domain env: $DOMAIN_ENV_FILE" >&2
    echo "Run bash create_cluster_envs.sh or bash create_private_env.sh first." >&2
    exit 1
  fi
  ENV_FILE="$DOMAIN_ENV_FILE"
fi
set -a
source "$ENV_FILE"
set +a
MODE="${REQUEST_EXECUTION_MODE:-queue}"
echo "[private] starting REQUEST_EXECUTION_MODE=${MODE} PRIVATE_DOMAIN_ID=${PRIVATE_DOMAIN_ID:-default} env=${ENV_FILE}"
case "$MODE" in
  queue)
    echo "[private] queue mode: queue polling executor, scheduler=${EXECUTOR_SCHEDULER_MODE:-reuse}, workers=${EXECUTOR_WORKER_COUNT:-1}, noop=${EXECUTOR_NOOP_ENABLED:-false}, inflight_reuse=${EXECUTOR_INFLIGHT_REUSE_ENABLED:-true}"
    python executor.py
    ;;
  executor_rpc|rpc|coupled)
    echo "[private] executor_rpc mode: executor RPC server on port ${EXECUTOR_RPC_PORT:-9008}"
    uvicorn rpc_server:app --host 0.0.0.0 --port "${EXECUTOR_RPC_PORT:-9008}"
    ;;
  storage_api)
    echo "[private] storage_api mode: private storage API on port ${PRIVATE_STORAGE_API_PORT:-9010}"
    uvicorn storage_server:app --host 0.0.0.0 --port "${PRIVATE_STORAGE_API_PORT:-9010}"
    ;;
  *)
    echo "Unsupported REQUEST_EXECUTION_MODE for private service: ${MODE}"
    exit 1
    ;;
esac
