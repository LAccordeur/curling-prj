#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
set -a
source "$ROOT_DIR/inventory.env"
set +a
MINIO_HOST="${NODE5_HOST:-$NODE2_HOST}"
QUEUE_HOST="${NODE6_HOST:-$NODE2_HOST}"
OBJECT_STORE_BACKEND="${OBJECT_STORE_BACKEND:-minio}"
QUEUE_BACKEND="${QUEUE_BACKEND:-elasticmq}"
source "$ROOT_DIR/deploy/scripts/domain_config.sh"

source "$ROOT_DIR/deploy/scripts/exchange_env.sh"

write_private_env() {
  local domain_id="$1"
  local output_file="$2"
cat > "$output_file" <<EOF3
DB_URL=postgresql://${DB_USER}:${DB_PASSWORD}@${NODE1_HOST}:${DB_PORT}/${DB_NAME}
$(emit_s3_env)
$(emit_queue_env)
DB_POOL_SIZE=${DB_POOL_SIZE:-5}
DB_MAX_OVERFLOW=${DB_MAX_OVERFLOW:-10}
DB_POOL_TIMEOUT=${DB_POOL_TIMEOUT:-30}
PRIVATE_DOMAIN_ID=${domain_id}
S3_BUCKET_STAGED=$(domain_staged_bucket "$domain_id")
S3_BUCKET_RELEASED=$(domain_released_bucket "$domain_id")
SQS_QUEUE_URL=$(domain_queue_url "$domain_id")
PRIVATE_BASE=$(domain_private_base "$domain_id")
WORK_BASE=$(domain_work_base "$domain_id")
EXECUTOR_LOG_ENABLED=${EXECUTOR_LOG_ENABLED:-true}
EXECUTOR_DIAGNOSTICS_ENABLED=${EXECUTOR_DIAGNOSTICS_ENABLED:-false}
EXECUTOR_DIAGNOSTICS_INTERVAL_SECONDS=${EXECUTOR_DIAGNOSTICS_INTERVAL_SECONDS:-1}
EXECUTOR_SCHEDULER_MODE=${EXECUTOR_SCHEDULER_MODE:-reuse}
EXECUTOR_STAGING_ENABLED=${EXECUTOR_STAGING_ENABLED:-false}
EXECUTOR_WORKER_COUNT=${EXECUTOR_WORKER_COUNT:-1}
EXECUTOR_NOOP_ENABLED=${EXECUTOR_NOOP_ENABLED:-false}
EXECUTOR_INFLIGHT_REUSE_ENABLED=${EXECUTOR_INFLIGHT_REUSE_ENABLED:-true}
EXECUTOR_MIN_BACKOFF_SECONDS=${EXECUTOR_MIN_BACKOFF_SECONDS:-0.01}
EXECUTOR_MAX_BACKOFF_SECONDS=${EXECUTOR_MAX_BACKOFF_SECONDS:-1.0}
EXECUTOR_SCHEDULER_BATCH_SIZE=${EXECUTOR_SCHEDULER_BATCH_SIZE:-${EXECUTOR_WORKER_COUNT:-1}}
EXECUTOR_SCHEDULER_BATCH_WAIT_SECONDS=${EXECUTOR_SCHEDULER_BATCH_WAIT_SECONDS:-0.02}
REQUEST_EXECUTION_MODE=${REQUEST_EXECUTION_MODE:-queue}
EXECUTOR_RPC_PORT=$(domain_rpc_port "$domain_id")
PRIVATE_STORAGE_API_PORT=$(domain_storage_api_port "$domain_id")
EOF3
}

first_private_env=1
for domain_id in $(domain_ids); do
  write_private_env "$domain_id" "$ROOT_DIR/deploy/private/${domain_id}.env"
  if [[ "$first_private_env" -eq 1 ]]; then
    cp "$ROOT_DIR/deploy/private/${domain_id}.env" "$ROOT_DIR/deploy/private/.env"
    first_private_env=0
  fi
done
echo "Generated deploy/private/.env"
echo "Generated deploy/private/<domain>.env for: $(domain_ids)"
