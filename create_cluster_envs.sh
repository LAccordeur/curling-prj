#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
INV_FILE="$ROOT_DIR/inventory.env"

if [ ! -f "$INV_FILE" ]; then
  echo "inventory.env not found. Run: bash create_inventory.sh"
  exit 1
fi

set -a
source "$INV_FILE"
set +a
MINIO_HOST="${NODE5_HOST:-$NODE2_HOST}"
QUEUE_HOST="${NODE6_HOST:-$NODE2_HOST}"
OBJECT_STORE_BACKEND="${OBJECT_STORE_BACKEND:-minio}"
QUEUE_BACKEND="${QUEUE_BACKEND:-elasticmq}"
source "$ROOT_DIR/deploy/scripts/domain_config.sh"
PRIVATE_DOMAINS_JSON_VALUE="$(private_domains_json)"

source "$ROOT_DIR/deploy/scripts/exchange_env.sh"

cat > "$ROOT_DIR/deploy/control-plane/.env" <<EOF3
DB_URL=postgresql://${DB_USER}:${DB_PASSWORD}@${NODE1_HOST}:${DB_PORT}/${DB_NAME}

$(emit_s3_env)
$(emit_queue_env)

DB_POOL_SIZE=${DB_POOL_SIZE:-5}
DB_MAX_OVERFLOW=${DB_MAX_OVERFLOW:-10}
DB_POOL_TIMEOUT=${DB_POOL_TIMEOUT:-30}

S3_BUCKET_STAGED=${S3_BUCKET_STAGED}
S3_BUCKET_RELEASED=${S3_BUCKET_RELEASED}
SQS_QUEUE_URL=$(domain_queue_url default)
PRIVATE_DOMAINS_JSON='${PRIVATE_DOMAINS_JSON_VALUE}'
API_PORT=${API_PORT}
API_LOG_ENABLED=${API_LOG_ENABLED:-true}
REQUEST_EXECUTION_MODE=${REQUEST_EXECUTION_MODE:-queue}
EXECUTOR_RPC_URL=http://${NODE3_HOST}:${EXECUTOR_RPC_PORT:-9008}
EXECUTOR_RPC_TIMEOUT_SECONDS=${EXECUTOR_RPC_TIMEOUT_SECONDS:-300}
PRIVATE_STORAGE_API_URL=http://${NODE3_HOST}:${PRIVATE_STORAGE_API_PORT:-9010}
PRIVATE_STORAGE_API_TIMEOUT_SECONDS=${PRIVATE_STORAGE_API_TIMEOUT_SECONDS:-300}
EOF3

cat > "$ROOT_DIR/deploy/exchange/.env" <<EOF3
MINIO_ROOT_USER=${MINIO_ROOT_USER}
MINIO_ROOT_PASSWORD=${MINIO_ROOT_PASSWORD}

MINIO_ENDPOINT=http://${MINIO_HOST}:${MINIO_PORT}
ELASTICMQ_ENDPOINT=http://${QUEUE_HOST}:${ELASTICMQ_PORT}

S3_BUCKET_STAGED=${S3_BUCKET_STAGED}
S3_BUCKET_RELEASED=${S3_BUCKET_RELEASED}
SQS_QUEUE_NAME=${SQS_QUEUE_NAME}
PRIVATE_DOMAIN_IDS="$(domain_ids)"
PRIVATE_DOMAIN_QUEUES=$(csv_join_domain_values domain_queue_name)
PRIVATE_DOMAIN_STAGED_BUCKETS=$(csv_join_domain_values domain_staged_bucket)
PRIVATE_DOMAIN_RELEASED_BUCKETS=$(csv_join_domain_values domain_released_bucket)
AWS_REGION=${AWS_REGION}
OBJECT_STORE_BACKEND=${OBJECT_STORE_BACKEND}
QUEUE_BACKEND=${QUEUE_BACKEND}
AWS_ACCOUNT_ID=${AWS_ACCOUNT_ID:-}
$(if [[ -n "${AWS_PROFILE:-}" ]]; then echo "AWS_PROFILE=${AWS_PROFILE}"; fi)
MINIO_ACCESS_KEY_ID=${MINIO_ACCESS_KEY_ID:-${MINIO_ROOT_USER}}
MINIO_SECRET_ACCESS_KEY=${MINIO_SECRET_ACCESS_KEY:-${MINIO_ROOT_PASSWORD}}
EOF3

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

cat > "$ROOT_DIR/deploy/client/.env" <<EOF3
CONTROL_API=http://${NODE1_HOST}:${API_PORT}
EOF3

echo "Generated deploy/control-plane/.env"
echo "Generated deploy/exchange/.env"
echo "Generated deploy/private/.env"
echo "Generated deploy/private/<domain>.env for: $(domain_ids)"
echo "Generated deploy/client/.env"
