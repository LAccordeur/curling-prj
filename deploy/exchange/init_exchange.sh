#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
ENV_FILE="$ROOT_DIR/deploy/exchange/.env"
INV_FILE="$ROOT_DIR/inventory.env"
set -a
source "$ENV_FILE"
if [[ -f "$INV_FILE" ]]; then
  source "$INV_FILE"
fi
set +a

# An exported empty profile is not the same as an unset profile for AWS SDKs.
if [[ -z "${AWS_PROFILE:-}" ]]; then
  unset AWS_PROFILE
fi

export AWS_DEFAULT_REGION="${AWS_REGION:-us-east-1}"
OBJECT_STORE_BACKEND="${OBJECT_STORE_BACKEND:-minio}"
QUEUE_BACKEND="${QUEUE_BACKEND:-elasticmq}"

if ! command -v aws >/dev/null 2>&1; then
  source "$ROOT_DIR/.venv/bin/activate"
  pip install awscli >/dev/null
fi

is_aws_s3_backend() {
  [[ "$OBJECT_STORE_BACKEND" == "aws_s3" || "$OBJECT_STORE_BACKEND" == "s3" || "$OBJECT_STORE_BACKEND" == "aws" ]]
}

is_aws_queue_backend() {
  [[ "$QUEUE_BACKEND" == "aws_sqs" || "$QUEUE_BACKEND" == "sqs" || "$QUEUE_BACKEND" == "aws" ]]
}

create_bucket() {
  local bucket="$1"
  if is_aws_s3_backend; then
    aws s3 mb "s3://$bucket" --region "${AWS_REGION:-us-east-1}" || true
  else
    AWS_ACCESS_KEY_ID="${MINIO_ACCESS_KEY_ID:-$MINIO_ROOT_USER}" \
    AWS_SECRET_ACCESS_KEY="${MINIO_SECRET_ACCESS_KEY:-$MINIO_ROOT_PASSWORD}" \
      env -u AWS_SESSION_TOKEN -u AWS_SECURITY_TOKEN aws --endpoint-url="$MINIO_ENDPOINT" s3 mb "s3://$bucket" || true
  fi
}

create_queue() {
  local queue_name="$1"
  if is_aws_queue_backend; then
    aws sqs create-queue --queue-name "$queue_name" --region "${AWS_REGION:-us-east-1}" || true
  else
    AWS_ACCESS_KEY_ID=x \
    AWS_SECRET_ACCESS_KEY=x \
      env -u AWS_SESSION_TOKEN -u AWS_SECURITY_TOKEN aws --endpoint-url="$ELASTICMQ_ENDPOINT" sqs create-queue --queue-name "$queue_name" || true
  fi
}

list_queues() {
  if is_aws_queue_backend; then
    aws sqs list-queues --region "${AWS_REGION:-us-east-1}" || true
  else
    AWS_ACCESS_KEY_ID=x \
    AWS_SECRET_ACCESS_KEY=x \
      env -u AWS_SESSION_TOKEN -u AWS_SECURITY_TOKEN aws --endpoint-url="$ELASTICMQ_ENDPOINT" sqs list-queues
  fi
}

IFS=',' read -r -a staged_buckets <<< "${PRIVATE_DOMAIN_STAGED_BUCKETS:-$S3_BUCKET_STAGED}"
IFS=',' read -r -a released_buckets <<< "${PRIVATE_DOMAIN_RELEASED_BUCKETS:-$S3_BUCKET_RELEASED}"

for bucket in "${staged_buckets[@]}"; do
  if [[ -n "$bucket" ]]; then
    echo "[exchange] creating staged bucket $bucket"
    create_bucket "$bucket"
  fi
done

for bucket in "${released_buckets[@]}"; do
  if [[ -n "$bucket" ]]; then
    echo "[exchange] creating released bucket $bucket"
    create_bucket "$bucket"
  fi
done

IFS=',' read -r -a queue_names <<< "${PRIVATE_DOMAIN_QUEUES:-$SQS_QUEUE_NAME}"
for queue_name in "${queue_names[@]}"; do
  if [[ -n "$queue_name" ]]; then
    echo "[exchange] creating queue $queue_name"
    create_queue "$queue_name"
  fi
done
list_queues
