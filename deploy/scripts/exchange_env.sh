#!/usr/bin/env bash
# Shared backend configuration for service env generators. AWS credentials stay on each node.
s3_endpoint_url() {
  if [[ -n "${S3_ENDPOINT_URL:-}" ]]; then
    echo "${S3_ENDPOINT_URL}"
  elif [[ "$OBJECT_STORE_BACKEND" == "aws_s3" || "$OBJECT_STORE_BACKEND" == "s3" || "$OBJECT_STORE_BACKEND" == "aws" ]]; then
    echo ""
  else
    echo "http://${MINIO_HOST}:${MINIO_PORT}"
  fi
}

emit_s3_env() {
  local endpoint
  endpoint="$(s3_endpoint_url)"
  echo "AWS_REGION=${AWS_REGION}"
  echo "OBJECT_STORE_BACKEND=${OBJECT_STORE_BACKEND}"
  echo "S3_ENDPOINT_URL=${endpoint}"
  if [[ "$OBJECT_STORE_BACKEND" == "minio" ]]; then
    echo "MINIO_ACCESS_KEY_ID=${MINIO_ACCESS_KEY_ID:-${MINIO_ROOT_USER}}"
    echo "MINIO_SECRET_ACCESS_KEY=${MINIO_SECRET_ACCESS_KEY:-${MINIO_ROOT_PASSWORD}}"
  fi
  if [[ -n "${AWS_PROFILE:-}" ]]; then
    echo "AWS_PROFILE=${AWS_PROFILE}"
  fi
  echo "AWS_MAX_POOL_CONNECTIONS=${AWS_MAX_POOL_CONNECTIONS:-10}"
}

queue_endpoint_url() {
  if [[ -n "${SQS_ENDPOINT_URL:-}" ]]; then
    echo "${SQS_ENDPOINT_URL}"
  elif [[ "$QUEUE_BACKEND" == "aws_sqs" || "$QUEUE_BACKEND" == "sqs" || "$QUEUE_BACKEND" == "aws" ]]; then
    echo ""
  else
    echo "http://${QUEUE_HOST}:${ELASTICMQ_PORT}"
  fi
}

emit_queue_env() {
  local endpoint
  endpoint="$(queue_endpoint_url)"
  echo "QUEUE_BACKEND=${QUEUE_BACKEND}"
  if [[ -n "${AWS_ACCOUNT_ID:-}" ]]; then
    echo "AWS_ACCOUNT_ID=${AWS_ACCOUNT_ID}"
  fi
  echo "SQS_ENDPOINT_URL=${endpoint}"
}
