#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
INV_FILE="$ROOT_DIR/inventory.env"
EXCHANGE_ENV_FILE="$ROOT_DIR/deploy/exchange/.env"

RESET_DB=true
RESET_S3=true
RESET_QUEUE=true

usage() {
  cat <<EOF
Usage: $0 [options]

Reset benchmark state before running a new experiment.

Options:
  --keep-db       Do not truncate control-plane metadata tables.
  --keep-s3       Do not remove staged/released objects from object storage.
  --keep-queue    Do not purge the request queue.
  -h, --help      Show this help message.

This script intentionally keeps private storage data under PRIVATE_BASE intact.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --keep-db)
      RESET_DB=false
      ;;
    --keep-s3)
      RESET_S3=false
      ;;
    --keep-queue)
      RESET_QUEUE=false
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
  shift
done

if [[ ! -f "$INV_FILE" ]]; then
  echo "Missing $INV_FILE. Run bash create_inventory.sh first." >&2
  exit 1
fi

if [[ ! -f "$EXCHANGE_ENV_FILE" ]]; then
  echo "Missing $EXCHANGE_ENV_FILE. Run bash create_cluster_envs.sh first." >&2
  exit 1
fi

set -a
source "$EXCHANGE_ENV_FILE"
source "$INV_FILE"
set +a

# Accept older generated env files containing AWS_PROFILE=.
if [[ -z "${AWS_PROFILE:-}" ]]; then
  unset AWS_PROFILE
fi

python_bin() {
  if [[ -x "$ROOT_DIR/.venv/bin/python" ]]; then
    echo "$ROOT_DIR/.venv/bin/python"
    return
  fi
  if command -v python3 >/dev/null 2>&1; then
    command -v python3
    return
  fi
  echo "python3 is required when aws CLI is unavailable." >&2
  exit 1
}

ensure_exchange_client() {
  if command -v aws >/dev/null 2>&1; then
    return
  fi

  local py
  py="$(python_bin)"
  if ! "$py" -c 'import boto3' >/dev/null 2>&1; then
    echo "aws CLI is not installed, and boto3 is unavailable in $py." >&2
    echo "Install awscli, or install service dependencies into .venv." >&2
    exit 1
  fi
}

is_aws_s3_backend() {
  local backend="${OBJECT_STORE_BACKEND:-minio}"
  [[ "$backend" == "aws_s3" || "$backend" == "s3" || "$backend" == "aws" ]]
}

is_aws_queue_backend() {
  local backend="${QUEUE_BACKEND:-elasticmq}"
  [[ "$backend" == "aws_sqs" || "$backend" == "sqs" || "$backend" == "aws" ]]
}

csv_to_array() {
  local value="$1"
  local -n out_ref="$2"
  IFS=',' read -r -a out_ref <<< "$value"
}

reset_db() {
  echo "[reset] truncating control-plane metadata tables"
  local sql="TRUNCATE TABLE artifacts, staged_uploads, requests;"

  if command -v psql >/dev/null 2>&1; then
    PGPASSWORD="$DB_PASSWORD" psql \
      -h "$NODE1_HOST" \
      -p "$DB_PORT" \
      -U "$DB_USER" \
      -d "$DB_NAME" \
      -c "$sql"
    return
  fi

  if command -v docker >/dev/null 2>&1 && docker ps --format '{{.Names}}' | grep -qx 'prototype-postgres'; then
    docker exec -e PGPASSWORD="$DB_PASSWORD" prototype-postgres \
      psql -U "$DB_USER" -d "$DB_NAME" -c "$sql"
    return
  fi

  if command -v ssh >/dev/null 2>&1 && [[ -n "${NODE1_USER:-}" && -n "${NODE1_HOST:-}" ]]; then
    local ssh_cmd=(ssh)
    if [[ -n "${SSH_KEY:-}" ]]; then
      ssh_cmd+=(-i "$SSH_KEY")
    fi
    if [[ -n "${SSH_OPTS:-}" ]]; then
      # shellcheck disable=SC2206
      local extra_opts=( ${SSH_OPTS} )
      ssh_cmd+=("${extra_opts[@]}")
    fi
    "${ssh_cmd[@]}" "${NODE1_USER}@${NODE1_HOST}" \
      "docker exec -e PGPASSWORD='${DB_PASSWORD}' prototype-postgres psql -U '${DB_USER}' -d '${DB_NAME}' -c '${sql}'"
    return
  fi

  echo "psql was not found, local Docker postgres was not found, and SSH to node1 is unavailable." >&2
  echo "Install psql locally, run this script on node1, or ensure SSH access to node1 is configured." >&2
  exit 1
}

reset_s3() {
  ensure_exchange_client
  export AWS_DEFAULT_REGION="${AWS_REGION:-us-east-1}"
  local buckets=()
  local staged_buckets=()
  local released_buckets=()
  csv_to_array "${PRIVATE_DOMAIN_STAGED_BUCKETS:-$S3_BUCKET_STAGED}" staged_buckets
  csv_to_array "${PRIVATE_DOMAIN_RELEASED_BUCKETS:-$S3_BUCKET_RELEASED}" released_buckets
  buckets+=("${staged_buckets[@]}")
  buckets+=("${released_buckets[@]}")

  if command -v aws >/dev/null 2>&1; then
    local bucket
    for bucket in "${buckets[@]}"; do
      if [[ -n "$bucket" ]]; then
        echo "[reset] deleting objects from s3://$bucket"
        if is_aws_s3_backend; then
          aws s3 rm "s3://$bucket" --recursive || true
        else
          AWS_ACCESS_KEY_ID="${MINIO_ACCESS_KEY_ID:-$MINIO_ROOT_USER}" \
          AWS_SECRET_ACCESS_KEY="${MINIO_SECRET_ACCESS_KEY:-$MINIO_ROOT_PASSWORD}" \
            env -u AWS_SESSION_TOKEN -u AWS_SECURITY_TOKEN aws --endpoint-url="$MINIO_ENDPOINT" s3 rm "s3://$bucket" --recursive || true
        fi
      fi
    done
    return
  fi

  echo "[reset] aws CLI not found; deleting S3 objects via boto3"
  S3_BUCKETS="$(IFS=','; echo "${buckets[*]}")" \
  OBJECT_STORE_BACKEND="${OBJECT_STORE_BACKEND:-minio}" \
  MINIO_ENDPOINT="${MINIO_ENDPOINT:-}" \
  MINIO_ROOT_USER="${MINIO_ROOT_USER:-}" \
  MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-}" \
    "$(python_bin)" <<'PY'
import os

import boto3

backend = os.environ.get("OBJECT_STORE_BACKEND", "minio").lower()
is_aws_s3 = backend in {"aws_s3", "s3", "aws"}
kwargs = {"region_name": os.environ.get("AWS_DEFAULT_REGION", "us-east-1")}
if not is_aws_s3:
    kwargs.update(
        {
            "endpoint_url": os.environ["MINIO_ENDPOINT"],
            "aws_access_key_id": os.environ.get("MINIO_ACCESS_KEY_ID") or os.environ["MINIO_ROOT_USER"],
            "aws_secret_access_key": os.environ.get("MINIO_SECRET_ACCESS_KEY") or os.environ["MINIO_ROOT_PASSWORD"],
        }
    )
s3 = boto3.client("s3", **kwargs)

for bucket in os.environ["S3_BUCKETS"].split(","):
    print(f"[reset] deleting objects from s3://{bucket}", flush=True)
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket):
        objects = [{"Key": item["Key"]} for item in page.get("Contents", [])]
        if objects:
            s3.delete_objects(Bucket=bucket, Delete={"Objects": objects})
PY
}

reset_queue() {
  ensure_exchange_client
  export AWS_DEFAULT_REGION="${AWS_REGION:-us-east-1}"
  local queue_names=()
  csv_to_array "${PRIVATE_DOMAIN_QUEUES:-$SQS_QUEUE_NAME}" queue_names

  if command -v aws >/dev/null 2>&1; then
    local queue_name
    for queue_name in "${queue_names[@]}"; do
      if [[ -z "$queue_name" ]]; then
        continue
      fi
      echo "[reset] purging queue $queue_name"
      local queue_url
      if is_aws_queue_backend; then
        queue_url="$(aws sqs get-queue-url \
          --queue-name "$queue_name" \
          --region "${AWS_REGION:-us-east-1}" \
          --query QueueUrl \
          --output text)"
        aws sqs purge-queue --queue-url "$queue_url" --region "${AWS_REGION:-us-east-1}" || {
          echo "[reset] queue purge failed for $queue_name. AWS SQS rejects repeated purge calls within a short interval." >&2
          return 1
        }
      else
        queue_url="$(AWS_ACCESS_KEY_ID=x AWS_SECRET_ACCESS_KEY=x env -u AWS_SESSION_TOKEN -u AWS_SECURITY_TOKEN aws --endpoint-url="$ELASTICMQ_ENDPOINT" sqs get-queue-url \
          --queue-name "$queue_name" \
          --query QueueUrl \
          --output text)"
        AWS_ACCESS_KEY_ID=x AWS_SECRET_ACCESS_KEY=x env -u AWS_SESSION_TOKEN -u AWS_SECURITY_TOKEN aws --endpoint-url="$ELASTICMQ_ENDPOINT" sqs purge-queue --queue-url "$queue_url" || {
        echo "[reset] queue purge failed for $queue_name. ElasticMQ may reject repeated purge calls within a short interval." >&2
        return 1
        }
      fi
    done
    return
  fi

  echo "[reset] aws CLI not found; purging queue via boto3"
  SQS_QUEUE_NAMES="$(IFS=','; echo "${queue_names[*]}")" \
  QUEUE_BACKEND="${QUEUE_BACKEND:-elasticmq}" \
  ELASTICMQ_ENDPOINT="${ELASTICMQ_ENDPOINT:-}" \
    "$(python_bin)" <<'PY'
import os
import sys

import boto3
from botocore.exceptions import ClientError

backend = os.environ.get("QUEUE_BACKEND", "elasticmq").lower()
is_aws_sqs = backend in {"aws_sqs", "sqs", "aws"}
kwargs = {"region_name": os.environ.get("AWS_DEFAULT_REGION", "us-east-1")}
if not is_aws_sqs:
    kwargs.update(
        {
            "endpoint_url": os.environ["ELASTICMQ_ENDPOINT"],
            "aws_access_key_id": "x",
            "aws_secret_access_key": "x",
        }
    )
sqs = boto3.client("sqs", **kwargs)

for queue_name in [name for name in os.environ["SQS_QUEUE_NAMES"].split(",") if name]:
    print(f"[reset] purging queue {queue_name}", flush=True)
    queue_url = sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]
    try:
        sqs.purge_queue(QueueUrl=queue_url)
    except ClientError as exc:
        print(
            f"[reset] queue purge failed for {queue_name}. The queue backend may reject repeated purge calls within a short interval.",
            file=sys.stderr,
        )
        raise exc
PY
}

if [[ "$RESET_DB" == true ]]; then
  reset_db
else
  echo "[reset] skipping database metadata"
fi

if [[ "$RESET_S3" == true ]]; then
  reset_s3
else
  echo "[reset] skipping object storage"
fi

if [[ "$RESET_QUEUE" == true ]]; then
  reset_queue
else
  echo "[reset] skipping request queue"
fi

echo "[reset] benchmark state reset complete"
