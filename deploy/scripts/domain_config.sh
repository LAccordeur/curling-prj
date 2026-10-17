#!/usr/bin/env bash

domain_ids() {
  if [[ -n "${PRIVATE_DOMAINS:-}" ]]; then
    echo "$PRIVATE_DOMAINS"
  else
    echo "default"
  fi
}

domain_env_key() {
  local domain_id="$1"
  local suffix="$2"
  local normalized
  normalized="$(echo "$domain_id" | tr '[:lower:]' '[:upper:]' | sed 's/[^A-Z0-9]/_/g')"
  echo "DOMAIN_${normalized}_${suffix}"
}

domain_env_value() {
  local domain_id="$1"
  local suffix="$2"
  local fallback="$3"
  local key
  key="$(domain_env_key "$domain_id" "$suffix")"
  echo "${!key:-$fallback}"
}

domain_default_suffix() {
  local domain_id="$1"
  if [[ "$domain_id" == "default" ]]; then
    echo ""
  else
    echo "-$domain_id"
  fi
}

domain_private_host() {
  domain_env_value "$1" "PRIVATE_HOST" "${NODE3_HOST}"
}

domain_queue_host() {
  domain_env_value "$1" "QUEUE_HOST" "${QUEUE_HOST}"
}

domain_object_host() {
  domain_env_value "$1" "OBJECT_HOST" "${MINIO_HOST}"
}

domain_queue_name() {
  local suffix
  suffix="$(domain_default_suffix "$1")"
  domain_env_value "$1" "QUEUE_NAME" "${SQS_QUEUE_NAME}${suffix}"
}

is_aws_queue_backend() {
  local backend="${QUEUE_BACKEND:-elasticmq}"
  [[ "$backend" == "aws_sqs" || "$backend" == "sqs" || "$backend" == "aws" ]]
}

domain_staged_bucket() {
  local suffix
  suffix="$(domain_default_suffix "$1")"
  domain_env_value "$1" "STAGED_BUCKET" "${S3_BUCKET_STAGED}${suffix}"
}

domain_released_bucket() {
  local suffix
  suffix="$(domain_default_suffix "$1")"
  domain_env_value "$1" "RELEASED_BUCKET" "${S3_BUCKET_RELEASED}${suffix}"
}

domain_private_base() {
  local suffix
  suffix="$(domain_default_suffix "$1")"
  domain_env_value "$1" "PRIVATE_BASE" "${DEPLOY_BASE}/private-data${suffix}"
}

domain_work_base() {
  local suffix
  suffix="$(domain_default_suffix "$1")"
  domain_env_value "$1" "WORK_BASE" "${DEPLOY_BASE}/work${suffix}"
}

domain_rpc_port() {
  domain_env_value "$1" "EXECUTOR_RPC_PORT" "${EXECUTOR_RPC_PORT:-9008}"
}

domain_storage_api_port() {
  domain_env_value "$1" "PRIVATE_STORAGE_API_PORT" "${PRIVATE_STORAGE_API_PORT:-9010}"
}

domain_queue_url() {
  local domain_id="$1"
  local explicit_url
  explicit_url="$(domain_env_value "$domain_id" "QUEUE_URL" "")"
  if [[ -n "$explicit_url" ]]; then
    echo "$explicit_url"
    return
  fi
  if is_aws_queue_backend; then
    if [[ -z "${AWS_ACCOUNT_ID:-}" ]]; then
      echo "AWS_ACCOUNT_ID is required for QUEUE_BACKEND=aws_sqs unless DOMAIN_*_QUEUE_URL is set." >&2
      return 1
    fi
    echo "https://sqs.${AWS_REGION:-us-east-1}.amazonaws.com/${AWS_ACCOUNT_ID}/$(domain_queue_name "$domain_id")"
    return
  fi
  echo "http://$(domain_queue_host "$domain_id"):${ELASTICMQ_PORT}/queue/$(domain_queue_name "$domain_id")"
}

domain_rpc_url() {
  local domain_id="$1"
  echo "http://$(domain_private_host "$domain_id"):$(domain_rpc_port "$domain_id")"
}

domain_storage_api_url() {
  local domain_id="$1"
  echo "http://$(domain_private_host "$domain_id"):$(domain_storage_api_port "$domain_id")"
}

json_escape() {
  local value="$1"
  value="${value//\\/\\\\}"
  value="${value//\"/\\\"}"
  printf '%s' "$value"
}

private_domains_json() {
  local first=1
  local domain_id
  printf '{'
  for domain_id in $(domain_ids); do
    if [[ "$first" -eq 0 ]]; then
      printf ','
    fi
    first=0
    printf '"%s":{' "$(json_escape "$domain_id")"
    printf '"queue_url":"%s",' "$(json_escape "$(domain_queue_url "$domain_id")")"
    printf '"rpc_url":"%s",' "$(json_escape "$(domain_rpc_url "$domain_id")")"
    printf '"storage_api_url":"%s",' "$(json_escape "$(domain_storage_api_url "$domain_id")")"
    printf '"staged_bucket":"%s",' "$(json_escape "$(domain_staged_bucket "$domain_id")")"
    printf '"released_bucket":"%s"' "$(json_escape "$(domain_released_bucket "$domain_id")")"
    printf '}'
  done
  printf '}'
}

csv_join_domain_values() {
  local function_name="$1"
  local first=1
  local domain_id
  local value
  for domain_id in $(domain_ids); do
    if [[ "$first" -eq 0 ]]; then
      printf ','
    fi
    first=0
    value="$("$function_name" "$domain_id")"
    printf '%s' "$value"
  done
}
