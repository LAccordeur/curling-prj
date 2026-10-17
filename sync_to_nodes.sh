#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
set -a
source "$ROOT_DIR/inventory.env"
set +a

SSH_BASE=(ssh)
if [ -n "${SSH_KEY:-}" ]; then
  SSH_BASE+=( -i "$SSH_KEY" )
fi
if [ -n "${SSH_OPTS:-}" ]; then
  # shellcheck disable=SC2206
  EXTRA_OPTS=( ${SSH_OPTS} )
  SSH_BASE+=( "${EXTRA_OPTS[@]}" )
fi

sync_one() {
  local host="$1"
  local user="$2"
  echo "==> Syncing to ${user}@${host}:${DEPLOY_BASE}"
  rsync -az --delete -e "${SSH_BASE[*]}" \
    --exclude '.git/' \
    --exclude '.venv/' \
    --exclude 'postgres/data/' \
    --exclude 'minio/data/' \
    --exclude 'logs/' \
    "$ROOT_DIR/" "${user}@${host}:${DEPLOY_BASE}/"
}

sync_one "$NODE1_HOST" "$NODE1_USER"
sync_one "$NODE2_HOST" "$NODE2_USER"
sync_one "$NODE3_HOST" "$NODE3_USER"
#sync_one "$NODE7_HOST" "$NODE7_USER"
#sync_one "$NODE4_HOST" "$NODE4_USER"
