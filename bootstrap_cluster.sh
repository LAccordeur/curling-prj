#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
set -a
source "$ROOT_DIR/inventory.env"
set +a

INIT_SAMPLE_DATA=0
SMOKE_TEST=0

while [ $# -gt 0 ]; do
  case "$1" in
    --init-sample-data) INIT_SAMPLE_DATA=1 ;;
    --smoke-test) SMOKE_TEST=1 ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
  shift
done

SSH_BASE=(ssh)
if [ -n "${SSH_KEY:-}" ]; then
  SSH_BASE+=( -i "$SSH_KEY" )
fi
if [ -n "${SSH_OPTS:-}" ]; then
  # shellcheck disable=SC2206
  EXTRA_OPTS=( ${SSH_OPTS} )
  SSH_BASE+=( "${EXTRA_OPTS[@]}" )
fi

remote_run() {
  local user="$1"
  local host="$2"
  shift 2
  echo "==> ${user}@${host}: $*"
  "${SSH_BASE[@]}" "${user}@${host}" "cd ${DEPLOY_BASE} && $*"
}

remote_run "$NODE1_USER" "$NODE1_HOST" "bash bootstrap_node.sh control"
remote_run "${NODE5_USER:-$NODE2_USER}" "${NODE5_HOST:-$NODE2_HOST}" "bash bootstrap_node.sh object-storage"
remote_run "${NODE6_USER:-$NODE2_USER}" "${NODE6_HOST:-$NODE2_HOST}" "bash bootstrap_node.sh queue"
remote_run "$NODE3_USER" "$NODE3_HOST" "bash bootstrap_node.sh private"
#remote_run "$NODE4_USER" "$NODE4_HOST" "bash bootstrap_node.sh client"
bash "$ROOT_DIR/deploy/scripts/bootstrap_python.sh"

remote_run "$NODE1_USER" "$NODE1_HOST" "bash init_db.sh"
remote_run "${NODE5_USER:-$NODE2_USER}" "${NODE5_HOST:-$NODE2_HOST}" "bash deploy/exchange/init_exchange.sh"

if [ "$INIT_SAMPLE_DATA" -eq 1 ]; then
  remote_run "$NODE3_USER" "$NODE3_HOST" "bash deploy/scripts/init_sample_private_data.sh"
fi

if [ "$SMOKE_TEST" -eq 1 ]; then
  remote_run "$NODE4_USER" "$NODE4_HOST" "bash deploy/scripts/smoke_test.sh"
fi

echo "Cluster bootstrap complete."
echo "Next, start long-running services manually or via tmux/systemd:"
echo "  node1: bash deploy/control-plane/start.sh"
echo "  node3: bash deploy/private/start.sh"
