#!/usr/bin/env bash
set -euo pipefail

if [ $# -ne 1 ]; then
  echo "Usage: bash bootstrap_node.sh <control|exchange|object-storage|queue|private|client>"
  exit 1
fi

ROLE="$1"
ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"

sudo apt-get update
sudo apt-get install -y \
  python3 python3-venv python3-pip \
  curl wget jq rsync ca-certificates gnupg lsb-release \
  postgresql-client

if ! command -v docker >/dev/null 2>&1; then
  curl -fsSL https://get.docker.com | sh
  sudo usermod -aG docker "$USER" || true
fi

if ! docker compose version >/dev/null 2>&1; then
  sudo apt-get install -y docker-compose-plugin || true
fi


bash "$ROOT_DIR/deploy/scripts/bootstrap_python.sh"

case "$ROLE" in
  control)
    [ -f "$ROOT_DIR/deploy/control-plane/.env" ] || bash "$ROOT_DIR/create_control_env.sh"
    (cd "$ROOT_DIR/deploy/control-plane" && docker compose up -d)
    ;;
  exchange)
    [ -f "$ROOT_DIR/deploy/exchange/.env" ] || bash "$ROOT_DIR/create_exchange_env.sh"
    (cd "$ROOT_DIR/deploy/exchange" && docker compose up -d)
    ;;
  object-storage)
    [ -f "$ROOT_DIR/deploy/exchange/.env" ] || bash "$ROOT_DIR/create_exchange_env.sh"
    (cd "$ROOT_DIR/deploy/exchange" && docker compose up -d minio)
    ;;
  queue)
    [ -f "$ROOT_DIR/deploy/exchange/.env" ] || bash "$ROOT_DIR/create_exchange_env.sh"
    (cd "$ROOT_DIR/deploy/exchange" && docker compose up -d elasticmq)
    ;;
  private)
    [ -f "$ROOT_DIR/deploy/private/.env" ] || bash "$ROOT_DIR/create_private_env.sh"
    mkdir -p "$ROOT_DIR/work/inbound" "$ROOT_DIR/work/outbound" "$ROOT_DIR/private-data" "$ROOT_DIR/private-data/exports" "$ROOT_DIR/private-data/annotations"
    ;;
  client)
    [ -f "$ROOT_DIR/deploy/client/.env" ] || bash "$ROOT_DIR/create_client_env.sh"
    ;;
  *)
    echo "Unknown role: $ROLE"
    exit 1
    ;;
esac

echo "Bootstrap completed for role=$ROLE"
