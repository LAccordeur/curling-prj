#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
set -a
source "$ROOT_DIR/inventory.env"
set +a
cat > "$ROOT_DIR/deploy/client/.env" <<EOF3
CONTROL_API=http://${NODE1_HOST}:${API_PORT}
EOF3
echo "Generated deploy/client/.env"
