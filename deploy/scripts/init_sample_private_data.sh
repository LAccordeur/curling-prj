#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
mkdir -p "$ROOT_DIR/private-data/exports"
cat > "$ROOT_DIR/private-data/exports/studyA.txt" <<EOF3
sample line 1
sample line 2
sample line 3
EOF3
echo "Initialized sample private export data under private-data/exports/studyA.txt"
