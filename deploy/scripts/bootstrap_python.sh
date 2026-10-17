#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT_DIR"

python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip wheel

for req in \
  services/api/requirements.txt \
  services/executor/requirements.txt \
  services/client/requirements.txt; do
  if [ -f "$req" ]; then
    pip install -r "$req"
  fi
done
