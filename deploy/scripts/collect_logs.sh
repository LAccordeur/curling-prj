#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
mkdir -p "$ROOT_DIR/logs"
docker ps > "$ROOT_DIR/logs/docker-ps.txt" 2>/dev/null || true
docker logs prototype-postgres > "$ROOT_DIR/logs/postgres.log" 2>/dev/null || true
docker logs minio > "$ROOT_DIR/logs/minio.log" 2>/dev/null || true
docker logs elasticmq > "$ROOT_DIR/logs/elasticmq.log" 2>/dev/null || true
echo "Logs collected under $ROOT_DIR/logs"
