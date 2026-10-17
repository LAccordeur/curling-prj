#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
INV_FILE="$ROOT_DIR/inventory.env"
ENV_FILE="$ROOT_DIR/deploy/control-plane/.env"
SQL_FILE="$ROOT_DIR/deploy/control-plane/init_db.sql"

set -a
source "$INV_FILE"
set +a

if [ ! -f "$ENV_FILE" ]; then
  echo "Missing $ENV_FILE. Run bash create_control_env.sh first."
  exit 1
fi

for i in $(seq 1 30); do
  if PGPASSWORD="$DB_PASSWORD" psql -h "$NODE1_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c 'select 1' >/dev/null 2>&1; then
    break
  fi
  echo "Waiting for PostgreSQL on ${NODE1_HOST}:${DB_PORT}... ($i/30)"
  sleep 2
done

PGPASSWORD="$DB_PASSWORD" psql -h "$NODE1_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -f "$SQL_FILE"
echo "Database initialized."
