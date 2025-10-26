#!/usr/bin/env bash
set -euo pipefail

# This script initializes SQL Server objects for Day 9 and loads CSVs into the source schema.
# Requirements:
# - Docker and docker compose
# - The Day 9 docker-compose stack running (mssql service up)
# - Env vars SA_PASSWORD and DB_NAME provided via docker-compose

ROOT_DIR="$(cd "$(dirname "$0")"/.. && pwd)"
SQL_DIR="$ROOT_DIR/sql"
RAW_DIR="$ROOT_DIR/data/raw"

COMPOSE_FILE="$ROOT_DIR/docker/docker-compose.yml"

if [ ! -f "$COMPOSE_FILE" ]; then
  echo "docker-compose file not found at: $COMPOSE_FILE" >&2
  exit 1
fi

echo "[Day 9] Generating sample data..."
if [ ! -f "$RAW_DIR/customers.csv" ] || [ ! -f "$RAW_DIR/orders.csv" ]; then
  python3 "$ROOT_DIR/scripts/generate_sample_data.py" || true
fi

if [ ! -f "$RAW_DIR/customers.csv" ] || [ ! -f "$RAW_DIR/orders.csv" ]; then
  echo "CSV generation via host python failed or missing. Trying via airflow container..."
  docker compose -f "$COMPOSE_FILE" exec -T airflow python \
    /opt/workspace/lectures/day_9/scripts/generate_sample_data.py || true
fi

if [ ! -f "$RAW_DIR/customers.csv" ] || [ ! -f "$RAW_DIR/orders.csv" ]; then
  echo "Unable to generate CSVs. Aborting." >&2
  exit 1
fi

