#!/usr/bin/env bash
set -euo pipefail

# Initialize Postgres objects for Day 11 and load generated CSVs

ROOT_DIR="$(cd "$(dirname "$0")"/.. && pwd)"
RAW_DIR="$ROOT_DIR/data/raw"
SQL_DIR="$ROOT_DIR/sql"

DB_NAME="${DB_NAME:-example}"
PG_CONTAINER="postgres"

echo "[Day 11] Generating sales dataset (8 dimensions + fact)..."
python3 "$ROOT_DIR/scripts/generate_data.py"

echo "[Day 11] Verifying CSV outputs..."
for f in \
  "dim_date.csv" "dim_product.csv" "dim_customer.csv" "dim_store.csv" \
  "dim_channel.csv" "dim_salesperson.csv" "dim_promotion.csv" "fact_sales.csv"
do
  if [ ! -f "$RAW_DIR/$f" ]; then
    echo "Missing $RAW_DIR/$f" >&2
    exit 1
  fi
done

echo "[Day 11] Waiting for Postgres ($PG_CONTAINER) to be ready..."
until docker exec "$PG_CONTAINER" pg_isready -U dbz -d "$DB_NAME" >/dev/null 2>&1; do
  sleep 1
  printf "."
done
echo " ready."

echo "[Day 11] Running schema and load SQL files..."
SQL_DIR_CONTAINER="/opt/workspace/lectures/day_11/sql"
for base in 00_create_schemas.sql 01_create_tables.sql 02_load_data.sql; do
  echo "Executing $base"
  docker exec -i "$PG_CONTAINER" psql -U dbz -d "$DB_NAME" -v ON_ERROR_STOP=1 -f "$SQL_DIR_CONTAINER/$base"
done

echo "[Day 11] Init complete."


