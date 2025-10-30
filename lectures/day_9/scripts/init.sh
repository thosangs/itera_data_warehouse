#!/usr/bin/env bash
set -euo pipefail

# This script initializes Postgres objects for Day 9 and loads CSVs into the source schema,
# then registers a Debezium Postgres source connector in Kafka Connect.

ROOT_DIR="$(cd "$(dirname "$0")"/.. && pwd)"
RAW_DIR="$ROOT_DIR/data/raw"

DB_NAME="${DB_NAME:-example}"
PG_CONTAINER="postgres"
CONNECT_URL="http://localhost:8083"

echo "[Day 9] Generating sample data..."
if [ ! -f "$RAW_DIR/customers.csv" ] || [ ! -f "$RAW_DIR/orders.csv" ]; then
  python3 "$ROOT_DIR/scripts/generate_sample_data.py" || true
fi

if [ ! -f "$RAW_DIR/customers.csv" ] || [ ! -f "$RAW_DIR/orders.csv" ]; then
  echo "Unable to generate CSVs. Aborting." >&2
  exit 1
fi

echo "[Day 9] Waiting for Postgres ($PG_CONTAINER) to be ready..."
until docker exec "$PG_CONTAINER" pg_isready -U dbz -d "$DB_NAME" >/dev/null 2>&1; do
  sleep 1
  printf "."
done
echo " ready."

echo "[Day 9] Running SQL files in lectures/day_9/sql on Postgres ($DB_NAME)..."
SQL_DIR_CONTAINER="/opt/workspace/lectures/day_9/sql"
for f in $(ls "$ROOT_DIR/sql"/*.sql | sort); do
  base="$(basename "$f")"
  echo "Executing $base"
  docker exec -i "$PG_CONTAINER" psql -U dbz -d "$DB_NAME" -v ON_ERROR_STOP=1 -f "$SQL_DIR_CONTAINER/$base"
done

echo "[Day 9] Waiting for Kafka Connect to be available..."
until curl -fsS "$CONNECT_URL/connectors" >/dev/null 2>&1; do
  sleep 1
  printf "."
done
echo " ready."

echo "[Day 9] Connector plugins available:"
curl -sS "$CONNECT_URL/connector-plugins" || true

echo "[Day 9] Registering/Updating Debezium Postgres connector 'pg-source'..."

SAFE_DB_NAME=$(echo "$DB_NAME" | tr '[:upper:]' '[:lower:]' | sed -E 's/[^a-z0-9_]+/_/g')
SLOT_BASE="pgslot_${SAFE_DB_NAME}"
PUB_BASE="dbz_pub_${SAFE_DB_NAME}"
# Enforce <=63 chars per Postgres slot/publication limits
SLOT_NAME="${SLOT_BASE:0:63}"
PUB_NAME="${PUB_BASE:0:63}"

CREATE_BODY=$(cat <<JSON
{
  "name": "pg-source",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "tasks.max": "1",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "dbz",
    "database.password": "dbz",
    "database.dbname": "${DB_NAME}",
    "topic.prefix": "${DB_NAME}",
    "plugin.name": "pgoutput",
    "slot.name": "${SLOT_NAME}",
    "publication.name": "${PUB_NAME}",
    "publication.autocreate.mode": "filtered",
    "schema.include.list": "source",
    "table.include.list": "source.customers,source.orders",
    "tombstones.on.delete": "false",
    "decimal.handling.mode": "double",
    "time.precision.mode": "adaptive_time_microseconds"
  }
}
JSON
)

UPDATE_BODY=$(cat <<JSON
{
  "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
  "tasks.max": "1",
  "database.hostname": "postgres",
  "database.port": "5432",
  "database.user": "dbz",
  "database.password": "dbz",
  "database.dbname": "${DB_NAME}",
  "topic.prefix": "${DB_NAME}",
  "plugin.name": "pgoutput",
  "slot.name": "${SLOT_NAME}",
  "publication.name": "${PUB_NAME}",
  "publication.autocreate.mode": "filtered",
  "schema.include.list": "source",
  "table.include.list": "source.customers,source.orders",
  "tombstones.on.delete": "false",
  "decimal.handling.mode": "double",
  "time.precision.mode": "adaptive_time_microseconds"
}
JSON
)

# Try POST (create). If 409 (exists), switch to PUT (update config)
HTTP_CODE=$(echo "$CREATE_BODY" | curl -s -o /tmp/pg_source_resp.txt -w "%{http_code}" -X POST "$CONNECT_URL/connectors" -H 'Content-Type: application/json' -d @- || true)
if [ "$HTTP_CODE" = "409" ]; then
  HTTP_CODE=$(echo "$UPDATE_BODY" | curl -s -o /tmp/pg_source_resp.txt -w "%{http_code}" -X PUT "$CONNECT_URL/connectors/pg-source/config" -H 'Content-Type: application/json' -d @- || true)
fi

if [ "$HTTP_CODE" != "200" ] && [ "$HTTP_CODE" != "201" ]; then
  echo "[Day 9] Connector registration failed (HTTP $HTTP_CODE). Response:" >&2
  cat /tmp/pg_source_resp.txt >&2 || true
  exit 1
fi
echo "[Day 9] Connector up (HTTP $HTTP_CODE)."

echo "[Day 9] Init complete."