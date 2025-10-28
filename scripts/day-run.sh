#!/usr/bin/env bash
set -euo pipefail

DAY="${DAY:-${1:-}}"
if [[ -z "$DAY" ]]; then
  echo "Usage: DAY=8 $0  or  $0 8" >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Load .env if present (export all vars)
if [[ -f "$REPO_ROOT/.env" ]]; then
  set -a
  . "$REPO_ROOT/.env"
  set +a
fi

# DB_NAME is used for both MSSQL/PG flows
if [[ -z "${DB_NAME:-}" ]]; then
  echo "DB_NAME is not set (load .env)" >&2
  exit 1
fi

# Run optional init script for the target day on the host (e.g., generate data)
LECTURE_DIR="$REPO_ROOT/lectures/day_${DAY}"
INIT_SCRIPT="$LECTURE_DIR/scripts/init.sh"
if [[ -f "$INIT_SCRIPT" ]]; then
  echo "Running init script: $INIT_SCRIPT"
  (
    cd "$(dirname "$INIT_SCRIPT")"
    bash "$(basename "$INIT_SCRIPT")"
  )
else
  echo "No init script found at $INIT_SCRIPT; skipping."
fi

SQL_DIR="/opt/workspace/lectures/day_${DAY}/sql"

# Prefer Postgres container if present; otherwise try MSSQL
if docker ps --format '{{.Names}}' | grep -q '^postgres$'; then
  echo "Executing SQL scripts in Postgres..."
  for f in "$REPO_ROOT/lectures/day_${DAY}/sql"/*.sql; do
    [ -e "$f" ] || continue
    echo "Executing $(basename "$f") via psql..."
    docker exec -i postgres psql -U dbz -d "$DB_NAME" -v ON_ERROR_STOP=1 -f "/opt/workspace/lectures/day_${DAY}/sql/$(basename "$f")"
  done
elif docker ps --format '{{.Names}}' | grep -q '^mssql$'; then
  if [[ -z "${SA_PASSWORD:-}" ]]; then
    echo "SA_PASSWORD not set for MSSQL flow" >&2
    exit 1
  fi
  echo "Executing SQL scripts in MSSQL..."
  docker exec -e SA_PASSWORD="$SA_PASSWORD" -e DB_NAME="$DB_NAME" -e SQL_DIR="$SQL_DIR" mssql /bin/bash -lc '
    set -e
    if [ ! -d "$SQL_DIR" ]; then echo "SQL_DIR not found: $SQL_DIR" >&2; exit 1; fi
    if ! compgen -G "$SQL_DIR/*.sql" > /dev/null; then echo "No SQL files found at $SQL_DIR" >&2; exit 1; fi
    if [ -x /opt/mssql-tools18/bin/sqlcmd ]; then SQLCMD_BIN=/opt/mssql-tools18/bin/sqlcmd; SQLCMD_FLAGS="-C";
    elif [ -x /opt/mssql-tools/bin/sqlcmd ]; then SQLCMD_BIN=/opt/mssql-tools/bin/sqlcmd; SQLCMD_FLAGS="";
    else echo "sqlcmd not found" >&2; exit 1; fi
    for f in "$SQL_DIR"/*.sql; do echo "Executing $f"; $SQLCMD_BIN $SQLCMD_FLAGS -S localhost -U sa -P "$SA_PASSWORD" -d "$DB_NAME" -b -i "$f"; done
  '
else
  echo "No supported database container found (postgres or mssql). Skipping SQL execution."
fi

echo "Day $DAY scripts executed on database $DB_NAME (if applicable)."


