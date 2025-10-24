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

if [[ -z "${SA_PASSWORD:-}" || -z "${DB_NAME:-}" ]]; then
  echo "SA_PASSWORD or DB_NAME is not set (load .env)" >&2
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

docker exec -e SA_PASSWORD="$SA_PASSWORD" -e DB_NAME="$DB_NAME" -e SQL_DIR="$SQL_DIR" mssql /bin/bash -lc '
  set -e
  if [ ! -d "$SQL_DIR" ]; then echo "SQL_DIR not found: $SQL_DIR" >&2; exit 1; fi
  if ! compgen -G "$SQL_DIR/*.sql" > /dev/null; then echo "No SQL files found at $SQL_DIR" >&2; exit 1; fi
  if [ -x /opt/mssql-tools18/bin/sqlcmd ]; then SQLCMD_BIN=/opt/mssql-tools18/bin/sqlcmd; SQLCMD_FLAGS="-C";
  elif [ -x /opt/mssql-tools/bin/sqlcmd ]; then SQLCMD_BIN=/opt/mssql-tools/bin/sqlcmd; SQLCMD_FLAGS="";
  else echo "sqlcmd not found" >&2; exit 1; fi
  for f in "$SQL_DIR"/*.sql; do echo "Executing $f"; $SQLCMD_BIN $SQLCMD_FLAGS -S localhost -U sa -P "$SA_PASSWORD" -d "$DB_NAME" -b -i "$f"; done
'

echo "Day $DAY scripts executed on database $DB_NAME."


