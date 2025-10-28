#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

echo "[Day 8] Generating sample data..."
python3 "$SCRIPT_DIR/generate_data.py" --n 1000

echo "[Day 8] Waiting for mssql to be healthy..."
until [ "$(${SHELL:-/bin/bash} -lc "/usr/bin/env docker inspect -f '{{.State.Health.Status}}' mssql 2>/dev/null")" = "healthy" ]; do
  sleep 2; printf "."
done
echo " mssql is healthy."

SQL_DIR="/opt/workspace/lectures/day_8/sql"
echo "[Day 8] Executing SQL files inside MSSQL container..."
docker exec -e SA_PASSWORD="${SA_PASSWORD}" -e DB_NAME="${DB_NAME}" -e SQL_DIR="$SQL_DIR" mssql /bin/bash -lc '
  set -e
  if [ -x /opt/mssql-tools18/bin/sqlcmd ]; then SQLCMD_BIN=/opt/mssql-tools18/bin/sqlcmd; SQLCMD_FLAGS="-C";
  elif [ -x /opt/mssql-tools/bin/sqlcmd ]; then SQLCMD_BIN=/opt/mssql-tools/bin/sqlcmd; SQLCMD_FLAGS="";
  else echo "sqlcmd not found" >&2; exit 1; fi
  for f in "$SQL_DIR"/*.sql; do echo "Executing $f"; $SQLCMD_BIN $SQLCMD_FLAGS -S localhost -U sa -P "$SA_PASSWORD" -d "$DB_NAME" -b -i "$f"; done
'

echo "[Day 8] Init complete."