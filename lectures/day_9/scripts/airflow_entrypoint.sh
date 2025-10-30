#!/bin/bash
airflow db init
echo "AUTH_ROLE_PUBLIC = 'Admin'" >> webserver_config.py

# Ensure Airflow connection to Postgres matches docker-compose credentials
# Defaults DB_NAME to 'example' if not provided by compose env
DB_NAME=${DB_NAME:-example}
echo "configuring Airflow connection: postgres_default -> postgres:5432/${DB_NAME} (dbz)"
# Remove if exists to avoid duplicates
airflow connections delete postgres_default >/dev/null 2>&1 || true
airflow connections add postgres_default \
  --conn-uri "postgresql+psycopg2://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres:5432/${POSTGRES_DB}"

# Start the scheduler
echo "starting scheduler on background"
sh -c "airflow scheduler" &
status=$?
if [ $status -ne 0 ]; then
    echo "Failed to start scheduler: $status"
    exit $status
fi

# Start the webserver
echo "starting webserver"
sh -c "airflow webserver"
status=$?
if [ $status -ne 0 ]; then
    echo "Failed to start webserver: $status"
    exit $status
fi

while sleep 60; do
    ps aux |grep scheduler |grep -q -v grep
    PROCESS_1_STATUS=$?
    ps aux |grep webserver |grep -q -v grep
    PROCESS_2_STATUS=$?
    if [ $PROCESS_1_STATUS -ne 0 -o $PROCESS_2_STATUS -ne 0 ]; then
        echo "One of the processes has already exited."
        exit 1
    fi
done