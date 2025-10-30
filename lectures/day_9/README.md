# Day 9 – ETL, ELT, CDC with Airflow + Postgres

This module covers ETL vs ELT, Source System Analysis (SSA), and Change Data Capture (CDC) using Postgres, Debezium (Kafka Connect), and Airflow. You will generate sample CSVs, load them into a Postgres `source` schema, then run pipelines that land data into staging (ELT) or transform in Python (ETL), upsert into the data warehouse (DW), and demonstrate CDC by streaming changes from `source.*` to Kafka topics and logging them back into Postgres.

## Learning objectives

- Understand differences and trade-offs between ETL and ELT
- Practice SSA by profiling inbound files and maintaining a data inventory
- Implement ELT (land to staging → transform in SQL) and ETL (transform in Python → load DW)
- Enable and consume Debezium CDC to synchronize incremental changes

## What we will do

1. Generate inbound CSVs (`customers.csv`, `orders.csv`)
2. Initialize schemas and tables; bulk load CSVs into `source.*`
3. Optionally enable CDC on `source.*` tables
4. Run Airflow pipelines:
   - SSA profiling (log file structure and counts)
   - ELT pipeline (staging → SQL transforms → DW)
   - ETL pipeline (Python transforms → DW)
   - CDC sync (apply incremental changes to DW)
5. Perform a simple universe-to-universe compare (row counts)

## How to run

### 1) Prerequisites

- Docker, Docker Compose, and `make`
- `.env` file (create defaults):

```sh
make setup
```

Key variables: `SA_PASSWORD`, `DB_NAME`, `SQLPAD_ADMIN`, `SQLPAD_ADMIN_PASSWORD`, `AIRFLOW_WEBSERVER_PORT` (default 80).

### 2) Start environment and load source data

This brings up Postgres, Kafka, Kafka Connect (Debezium), Airflow, SQLPad for Day 9 and executes SQL scripts under `lectures/day_9/sql` inside the container.

```sh
make day-run DAY=9
```

What it does in order:

- `00_init_schemas.sql` – ensure schemas
- `01_create_tables.sql` – create tables (idempotent). On reruns, it clears data. If CDC is enabled for a table, it uses `DELETE` (TRUNCATE is not allowed for CDC-enabled tables).
- `02_bulk_load_source.sql` – bulk insert CSVs into `source.*` from `/opt/workspace/lectures/day_9/data/raw`
- `03_enable_cdc.sql` – placeholder; CDC is configured by Debezium in Kafka Connect (via `scripts/init.sh`)
- `04_elt_transforms.sql` – ELT merge from `staging.*` into `dw.*`. This script expects `batch_id`; it will skip if no staging batch is present (expected when running `make day-run DAY=9` since day-run doesn’t load staging).

Notes:

- Seeing `No staging batch_id available; skipping transforms.` is expected with `make day-run DAY=9`.
- Use the ELT DAG to create `staging.*` data and a `batch_id` (see below).

### 3) Open UIs

- SQLPad: http://localhost:3000 (admin uses `SQLPAD_ADMIN` / `SQLPAD_ADMIN_PASSWORD`)
- Airflow UI: http://localhost:${AIRFLOW_WEBSERVER_PORT} (default http://localhost:80)
- Kafka Connect API: http://localhost:8083

### 4) Run Airflow pipelines

All DAGs are available under `/opt/airflow/dags` (mounted from `lectures/day_9/dags`). Trigger from Airflow UI.

- SSA profiling: `ssa_profile_dag`

  - Generates CSVs (if missing) and logs dataset inventory to `metadata.data_inventory`.

- ELT pipeline: `elt_pipeline_dag`

  - Steps: `begin_checks` → `load_staging` (customers, orders) → `run_sql_transforms` → `end_checks`
  - Creates a `batch_id`, lands CSVs in `staging.*`, then runs `04_elt_transforms.sql` to upsert into `dw.*`.

- ETL pipeline: `etl_pipeline_dag`

  - Transforms CSVs with pandas (type, cleanse, conform) and loads directly to `dw.*` via per-row MERGE.

- CDC demo: `cdc_sync_dag`
  - Steps: `ensure_cdc_enabled` (no-op), `apply_customers_changes` and `apply_orders_changes` (each generates and inserts one random row into `source.customers`/`source.orders`), short `wait_for_stream`, then `consume_cdc_and_log` (reads Debezium topics and writes raw events to `metadata.cdc_events`).
  - Topics are `${DB_NAME}.source.customers` and `${DB_NAME}.source.orders`.
  - Inspect recent events:
    ```sql
    SELECT topic, op, to_timestamp(ts_ms/1000.0) AS source_ts, payload
    FROM metadata.cdc_events
    ORDER BY id DESC
    LIMIT 20;
    ```

### 5) Tear down or reset

```sh
make down DAY=9      # stop containers
make clean DAY=9     # stop and remove volumes for a clean slate
```

## Files and structure

- `data/raw/`: generated CSVs mounted to container at `/opt/workspace/lectures/day_9/data/raw`
- `docker/`
  - `docker-compose.yml`: Postgres, Kafka, Kafka Connect, Airflow, SQLPad stack for this day
- `dags/`
  - `ssa_profile_dag.py`, `elt_pipeline_dag.py`, `etl_pipeline_dag.py`, `cdc_sync_dag.py`
- `scripts/`
  - `generate_sample_data.py`: creates `customers.csv` and `orders.csv`
  - `airflow_entrypoint.sh`: starts Airflow
  - `init.sh`: helper invoked by the repo-level `scripts/day-run.sh`
- `sql/`
  - `00_init_schemas.sql`
  - `01_create_tables.sql` (idempotent clears; CDC-aware)
  - `02_bulk_load_source.sql`
  - `03_enable_cdc.sql`
  - `04_elt_transforms.sql` (requires `batch_id`)
  - `05_universe_compare.sql`

## Troubleshooting

- Debezium connector status:
  - `curl -s http://localhost:8083/connectors | jq`
  - `curl -s http://localhost:8083/connectors/pg-source/status | jq`
- `No staging batch_id available; skipping transforms.`: expected if you haven’t run the ELT DAG; run `elt_pipeline_dag` to create staging data and `batch_id`.
- Airflow Postgres connection (`postgres_default`) points to `dbz/dbz@postgres:5432/${DB_NAME}`. Ensure `DB_NAME` is set in `.env` and passed to the Airflow container.
- Rebuild Airflow if you changed `lectures/day_9/requirements.txt` (Kafka client):
  ```sh
  docker compose -f lectures/day_9/docker/docker-compose.yml --env-file .env build airflow
  docker compose -f lectures/day_9/docker/docker-compose.yml --env-file .env up -d airflow
  ```

## References

- Lecture 9 notes in `lecture_notes.md`
- Debezium Postgres connector documentation
- Airflow PostgresHook provider
