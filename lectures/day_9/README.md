# Day 9 – ETL, ELT, CDC with Airflow + SQL Server

This module covers ETL vs ELT, Source System Analysis (SSA), and Change Data Capture (CDC) using SQL Server and Airflow. You will generate sample CSVs, load them into a SQL Server "source" schema, then run pipelines that land data into staging (ELT) or transform in Python (ETL), upsert into the data warehouse (DW), and optionally synchronize changes using CDC.

## Learning objectives

- Understand differences and trade-offs between ETL and ELT
- Practice SSA by profiling inbound files and maintaining a data inventory
- Implement ELT (land to staging → transform in SQL) and ETL (transform in Python → load DW)
- Enable and consume SQL Server CDC to synchronize incremental changes

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

This brings up SQL Server, Airflow, SQLPad for Day 9 and executes SQL scripts under `lectures/day_9/sql` inside the container.

```sh
make day-run DAY=9
```

What it does in order:

- `00_init_schemas.sql` – ensure schemas
- `01_create_tables.sql` – create tables (idempotent). On reruns, it clears data. If CDC is enabled for a table, it uses `DELETE` (TRUNCATE is not allowed for CDC-enabled tables).
- `02_bulk_load_source.sql` – bulk insert CSVs into `source.*` from `/opt/workspace/lectures/day_9/data/raw`
- `03_enable_cdc.sql` – enable CDC at DB level and for `source.*` tables if not already
- `04_elt_transforms.sql` – ELT merge from `staging.*` into `dw.*`. This script expects `batch_id`; it will skip if no staging batch is present (expected when running `make day-run DAY=9` since day-run doesn’t load staging).

Notes:

- Seeing `No staging batch_id available; skipping transforms.` is expected with `make day-run DAY=9`.
- Use the ELT DAG to create `staging.*` data and a `batch_id` (see below).

### 3) Open UIs

- SQLPad: http://localhost:3000 (admin uses `SQLPAD_ADMIN` / `SQLPAD_ADMIN_PASSWORD`)
- Airflow UI: http://localhost:${AIRFLOW_WEBSERVER_PORT} (default http://localhost:80)

### 4) Run Airflow pipelines

All DAGs are available under `/opt/airflow/dags` (mounted from `lectures/day_9/dags`). Trigger from Airflow UI.

- SSA profiling: `ssa_profile_dag`

  - Generates CSVs (if missing) and logs dataset inventory to `metadata.data_inventory`.

- ELT pipeline: `elt_pipeline_dag`

  - Steps: `begin_checks` → `load_staging` (customers, orders) → `run_sql_transforms` → `end_checks`
  - Creates a `batch_id`, lands CSVs in `staging.*`, then runs `04_elt_transforms.sql` to upsert into `dw.*`.

- ETL pipeline: `etl_pipeline_dag`

  - Transforms CSVs with pandas (type, cleanse, conform) and loads directly to `dw.*` via per-row MERGE.

- CDC sync: `cdc_sync_dag`
  - Ensures CDC is enabled, then applies changes from CDC functions to `dw.*` (upserts; soft-delete by setting `is_active = 0`). CDC state is tracked in `metadata.cdc_state` with LSN windows.

### 5) Tear down or reset

```sh
make down DAY=9      # stop containers
make clean DAY=9     # stop and remove volumes for a clean slate
```

## Files and structure

- `data/raw/`: generated CSVs mounted to container at `/opt/workspace/lectures/day_9/data/raw`
- `docker/`
  - `docker-compose.yml`: Airflow, SQL Server, SQLPad stack for this day
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

- TRUNCATE with CDC error (Msg 4711): handled in `01_create_tables.sql` by using `DELETE` when CDC is enabled for the table.
- `No staging batch_id available; skipping transforms.`: expected if you haven’t run the ELT DAG; run `elt_pipeline_dag` to create staging data and `batch_id`.
- `sqlcmd not found`: scripts try both `/opt/mssql-tools18/bin/sqlcmd` and `/opt/mssql-tools/bin/sqlcmd` in the container.

## References

- Lecture 9 notes in `lecture_notes.md`
- SQL Server CDC documentation
- Airflow MsSqlHook provider
