from __future__ import annotations

import os
from datetime import datetime

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.utils.task_group import TaskGroup

RAW_DIR = "/opt/workspace/lectures/day_9/data/raw"
GEN_SCRIPT = "/opt/workspace/lectures/day_9/scripts/generate_sample_data.py"


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}


def generate_files() -> None:
    # Run generator script in-process to ensure files exist
    import runpy

    runpy.run_path(GEN_SCRIPT, run_name="__main__")


def profile_csv(
    dataset_name: str, filename: str, expected_columns: list[str] | None = None
) -> None:
    path = os.path.join(RAW_DIR, filename)
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    df = pd.read_csv(path)
    actual_columns = list(df.columns)
    row_count = int(len(df))
    hook = MsSqlHook(mssql_conn_id="mssql_default")
    hook.run(
        sql=(
            "INSERT INTO metadata.data_inventory(dataset_name, file_path, expected_columns, actual_columns, row_count) "
            "VALUES (%(dataset)s, %(file)s, %(expected)s, %(actual)s, %(rows)s)"
        ),
        parameters={
            "dataset": dataset_name,
            "file": path,
            "expected": ",".join(expected_columns or []),
            "actual": ",".join(actual_columns),
            "rows": row_count,
        },
    )


with DAG(
    dag_id="ssa_profile_dag",
    description="Profile inbound CSVs and log to metadata.data_inventory",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    prepare = PythonOperator(task_id="generate_files", python_callable=generate_files)

    with TaskGroup(group_id="profile"):
        profile_customers = PythonOperator(
            task_id="profile_customers",
            python_callable=profile_csv,
            op_kwargs={
                "dataset_name": "customers",
                "filename": "customers.csv",
                "expected_columns": [
                    "customer_id",
                    "name",
                    "city",
                    "updated_at",
                ],
            },
        )

        profile_orders = PythonOperator(
            task_id="profile_orders",
            python_callable=profile_csv,
            op_kwargs={
                "dataset_name": "orders",
                "filename": "orders.csv",
                "expected_columns": [
                    "order_id",
                    "customer_id",
                    "amount",
                    "status",
                    "updated_at",
                ],
            },
        )

    prepare >> [profile_customers, profile_orders]
