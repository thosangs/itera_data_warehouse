from __future__ import annotations

import os
import uuid
from datetime import datetime

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup

RAW_DIR = "/opt/workspace/lectures/day_9/data/raw"


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}


def begin_checks() -> None:
    required = [
        os.path.join(RAW_DIR, "customers.csv"),
        os.path.join(RAW_DIR, "orders.csv"),
    ]
    for path in required:
        if not os.path.exists(path):
            raise FileNotFoundError(path)
        if os.path.getsize(path) == 0:
            raise ValueError(f"Empty file: {path}")


def load_to_staging(batch_id: str, table: str, csv_name: str) -> int:
    path = os.path.join(RAW_DIR, csv_name)
    df = pd.read_csv(path)
    # basic typing
    if table == "customers":
        df["customer_id"] = df["customer_id"].astype(int)
        df["name"] = df["name"].astype(str)
        df["city"] = df["city"].astype(str)
        df["updated_at"] = pd.to_datetime(df["updated_at"], utc=True)
    elif table == "orders":
        df["order_id"] = df["order_id"].astype(int)
        df["customer_id"] = df["customer_id"].astype(int)
        df["amount"] = df["amount"].astype(float)
        df["status"] = df["status"].astype(str)
        df["updated_at"] = pd.to_datetime(df["updated_at"], utc=True)
    else:
        raise ValueError(table)

    df["batch_id"] = batch_id
    hook = PostgresHook(postgres_conn_id="postgres_default")

    if table == "customers":
        rows = [
            (
                int(r.customer_id),
                str(r.name),
                str(r.city),
                pd.Timestamp(r.updated_at).to_pydatetime(),
                batch_id,
            )
            for r in df.itertuples(index=False)
        ]
        hook.insert_rows(
            table="staging.customers",
            rows=rows,
            target_fields=["customer_id", "name", "city", "updated_at", "batch_id"],
            commit_every=1000,
        )
    else:
        rows = [
            (
                int(r.order_id),
                int(r.customer_id),
                float(r.amount),
                str(r.status),
                pd.Timestamp(r.updated_at).to_pydatetime(),
                batch_id,
            )
            for r in df.itertuples(index=False)
        ]
        hook.insert_rows(
            table="staging.orders",
            rows=rows,
            target_fields=[
                "order_id",
                "customer_id",
                "amount",
                "status",
                "updated_at",
                "batch_id",
            ],
            commit_every=1000,
        )

    # log load_history
    hook.run(
        sql=(
            "INSERT INTO metadata.load_history(batch_id, pipeline, table_name, file_path, row_count_source, row_count_staging) "
            "VALUES (%(batch_id)s, 'ELT', %(table)s, %(file)s, %(src)s, %(stg)s)"
        ),
        parameters={
            "batch_id": batch_id,
            "table": table,
            "file": path,
            "src": int(len(df)),
            "stg": int(len(df)),
        },
    )
    return int(len(df))


def run_transforms(batch_id: str) -> None:
    hook = PostgresHook(postgres_conn_id="postgres_default")
    sql_path = "/opt/workspace/lectures/day_9/sql/04_elt_transforms.sql"
    with open(sql_path, "r", encoding="utf-8") as f:
        sql = f.read()
    hook.run(sql=sql, parameters={"batch_id": batch_id})


def end_checks() -> None:
    hook = PostgresHook(postgres_conn_id="postgres_default")
    res = hook.get_first("SELECT COUNT(*) FROM dw.customers")
    if not res or res[0] is None or int(res[0]) <= 0:
        raise ValueError("dw.customers is empty after ELT")


with DAG(
    dag_id="elt_pipeline_dag",
    description="ELT pipeline: land CSV into staging then transform in SQL to DW",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    begin = PythonOperator(task_id="begin_checks", python_callable=begin_checks)

    batch_id = str(uuid.uuid4())

    with TaskGroup(group_id="load_staging"):
        load_customers = PythonOperator(
            task_id="load_customers",
            python_callable=load_to_staging,
            op_kwargs={
                "batch_id": batch_id,
                "table": "customers",
                "csv_name": "customers.csv",
            },
        )
        load_orders = PythonOperator(
            task_id="load_orders",
            python_callable=load_to_staging,
            op_kwargs={
                "batch_id": batch_id,
                "table": "orders",
                "csv_name": "orders.csv",
            },
        )

    transform = PythonOperator(
        task_id="run_sql_transforms",
        python_callable=run_transforms,
        op_kwargs={"batch_id": batch_id},
    )

    end = PythonOperator(task_id="end_checks", python_callable=end_checks)

    begin >> [load_customers, load_orders] >> transform >> end
