from __future__ import annotations

import os
from datetime import datetime

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

RAW_DIR = "/opt/workspace/lectures/day_9/data/raw"


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}


def extract_transform(csv_name: str) -> pd.DataFrame:
    path = os.path.join(RAW_DIR, csv_name)
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    df = pd.read_csv(path)
    # cleansing: trim strings, fill missing, enforce types
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].astype(str).str.strip()

    if csv_name == "customers.csv":
        df["customer_id"] = df["customer_id"].astype(int)
        df = df.drop_duplicates(subset=["customer_id"])  # conform uniqueness
        df["updated_at"] = pd.to_datetime(df["updated_at"], utc=True)
    else:
        df["order_id"] = df["order_id"].astype(int)
        df = df.drop_duplicates(subset=["order_id"])  # conform uniqueness
        df["customer_id"] = df["customer_id"].astype(int)
        df["amount"] = df["amount"].astype(float).clip(lower=0)
        df["status"] = df["status"].where(
            df["status"].isin(["new", "processing", "shipped", "cancelled"]), "new"
        )
        df["updated_at"] = pd.to_datetime(df["updated_at"], utc=True)
    return df


def load_dw(table: str, df: pd.DataFrame) -> int:
    hook = PostgresHook(postgres_conn_id="postgres_default")
    if table == "customers":
        rows = [
            (
                int(r.customer_id),
                str(r.name),
                str(r.city),
                pd.Timestamp(r.updated_at).to_pydatetime(),
            )
            for r in df.itertuples(index=False)
        ]
        # upsert via INSERT ... ON CONFLICT one by one (simple)
        for row in rows:
            hook.run(
                sql=(
                    "INSERT INTO dw.customers (customer_id, name, city, is_active, updated_at) "
                    "VALUES (%(id)s, %(name)s, %(city)s, TRUE, %(upd)s) "
                    "ON CONFLICT (customer_id) DO UPDATE SET "
                    "name = EXCLUDED.name, city = EXCLUDED.city, updated_at = EXCLUDED.updated_at, is_active = TRUE;"
                ),
                parameters={
                    "id": row[0],
                    "name": row[1],
                    "city": row[2],
                    "upd": row[3],
                },
            )
    else:
        rows = [
            (
                int(r.order_id),
                int(r.customer_id),
                float(r.amount),
                str(r.status),
                pd.Timestamp(r.updated_at).to_pydatetime(),
            )
            for r in df.itertuples(index=False)
        ]
        for row in rows:
            hook.run(
                sql=(
                    "INSERT INTO dw.orders (order_id, customer_id, amount, status, is_active, updated_at) "
                    "VALUES (%(id)s, %(cid)s, %(amt)s, %(st)s, TRUE, %(upd)s) "
                    "ON CONFLICT (order_id) DO UPDATE SET "
                    "customer_id = EXCLUDED.customer_id, amount = EXCLUDED.amount, "
                    "status = EXCLUDED.status, updated_at = EXCLUDED.updated_at, is_active = TRUE;"
                ),
                parameters={
                    "id": row[0],
                    "cid": row[1],
                    "amt": row[2],
                    "st": row[3],
                    "upd": row[4],
                },
            )
    return len(df)


def end_checks() -> None:
    hook = PostgresHook(postgres_conn_id="postgres_default")
    res = hook.get_first("SELECT COUNT(*) FROM dw.orders")
    if not res or int(res[0]) <= 0:
        raise ValueError("dw.orders is empty after ETL")


with DAG(
    dag_id="etl_pipeline_dag",
    description="ETL pipeline: transform in Python, load directly to DW",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    t_extract_customers = PythonOperator(
        task_id="extract_transform_customers",
        python_callable=extract_transform,
        op_kwargs={"csv_name": "customers.csv"},
    )
    t_extract_orders = PythonOperator(
        task_id="extract_transform_orders",
        python_callable=extract_transform,
        op_kwargs={"csv_name": "orders.csv"},
    )

    t_load_customers = PythonOperator(
        task_id="load_dw_customers",
        python_callable=lambda df: load_dw("customers", df),
        op_args=[t_extract_customers.output],
    )
    t_load_orders = PythonOperator(
        task_id="load_dw_orders",
        python_callable=lambda df: load_dw("orders", df),
        op_args=[t_extract_orders.output],
    )

    t_end = PythonOperator(task_id="end_checks", python_callable=end_checks)

    (
        [t_extract_customers, t_extract_orders]
        >> [t_load_customers, t_load_orders]
        >> t_end
    )
