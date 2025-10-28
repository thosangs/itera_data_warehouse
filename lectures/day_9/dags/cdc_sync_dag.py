from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}


def ensure_cdc_enabled() -> None:
    # Debezium for Postgres is configured via Kafka Connect (handled in init.sh). Nothing to do here.
    return None


def apply_cdc_changes(table_name: str) -> None:
    # For Postgres + Debezium, we simply perform an upsert from source to DW as a snapshot step;
    # incremental streaming is handled by Kafka Connect -> downstream consumers (not in this DAG).
    hook = PostgresHook(postgres_conn_id="postgres_default")
    if table_name == "customers":
        hook.run(
            sql=(
                "INSERT INTO dw.customers (customer_id, name, city, is_active, updated_at) "
                "SELECT customer_id, name, city, TRUE, updated_at FROM source.customers "
                "ON CONFLICT (customer_id) DO UPDATE SET "
                "name = EXCLUDED.name, city = EXCLUDED.city, updated_at = EXCLUDED.updated_at, is_active = TRUE;"
            )
        )
    elif table_name == "orders":
        hook.run(
            sql=(
                "INSERT INTO dw.orders (order_id, customer_id, amount, status, is_active, updated_at) "
                "SELECT order_id, customer_id, amount, status, TRUE, updated_at FROM source.orders "
                "ON CONFLICT (order_id) DO UPDATE SET "
                "customer_id = EXCLUDED.customer_id, amount = EXCLUDED.amount, status = EXCLUDED.status, "
                "updated_at = EXCLUDED.updated_at, is_active = TRUE;"
            )
        )
    else:
        raise ValueError(table_name)


with DAG(
    dag_id="cdc_sync_dag",
    description="Apply snapshot sync from Postgres source to DW (Debezium handles incremental)",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    t_enable = PythonOperator(
        task_id="ensure_cdc_enabled", python_callable=ensure_cdc_enabled
    )

    t_customers = PythonOperator(
        task_id="apply_customers_changes",
        python_callable=apply_cdc_changes,
        op_kwargs={"table_name": "customers"},
    )
    t_orders = PythonOperator(
        task_id="apply_orders_changes",
        python_callable=apply_cdc_changes,
        op_kwargs={"table_name": "orders"},
    )

    t_enable >> [t_customers, t_orders]
