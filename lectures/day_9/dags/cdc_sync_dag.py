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
    # Generate a brand-new random row and insert it into SOURCE tables
    # so that Debezium emits CDC events. Each run inserts one new record.
    import random
    from datetime import datetime, timezone

    hook = PostgresHook(postgres_conn_id="postgres_default")

    if table_name == "customers":
        # Compute next natural key in source
        next_id_row = hook.get_first(
            "SELECT COALESCE(MAX(customer_id), 0) + 1 FROM source.customers"
        )
        next_id = int(
            next_id_row[0] if next_id_row and next_id_row[0] is not None else 1
        )

        names = [
            "Alice",
            "Bob",
            "Carol",
            "Dave",
            "Erin",
            "Frank",
            "Grace",
            "Heidi",
            "Ivan",
            "Judy",
        ]
        cities = [
            "Jakarta",
            "Tokyo",
            "Singapore",
            "Sydney",
            "Kuala Lumpur",
            "Bangkok",
            "Manila",
            "Seoul",
        ]

        params = {
            "id": next_id,
            "name": f"{random.choice(names)} {random.randint(1000, 9999)}",
            "city": random.choice(cities),
            "updated_at": datetime.now(timezone.utc),
        }

        hook.run(
            sql=(
                "INSERT INTO source.customers (customer_id, name, city, updated_at) "
                "VALUES (%(id)s, %(name)s, %(city)s, %(updated_at)s);"
            ),
            parameters=params,
        )

    elif table_name == "orders":
        # Ensure we have at least one source customer to reference
        existing_cid = hook.get_first(
            "SELECT customer_id FROM source.customers ORDER BY random() LIMIT 1"
        )
        if not existing_cid or existing_cid[0] is None:
            # Create a fallback customer
            fallback_id_row = hook.get_first(
                "SELECT COALESCE(MAX(customer_id), 0) + 1 FROM source.customers"
            )
            fallback_id = int(
                fallback_id_row[0]
                if fallback_id_row and fallback_id_row[0] is not None
                else 1
            )
            hook.run(
                sql=(
                    "INSERT INTO source.customers (customer_id, name, city, updated_at) "
                    "VALUES (%(id)s, 'Auto Customer', 'Jakarta', %(updated_at)s);"
                ),
                parameters={
                    "id": fallback_id,
                    "updated_at": datetime.now(timezone.utc),
                },
            )
            customer_id = fallback_id
        else:
            customer_id = int(existing_cid[0])

        # Next order id
        next_oid_row = hook.get_first(
            "SELECT COALESCE(MAX(order_id), 0) + 1 FROM source.orders"
        )
        order_id = int(
            next_oid_row[0] if next_oid_row and next_oid_row[0] is not None else 1
        )

        statuses = ["new", "processing", "shipped", "cancelled"]
        params = {
            "id": order_id,
            "cid": customer_id,
            "amt": round(random.uniform(10.0, 500.0), 2),
            "status": random.choice(statuses),
            "updated_at": datetime.now(timezone.utc),
        }

        hook.run(
            sql=(
                "INSERT INTO source.orders (order_id, customer_id, amount, status, updated_at) "
                "VALUES (%(id)s, %(cid)s, %(amt)s, %(status)s, %(updated_at)s);"
            ),
            parameters=params,
        )
    else:
        raise ValueError(table_name)


def update_one_row(table_name: str) -> None:
    # Update exactly one random row in source tables to emit CDC events
    import random
    from datetime import datetime, timezone

    hook = PostgresHook(postgres_conn_id="postgres_default")

    if table_name == "customers":
        row = hook.get_first(
            "SELECT customer_id FROM source.customers ORDER BY random() LIMIT 1"
        )
        if not row or row[0] is None:
            return
        customer_id = int(row[0])
        cities = [
            "Jakarta",
            "Tokyo",
            "Singapore",
            "Sydney",
            "Kuala Lumpur",
            "Bangkok",
            "Manila",
            "Seoul",
        ]
        params = {
            "id": customer_id,
            "city": random.choice([c for c in cities]),
            "updated_at": datetime.now(timezone.utc),
        }
        hook.run(
            sql=(
                "UPDATE source.customers SET city = %(city)s, updated_at = %(updated_at)s "
                "WHERE customer_id = %(id)s"
            ),
            parameters=params,
        )
    elif table_name == "orders":
        row = hook.get_first(
            "SELECT order_id FROM source.orders ORDER BY random() LIMIT 1"
        )
        if not row or row[0] is None:
            return
        order_id = int(row[0])
        statuses = ["new", "processing", "shipped", "cancelled"]
        params = {
            "id": order_id,
            "amt": round(random.uniform(10.0, 500.0), 2),
            "status": random.choice(statuses),
            "updated_at": datetime.now(timezone.utc),
        }
        hook.run(
            sql=(
                "UPDATE source.orders SET amount = %(amt)s, status = %(status)s, updated_at = %(updated_at)s "
                "WHERE order_id = %(id)s"
            ),
            parameters=params,
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

    t_update_customers = PythonOperator(
        task_id="update_one_customer",
        python_callable=update_one_row,
        op_kwargs={"table_name": "customers"},
    )

    t_update_orders = PythonOperator(
        task_id="update_one_order",
        python_callable=update_one_row,
        op_kwargs={"table_name": "orders"},
    )

    t_enable >> [t_customers, t_orders] >> [t_update_customers, t_update_orders]
