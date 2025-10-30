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


def wait_for_stream(seconds: int = 15) -> None:
    import time

    time.sleep(int(seconds))


def consume_cdc_and_log(max_messages: int = 50, timeout_s: int = 10) -> None:
    # Consume Debezium events from Kafka and log to metadata.cdc_events
    import json
    import uuid

    from kafka import KafkaConsumer

    hook = PostgresHook(postgres_conn_id="postgres_default")
    dbname_row = hook.get_first("SELECT current_database()")
    dbname = dbname_row[0] if dbname_row else "example"

    topics = [f"{dbname}.source.customers", f"{dbname}.source.orders"]

    consumer = KafkaConsumer(
        *topics,
        bootstrap_servers="kafka:9092",
        group_id=f"airflow-cdc-logger-{uuid.uuid4()}",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda v: json.loads(v.decode("utf-8")) if v else None,
        consumer_timeout_ms=int(timeout_s * 1000),
    )

    count = 0
    for msg in consumer:
        val = msg.value or {}
        key = msg.key
        op = val.get("op")
        ts = val.get("ts_ms")

        hook.run(
            sql=(
                "INSERT INTO metadata.cdc_events (topic, partition, offset, key, op, ts_ms, payload) "
                "VALUES (%(t)s, %(p)s, %(o)s, %(k)s, %(op)s, %(ts)s, %(v)s) "
                "ON CONFLICT (topic, partition, offset) DO NOTHING"
            ),
            parameters={
                "t": msg.topic,
                "p": int(msg.partition),
                "o": int(msg.offset),
                "k": json.dumps(key) if key is not None else None,
                "op": op,
                "ts": ts,
                "v": json.dumps(val),
            },
        )

        count += 1
        if count >= int(max_messages):
            break


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

    t_wait = PythonOperator(
        task_id="wait_for_stream",
        python_callable=wait_for_stream,
        op_kwargs={"seconds": 15},
    )

    t_consume = PythonOperator(
        task_id="consume_cdc_and_log",
        python_callable=consume_cdc_and_log,
        op_kwargs={"max_messages": 50, "timeout_s": 10},
    )

    t_enable >> [t_customers, t_orders] >> t_wait >> t_consume
