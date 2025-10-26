from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}


def ensure_cdc_enabled() -> None:
    hook = MsSqlHook(mssql_conn_id="mssql_default")
    sql_path = "/opt/workspace/lectures/day_9/sql/03_enable_cdc.sql"
    with open(sql_path, "r", encoding="utf-8") as f:
        hook.run(sql=f.read())


def apply_cdc_changes(table_name: str) -> None:
    hook = MsSqlHook(mssql_conn_id="mssql_default")
    # get LSN window
    start_lsn = hook.get_first(
        "SELECT last_end_lsn FROM metadata.cdc_state WHERE table_name = %(t)s",
        parameters={"t": table_name},
    )
    start_lsn = start_lsn[0] if start_lsn and start_lsn[0] is not None else None
    end_lsn = hook.get_first("SELECT sys.fn_cdc_get_max_lsn()")
    end_lsn = end_lsn[0]

    if table_name == "customers":
        cdc_fn = "cdc.fn_cdc_get_all_changes_source_customers"
        key = "customer_id"
        cols = "customer_id, name, city, updated_at"
        dw_table = "dw.customers"
    elif table_name == "orders":
        cdc_fn = "cdc.fn_cdc_get_all_changes_source_orders"
        key = "order_id"
        cols = "order_id, customer_id, amount, status, updated_at"
        dw_table = "dw.orders"
    else:
        raise ValueError(table_name)

    # build dynamic MERGE using change rows
    if start_lsn is None:
        # initial snapshot: treat as full load using source table for simplicity
        src_query = f"SELECT {cols} FROM source.{table_name}"
    else:
        src_query = (
            f"SELECT {cols}, __$operation AS op FROM {cdc_fn}(%(s)s, %(e)s, 'all')"
        )

    # Upsert for inserts/updates; soft-delete on missing when using snapshot
    if start_lsn is None:
        hook.run(
            sql=(
                f"MERGE {dw_table} AS tgt USING ({src_query}) AS src ON tgt.{key} = src.{key} "
                f"WHEN MATCHED THEN UPDATE SET "
                f"  {', '.join([f' t.{c.strip()} = src.{c.strip()}' for c in cols.split(', ') if c.strip() not in [key]])} "
                f", t.updated_at = src.updated_at, t.is_active = 1 "
                f"WHEN NOT MATCHED THEN INSERT ({cols}, is_active) VALUES (src.{cols.replace(', ', ', src.')}, 1);"
            ),
            parameters={},
        )
    else:
        # process changes window
        rows = hook.get_records(src_query, parameters={"s": start_lsn, "e": end_lsn})
        # op: 1=delete, 2=insert, 3=update(before), 4=update(after)
        for r in rows:
            data = list(r)
            op = int(data[-1])
            values = data[:-1]
            if table_name == "customers":
                customer_id, name, city, updated_at = values
                if op in (2, 4):  # insert or update after
                    hook.run(
                        sql=(
                            "MERGE dw.customers AS tgt USING "
                            "(SELECT %(id)s AS customer_id, %(name)s AS name, %(city)s AS city, %(upd)s AS updated_at) AS src "
                            "ON tgt.customer_id = src.customer_id "
                            "WHEN MATCHED THEN UPDATE SET "
                            "name = src.name, city = src.city, updated_at = src.updated_at, is_active = 1 "
                            "WHEN NOT MATCHED THEN INSERT (customer_id, name, city, is_active, updated_at) "
                            "VALUES (src.customer_id, src.name, src.city, 1, src.updated_at);"
                        ),
                        parameters={
                            "id": customer_id,
                            "name": name,
                            "city": city,
                            "upd": updated_at,
                        },
                    )
                elif op == 1:  # delete
                    hook.run(
                        "UPDATE dw.customers SET is_active = 0 WHERE customer_id = %(id)s",
                        parameters={"id": customer_id},
                    )
            else:
                order_id, customer_id, amount, status, updated_at = values
                if op in (2, 4):
                    hook.run(
                        sql=(
                            "MERGE dw.orders AS tgt USING "
                            "(SELECT %(id)s AS order_id, %(cid)s AS customer_id, %(amt)s AS amount, "
                            "%(st)s AS status, %(upd)s AS updated_at) AS src "
                            "ON tgt.order_id = src.order_id "
                            "WHEN MATCHED THEN UPDATE "
                            "SET customer_id = src.customer_id, amount = src.amount, "
                            "status = src.status, updated_at = src.updated_at, is_active = 1 "
                            "WHEN NOT MATCHED THEN INSERT (order_id, customer_id, amount, status, is_active, updated_at) "
                            "VALUES (src.order_id, src.customer_id, src.amount, src.status, 1, src.updated_at);"
                        ),
                        parameters={
                            "id": order_id,
                            "cid": customer_id,
                            "amt": amount,
                            "st": status,
                            "upd": updated_at,
                        },
                    )
                elif op == 1:
                    hook.run(
                        "UPDATE dw.orders SET is_active = 0 WHERE order_id = %(id)s",
                        parameters={"id": order_id},
                    )

    # record LSN state
    hook.run(
        sql=(
            "MERGE metadata.cdc_state AS tgt USING (SELECT %(t)s AS table_name) AS src ON tgt.table_name = src.table_name "
            "WHEN MATCHED THEN UPDATE SET last_start_lsn = %(s)s, last_end_lsn = %(e)s, updated_at = SYSUTCDATETIME() "
            "WHEN NOT MATCHED THEN INSERT (table_name, last_start_lsn, last_end_lsn) VALUES (%(t)s, %(s)s, %(e)s);"
        ),
        parameters={"t": table_name, "s": start_lsn, "e": end_lsn},
    )


with DAG(
    dag_id="cdc_sync_dag",
    description="Apply SQL Server CDC changes to DW tables",
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
