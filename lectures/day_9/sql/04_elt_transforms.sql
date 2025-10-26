-- Expects Jinja param: {{ params.batch_id }}
SET NOCOUNT ON;
DECLARE @batch_id UNIQUEIDENTIFIER = TRY_CONVERT(UNIQUEIDENTIFIER, '{{ params.batch_id }}');

-- If running outside Airflow, infer latest batch_id from staging; if no staging data, skip
IF @batch_id IS NULL
BEGIN
  IF OBJECT_ID('staging.customers') IS NOT NULL AND EXISTS (SELECT 1 FROM staging.customers)
  BEGIN
    SELECT TOP 1 @batch_id = batch_id FROM staging.customers ORDER BY load_ts DESC;
  END
  IF @batch_id IS NULL AND OBJECT_ID('staging.orders') IS NOT NULL AND EXISTS (SELECT 1 FROM staging.orders)
  BEGIN
    SELECT TOP 1 @batch_id = batch_id FROM staging.orders ORDER BY load_ts DESC;
  END
END

IF @batch_id IS NULL
BEGIN
  PRINT 'No staging batch_id available; skipping transforms.';
  RETURN;
END

-- Upsert customers
MERGE dw.customers AS tgt
USING (
  SELECT customer_id, name, city, updated_at
  FROM staging.customers
  WHERE batch_id = @batch_id
) AS src
ON tgt.customer_id = src.customer_id
WHEN MATCHED THEN UPDATE SET
  tgt.name = src.name,
  tgt.city = src.city,
  tgt.updated_at = src.updated_at,
  tgt.is_active = 1
WHEN NOT MATCHED BY TARGET THEN INSERT (customer_id, name, city, is_active, updated_at)
VALUES (src.customer_id, src.name, src.city, 1, src.updated_at)
WHEN NOT MATCHED BY SOURCE THEN UPDATE SET
  tgt.is_active = 0;

-- Upsert orders
MERGE dw.orders AS tgt
USING (
  SELECT order_id, customer_id, amount, status, updated_at
  FROM staging.orders
  WHERE batch_id = @batch_id
) AS src
ON tgt.order_id = src.order_id
WHEN MATCHED THEN UPDATE SET
  tgt.customer_id = src.customer_id,
  tgt.amount = src.amount,
  tgt.status = src.status,
  tgt.updated_at = src.updated_at,
  tgt.is_active = 1
WHEN NOT MATCHED BY TARGET THEN INSERT (order_id, customer_id, amount, status, is_active, updated_at)
VALUES (src.order_id, src.customer_id, src.amount, src.status, 1, src.updated_at)
WHEN NOT MATCHED BY SOURCE THEN UPDATE SET
  tgt.is_active = 0;


