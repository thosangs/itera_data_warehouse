-- ELT transforms for Day 9
-- Applies the current batch from staging into DW using upserts (SCD Type 1)
-- Expects parameter: %(batch_id)s (provided by Airflow PostgresHook)

-- Upsert customers from the current batch
INSERT INTO dw.customers (customer_id, name, city, is_active, updated_at)
SELECT s.customer_id, s.name, s.city, TRUE, s.updated_at
FROM staging.customers s
WHERE s.batch_id = %(batch_id)s
ON CONFLICT (customer_id) DO UPDATE
SET name = EXCLUDED.name,
    city = EXCLUDED.city,
    updated_at = EXCLUDED.updated_at,
    is_active = TRUE;

-- Record row counts for DW for this batch in metadata.load_history
UPDATE metadata.load_history lh
SET row_count_dw = sub.cnt
FROM (
  SELECT COUNT(*)::INT AS cnt
  FROM dw.customers d
  JOIN (
    SELECT DISTINCT customer_id
    FROM staging.customers
    WHERE batch_id = %(batch_id)s
  ) k USING (customer_id)
) sub
WHERE lh.batch_id = %(batch_id)s
  AND lh.pipeline = 'ELT'
  AND lh.table_name = 'customers';

-- Upsert orders from the current batch
INSERT INTO dw.orders (order_id, customer_id, amount, status, is_active, updated_at)
SELECT s.order_id, s.customer_id, s.amount, s.status, TRUE, s.updated_at
FROM staging.orders s
WHERE s.batch_id = %(batch_id)s
ON CONFLICT (order_id) DO UPDATE
SET customer_id = EXCLUDED.customer_id,
    amount = EXCLUDED.amount,
    status = EXCLUDED.status,
    updated_at = EXCLUDED.updated_at,
    is_active = TRUE;

-- Record row counts for DW for this batch in metadata.load_history
UPDATE metadata.load_history lh
SET row_count_dw = sub.cnt
FROM (
  SELECT COUNT(*)::INT AS cnt
  FROM dw.orders d
  JOIN (
    SELECT DISTINCT order_id
    FROM staging.orders
    WHERE batch_id = %(batch_id)s
  ) k USING (order_id)
) sub
WHERE lh.batch_id = %(batch_id)s
  AND lh.pipeline = 'ELT'
  AND lh.table_name = 'orders';

-- Optional: If your batches are full snapshots, you may want to deactivate
-- DW rows missing from the current snapshot. Commented out by default.
--
-- UPDATE dw.customers c
-- SET is_active = FALSE
-- WHERE NOT EXISTS (
--   SELECT 1 FROM staging.customers s
--   WHERE s.batch_id = %(batch_id)s AND s.customer_id = c.customer_id
-- );
--
-- UPDATE dw.orders o
-- SET is_active = FALSE
-- WHERE NOT EXISTS (
--   SELECT 1 FROM staging.orders s
--   WHERE s.batch_id = %(batch_id)s AND s.order_id = o.order_id
-- );


