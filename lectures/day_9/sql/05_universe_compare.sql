WITH counts AS (
  SELECT 'customers' AS table_name,
         (SELECT COUNT(*) FROM source.customers) AS source_count,
         (SELECT COUNT(*) FROM dw.customers WHERE is_active = TRUE) AS dw_count
  UNION ALL
  SELECT 'orders' AS table_name,
         (SELECT COUNT(*) FROM source.orders) AS source_count,
         (SELECT COUNT(*) FROM dw.orders WHERE is_active = TRUE) AS dw_count
)
INSERT INTO metadata.universe_compare_log(table_name, source_count, dw_count, source_checksum, dw_checksum, status)
SELECT table_name,
       source_count,
       dw_count,
       source_count AS source_checksum,
       dw_count AS dw_checksum,
       CASE WHEN source_count = dw_count THEN 'OK' ELSE 'MISMATCH' END AS status
FROM counts;
