-- ROLLUP and CUBE examples
-- These demonstrate hierarchical and cross-aggregation in SQL.

-- 1) ROLLUP: country -> region -> store
SELECT
  s.country,
  s.region,
  s.store_name,
  ROUND(SUM(f.sales_amount), 2) AS total_sales
FROM olap.fact_sales f
JOIN olap.dim_store s ON s.store_id = f.store_id
GROUP BY ROLLUP (s.country, s.region, s.store_name)
ORDER BY s.country NULLS LAST, s.region NULLS LAST, s.store_name NULLS LAST;

-- 2) CUBE: category x channel x country
SELECT
  p.category,
  ch.channel_name,
  s.country,
  ROUND(SUM(f.sales_amount), 2) AS total_sales
FROM olap.fact_sales f
JOIN olap.dim_product p ON p.product_id = f.product_id
JOIN olap.dim_channel ch ON ch.channel_id = f.channel_id
JOIN olap.dim_store s ON s.store_id = f.store_id
GROUP BY CUBE (p.category, ch.channel_name, s.country)
ORDER BY p.category NULLS LAST, ch.channel_name NULLS LAST, s.country NULLS LAST;


