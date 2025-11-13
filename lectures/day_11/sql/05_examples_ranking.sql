-- Ranking (window) function examples

-- 1) ROW_NUMBER: Top 10 products by sales globally
WITH product_sales AS (
  SELECT
    p.product_id,
    p.product_name,
    SUM(f.sales_amount) AS total_sales
  FROM olap.fact_sales f
  JOIN olap.dim_product p ON p.product_id = f.product_id
  GROUP BY p.product_id, p.product_name
)
SELECT
  ROW_NUMBER() OVER (ORDER BY total_sales DESC) AS row_num,
  product_id, product_name, ROUND(total_sales, 2) AS total_sales
FROM product_sales
ORDER BY row_num
LIMIT 10;

-- 2) RANK within each country: products ranked by sales
WITH product_country AS (
  SELECT
    s.country,
    p.product_name,
    SUM(f.sales_amount) AS total_sales
  FROM olap.fact_sales f
  JOIN olap.dim_store s ON s.store_id = f.store_id
  JOIN olap.dim_product p ON p.product_id = f.product_id
  GROUP BY s.country, p.product_name
)
SELECT
  country,
  product_name,
  ROUND(total_sales, 2) AS total_sales,
  RANK() OVER (PARTITION BY country ORDER BY total_sales DESC) AS sales_rank
FROM product_country
ORDER BY country, sales_rank;

-- 3) DENSE_RANK within each region and channel
WITH region_channel AS (
  SELECT
    s.region,
    ch.channel_name,
    p.category,
    SUM(f.sales_amount) AS total_sales
  FROM olap.fact_sales f
  JOIN olap.dim_store s ON s.store_id = f.store_id
  JOIN olap.dim_channel ch ON ch.channel_id = f.channel_id
  JOIN olap.dim_product p ON p.product_id = f.product_id
  GROUP BY s.region, ch.channel_name, p.category
)
SELECT
  region,
  channel_name,
  category,
  ROUND(total_sales, 2) AS total_sales,
  DENSE_RANK() OVER (PARTITION BY region, channel_name ORDER BY total_sales DESC) AS category_rank
FROM region_channel
ORDER BY region, channel_name, category_rank;

-- 4) NTILE quartiles of store performance (by sales)
WITH store_sales AS (
  SELECT
    s.store_id,
    s.store_name,
    s.country,
    SUM(f.sales_amount) AS total_sales
  FROM olap.fact_sales f
  JOIN olap.dim_store s ON s.store_id = f.store_id
  GROUP BY s.store_id, s.store_name, s.country
)
SELECT
  store_id,
  store_name,
  country,
  ROUND(total_sales, 2) AS total_sales,
  NTILE(4) OVER (ORDER BY total_sales DESC) AS performance_quartile
FROM store_sales
ORDER BY performance_quartile, total_sales DESC;