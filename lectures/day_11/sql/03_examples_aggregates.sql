-- Basic aggregates and GROUP BY examples
-- These can be run in SQLPad after loading data.

-- 1) Total sales amount by product category
SELECT
  p.category,
  SUM(f.sales_amount) AS total_sales,
  SUM(f.quantity) AS total_qty
FROM olap.fact_sales f
JOIN olap.dim_product p ON p.product_id = f.product_id
GROUP BY p.category
ORDER BY total_sales DESC;

-- 2) Total sales by country and channel
SELECT
  s.country,
  ch.channel_name,
  ROUND(SUM(f.sales_amount), 2) AS total_sales
FROM olap.fact_sales f
JOIN olap.dim_store s ON s.store_id = f.store_id
JOIN olap.dim_channel ch ON ch.channel_id = f.channel_id
GROUP BY s.country, ch.channel_name
ORDER BY s.country, total_sales DESC;

-- 3) Monthly sales trend (Year-Month)
SELECT
  d.year,
  d.month,
  ROUND(SUM(f.sales_amount), 2) AS total_sales
FROM olap.fact_sales f
JOIN olap.dim_date d ON d.date_id = f.date_id
GROUP BY d.year, d.month
ORDER BY d.year, d.month;


