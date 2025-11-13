-- Load dimensions
COPY olap.dim_date (date_id, date, year, quarter, month, day_of_week)
FROM '/opt/workspace/lectures/day_11/data/raw/dim_date.csv'
WITH (FORMAT csv, HEADER true);

COPY olap.dim_product (product_id, product_name, category, subcategory)
FROM '/opt/workspace/lectures/day_11/data/raw/dim_product.csv'
WITH (FORMAT csv, HEADER true);

COPY olap.dim_customer (customer_id, customer_name, segment, city, country)
FROM '/opt/workspace/lectures/day_11/data/raw/dim_customer.csv'
WITH (FORMAT csv, HEADER true);

COPY olap.dim_store (store_id, store_name, city, region, country)
FROM '/opt/workspace/lectures/day_11/data/raw/dim_store.csv'
WITH (FORMAT csv, HEADER true);

COPY olap.dim_channel (channel_id, channel_name)
FROM '/opt/workspace/lectures/day_11/data/raw/dim_channel.csv'
WITH (FORMAT csv, HEADER true);

COPY olap.dim_salesperson (salesperson_id, salesperson_name, team)
FROM '/opt/workspace/lectures/day_11/data/raw/dim_salesperson.csv'
WITH (FORMAT csv, HEADER true);

COPY olap.dim_promotion (promotion_id, promotion_name, promo_type)
FROM '/opt/workspace/lectures/day_11/data/raw/dim_promotion.csv'
WITH (FORMAT csv, HEADER true);

-- Ensure a 'No Promotion' member exists for promotion_id = 0 (used in fact)
INSERT INTO olap.dim_promotion (promotion_id, promotion_name, promo_type)
VALUES (0, 'No Promotion', 'None')
ON CONFLICT (promotion_id) DO NOTHING;

-- Load facts
COPY olap.fact_sales (
    sales_id, date_id, product_id, customer_id, store_id, channel_id,
    salesperson_id, promotion_id, quantity, unit_price, discount_rate,
    sales_amount, cost_amount
)
FROM '/opt/workspace/lectures/day_11/data/raw/fact_sales.csv'
WITH (FORMAT csv, HEADER true);


