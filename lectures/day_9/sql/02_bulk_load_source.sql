-- PostgreSQL bulk load using COPY
COPY source.customers (customer_id, name, city, updated_at)
FROM '/opt/workspace/lectures/day_9/data/raw/customers.csv'
WITH (FORMAT csv, HEADER true);

COPY source.orders (order_id, customer_id, amount, status, updated_at)
FROM '/opt/workspace/lectures/day_9/data/raw/orders.csv'
WITH (FORMAT csv, HEADER true);
