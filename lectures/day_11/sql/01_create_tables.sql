-- Drop existing to allow repeatable runs (lab-friendly)
DROP TABLE IF EXISTS olap.fact_sales CASCADE;
DROP TABLE IF EXISTS olap.dim_date CASCADE;
DROP TABLE IF EXISTS olap.dim_product CASCADE;
DROP TABLE IF EXISTS olap.dim_customer CASCADE;
DROP TABLE IF EXISTS olap.dim_store CASCADE;
DROP TABLE IF EXISTS olap.dim_channel CASCADE;
DROP TABLE IF EXISTS olap.dim_salesperson CASCADE;
DROP TABLE IF EXISTS olap.dim_promotion CASCADE;

-- Dimensions
CREATE TABLE olap.dim_date (
    date_id       integer PRIMARY KEY, -- YYYYMMDD
    date          date NOT NULL,
    year          integer NOT NULL,
    quarter       integer NOT NULL CHECK (quarter BETWEEN 1 AND 4),
    month         integer NOT NULL CHECK (month BETWEEN 1 AND 12),
    day_of_week   integer NOT NULL CHECK (day_of_week BETWEEN 1 AND 7)
);

CREATE TABLE olap.dim_product (
    product_id    integer PRIMARY KEY,
    product_name  text NOT NULL,
    category      text NOT NULL,
    subcategory   text NOT NULL
);

CREATE TABLE olap.dim_customer (
    customer_id    integer PRIMARY KEY,
    customer_name  text NOT NULL,
    segment        text NOT NULL,
    city           text NOT NULL,
    country        text NOT NULL
);

CREATE TABLE olap.dim_store (
    store_id    integer PRIMARY KEY,
    store_name  text NOT NULL,
    city        text NOT NULL,
    region      text NOT NULL,
    country     text NOT NULL
);

CREATE TABLE olap.dim_channel (
    channel_id    integer PRIMARY KEY,
    channel_name  text NOT NULL
);

CREATE TABLE olap.dim_salesperson (
    salesperson_id    integer PRIMARY KEY,
    salesperson_name  text NOT NULL,
    team              text NOT NULL
);

CREATE TABLE olap.dim_promotion (
    promotion_id    integer PRIMARY KEY, -- 0 will mean 'No Promotion' in fact for convenience
    promotion_name  text NOT NULL,
    promo_type      text NOT NULL
);

-- Facts
CREATE TABLE olap.fact_sales (
    sales_id        bigint PRIMARY KEY,
    date_id         integer NOT NULL REFERENCES olap.dim_date (date_id),
    product_id      integer NOT NULL REFERENCES olap.dim_product (product_id),
    customer_id     integer NOT NULL REFERENCES olap.dim_customer (customer_id),
    store_id        integer NOT NULL REFERENCES olap.dim_store (store_id),
    channel_id      integer NOT NULL REFERENCES olap.dim_channel (channel_id),
    salesperson_id  integer NOT NULL REFERENCES olap.dim_salesperson (salesperson_id),
    promotion_id    integer NOT NULL, -- allow 0 for 'no promotion'
    quantity        integer NOT NULL CHECK (quantity > 0),
    unit_price      numeric(12,2) NOT NULL CHECK (unit_price >= 0),
    discount_rate   numeric(5,2) NOT NULL CHECK (discount_rate >= 0),
    sales_amount    numeric(14,2) NOT NULL CHECK (sales_amount >= 0),
    cost_amount     numeric(14,2) NOT NULL CHECK (cost_amount >= 0),
    CONSTRAINT fact_sales_promotion_fk
        FOREIGN KEY (promotion_id)
        REFERENCES olap.dim_promotion (promotion_id)
        DEFERRABLE INITIALLY DEFERRED
);

-- Helpful indexes for typical OLAP joins/filters
CREATE INDEX IF NOT EXISTS fact_sales_date_idx ON olap.fact_sales (date_id);
CREATE INDEX IF NOT EXISTS fact_sales_product_idx ON olap.fact_sales (product_id);
CREATE INDEX IF NOT EXISTS fact_sales_store_idx ON olap.fact_sales (store_id);
CREATE INDEX IF NOT EXISTS fact_sales_channel_idx ON olap.fact_sales (channel_id);
CREATE INDEX IF NOT EXISTS fact_sales_salesperson_idx ON olap.fact_sales (salesperson_id);
CREATE INDEX IF NOT EXISTS fact_sales_customer_idx ON olap.fact_sales (customer_id);
CREATE INDEX IF NOT EXISTS fact_sales_promotion_idx ON olap.fact_sales (promotion_id);


