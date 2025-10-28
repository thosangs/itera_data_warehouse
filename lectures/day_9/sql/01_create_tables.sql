-- PostgreSQL DDL for Day 9 (kept for reference if you prefer SQL over init.sh)

-- Metadata tables
CREATE TABLE IF NOT EXISTS metadata.load_history (
  id BIGSERIAL PRIMARY KEY,
  batch_id UUID NOT NULL,
  pipeline TEXT NOT NULL,
  table_name TEXT NULL,
  file_path TEXT NULL,
  row_count_source INT NULL,
  row_count_staging INT NULL,
  row_count_dw INT NULL,
  load_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS metadata.data_inventory (
  id BIGSERIAL PRIMARY KEY,
  dataset_name TEXT NOT NULL,
  file_path TEXT NOT NULL,
  expected_columns TEXT NULL,
  actual_columns TEXT NULL,
  row_count INT NULL,
  profile_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS metadata.universe_compare_log (
  id BIGSERIAL PRIMARY KEY,
  table_name TEXT NOT NULL,
  compare_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  source_count BIGINT NULL,
  dw_count BIGINT NULL,
  source_checksum BIGINT NULL,
  dw_checksum BIGINT NULL,
  status TEXT NOT NULL
);

-- Source tables
CREATE TABLE IF NOT EXISTS source.customers (
  customer_id INT PRIMARY KEY,
  name TEXT NOT NULL,
  city TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS source.orders (
  order_id INT PRIMARY KEY,
  customer_id INT NOT NULL,
  amount NUMERIC(10,2) NOT NULL,
  status TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL
);

-- Staging tables
CREATE TABLE IF NOT EXISTS staging.customers (
  customer_id INT NOT NULL,
  name TEXT NOT NULL,
  city TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,
  batch_id UUID NOT NULL,
  load_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_stg_customers_batch ON staging.customers(batch_id);

CREATE TABLE IF NOT EXISTS staging.orders (
  order_id INT NOT NULL,
  customer_id INT NOT NULL,
  amount NUMERIC(10,2) NOT NULL,
  status TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,
  batch_id UUID NOT NULL,
  load_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_stg_orders_batch ON staging.orders(batch_id);

-- DW tables
CREATE TABLE IF NOT EXISTS dw.customers (
  customer_id INT PRIMARY KEY,
  name TEXT NOT NULL,
  city TEXT NOT NULL,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS dw.orders (
  order_id INT PRIMARY KEY,
  customer_id INT NOT NULL,
  amount NUMERIC(10,2) NOT NULL,
  status TEXT NOT NULL,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  updated_at TIMESTAMPTZ NOT NULL
);
