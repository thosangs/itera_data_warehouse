-- Reset Day 9 tables to avoid duplicate key errors on reruns
-- Drops existing source/staging/dw tables; they will be recreated by 01_create_tables.sql

DROP TABLE IF EXISTS source.orders;
DROP TABLE IF EXISTS source.customers;

DROP TABLE IF EXISTS staging.orders;
DROP TABLE IF EXISTS staging.customers;

-- Sink materialized tables
DROP TABLE IF EXISTS staging.orders_cdc;
DROP TABLE IF EXISTS staging.customers_cdc;

DROP TABLE IF EXISTS dw.orders;
DROP TABLE IF EXISTS dw.customers;


-- CDC event log
DROP TABLE IF EXISTS metadata.cdc_events;

