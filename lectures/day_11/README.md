# Day 11 - OLAP with Postgres + SQLPad

This lab mirrors the Day 9 structure but focuses on OLAP analysis using Postgres and SQLPad. It includes:

- Dockerized Postgres and SQLPad
- A synthetic sales dataset generator with 8 dimensions
- SQL scripts to create schemas/tables and bulk load CSVs
- Example queries: aggregates, ROLLUP, CUBE, and ranking/window functions

## Prerequisites

- Docker and Docker Compose
- Python 3.9+ available on the host (for data generation)

## Quickstart

1. Set environment variables (or rely on defaults):

   - `DB_NAME=example`
   - `SQLPAD_ADMIN=admin@example.com`
   - `SQLPAD_ADMIN_PASSWORD=admin`

2. Start containers from this directory:

   ```bash
   cd lectures/day_11/docker
   DB_NAME=example SQLPAD_ADMIN=admin@example.com SQLPAD_ADMIN_PASSWORD=admin docker compose up -d
   ```

3. Initialize database objects and load data:

   ```bash
   cd ../scripts
   DB_NAME=example ./init.sh
   ```

4. Open SQLPad: `http://localhost:3000`
   - Sign in with the admin credentials.
   - The Postgres connection is preconfigured and named after `DB_NAME`.

## Data Model

- Schema: `olap`
- Dimensions:
  - `dim_date(date_id, date, year, quarter, month, day_of_week)`
  - `dim_product(product_id, product_name, category, subcategory)`
  - `dim_customer(customer_id, customer_name, segment, city, country)`
  - `dim_store(store_id, store_name, city, region, country)`
  - `dim_channel(channel_id, channel_name)`
  - `dim_salesperson(salesperson_id, salesperson_name, team)`
  - `dim_promotion(promotion_id, promotion_name, promo_type)`
- Fact:
  - `fact_sales(sales_id, date_id, product_id, customer_id, store_id, channel_id, salesperson_id, promotion_id, quantity, unit_price, discount_rate, sales_amount, cost_amount)`

## SQL Examples

Run these in SQLPad after initialization:

- Aggregates: `sql/03_examples_aggregates.sql`
- ROLLUP & CUBE: `sql/04_examples_rollup_cube.sql`
- Ranking functions: `sql/05_examples_ranking.sql`

## Folder Structure (similar to Day 9)

```
lectures/day_11/
  docker/
    docker-compose.yml
  scripts/
    generate_data.py
    init.sh
  sql/
    00_create_schemas.sql
    01_create_tables.sql
    02_load_data.sql
    03_examples_aggregates.sql
    04_examples_rollup_cube.sql
    05_examples_ranking.sql
  data/
    raw/  # generated CSVs are written here
```
