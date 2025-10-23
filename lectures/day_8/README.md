# Day 8 – Multi-Input Operational Databases (MIOD) and Data Warehousing Granularity

This module covers combining multiple operational sources (MIOD) and building multi-level granular star schemas (Level-0/1/2).

## Learning objectives

- Understand MIOD patterns: vertical stacking vs horizontal stacking
- Design and build Level-0, Level-1, Level-2 stars
- Practice ingestion (CSV → SQL Server), integration, and analytics

## What we will do

1. Load small CSVs into SQL Server as operational/source tables.
2. MIOD – Vertical stacking (Clubs): integrate multiple clubs’ finance facts.
3. MIOD – Horizontal stacking (Property): integrate inspections, auctions, sales into one fact.
4. Granularity – Web/App events: build Level-0/1/2 stars and run DAU/WAU/funnel queries.

## Theory highlights

- MIOD
  - Vertical: stack compatible fact tables using UNION; shared dimensions (e.g., Club, Year) and common measures (Income, Expenditure).
  - Horizontal: combine distinct measures from different sources into one integrated fact on shared keys (e.g., Suburb, Month).
- Granularity and Levels
  - Level-0: highest granularity, no aggregation (raw facts).
  - Level-1: initial aggregation (add detail on some dimensions, aggregate on others).
  - Level-2: higher aggregation (fewer/more general dimensions).

## How to run

- Ensure the stack is up: `make up`
- Execute all scripts: `make day-run DAY=8`
- Use SQLPad at http://localhost:3000 to run `queries/practical.sql` or explore tables.

## Script order

1. `sql/00_create_schemas.sql`
2. `sql/01_load_data.sql`
3. `sql/02_dwh_vertical.sql`
4. `sql/03_dwh_horizontal.sql`
5. `sql/04_dwh_events_granularity.sql`

## Data files

CSV files are mounted inside the SQL Server container at `/opt/workspace/lectures/day_8/data`.

- Events CSV includes a leading `EventID` column with empty values so SQL Server auto-generates the identity.
- If you edit CSVs on Windows, ensure line endings are `\n` or adjust BULK INSERT accordingly.
