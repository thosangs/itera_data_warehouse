SET NOCOUNT ON;

-- Bulk load CSVs into source schema
BULK INSERT source.customers
FROM '/opt/workspace/lectures/day_9/data/raw/customers.csv'
WITH (
  FIRSTROW = 2,
  FIELDTERMINATOR = ',',
  ROWTERMINATOR = '0x0A',
  TABLOCK
);

BULK INSERT source.orders
FROM '/opt/workspace/lectures/day_9/data/raw/orders.csv'
WITH (
  FIRSTROW = 2,
  FIELDTERMINATOR = ',',
  ROWTERMINATOR = '0x0A',
  TABLOCK
);


