SET NOCOUNT ON;

-- Truncate tables if they already exist (supports idempotent reruns)
IF OBJECT_ID('source.customers') IS NOT NULL
BEGIN
  -- If CDC is enabled at DB level and for this table, TRUNCATE is not allowed; use DELETE instead
  IF EXISTS (SELECT 1 FROM sys.databases WHERE name = DB_NAME() AND is_cdc_enabled = 1)
  BEGIN
    IF EXISTS (SELECT 1 FROM cdc.change_tables WHERE source_object_id = OBJECT_ID('source.customers'))
    BEGIN
      DELETE FROM source.customers;
    END
    ELSE
    BEGIN
      TRUNCATE TABLE source.customers;
    END
  END
  ELSE
  BEGIN
    TRUNCATE TABLE source.customers;
  END
END

IF OBJECT_ID('source.orders') IS NOT NULL
BEGIN
  IF EXISTS (SELECT 1 FROM sys.databases WHERE name = DB_NAME() AND is_cdc_enabled = 1)
  BEGIN
    IF EXISTS (SELECT 1 FROM cdc.change_tables WHERE source_object_id = OBJECT_ID('source.orders'))
    BEGIN
      DELETE FROM source.orders;
    END
    ELSE
    BEGIN
      TRUNCATE TABLE source.orders;
    END
  END
  ELSE
  BEGIN
    TRUNCATE TABLE source.orders;
  END
END

IF OBJECT_ID('staging.customers') IS NOT NULL
BEGIN
  TRUNCATE TABLE staging.customers;
END

IF OBJECT_ID('staging.orders') IS NOT NULL
BEGIN
  TRUNCATE TABLE staging.orders;
END

IF OBJECT_ID('dw.customers') IS NOT NULL
BEGIN
  TRUNCATE TABLE dw.customers;
END

IF OBJECT_ID('dw.orders') IS NOT NULL
BEGIN
  TRUNCATE TABLE dw.orders;
END

-- metadata tables
IF OBJECT_ID('metadata.load_history') IS NULL
BEGIN
  CREATE TABLE metadata.load_history (
    id BIGINT IDENTITY(1,1) PRIMARY KEY,
    batch_id UNIQUEIDENTIFIER NOT NULL,
    pipeline NVARCHAR(50) NOT NULL,
    table_name NVARCHAR(128) NULL,
    file_path NVARCHAR(400) NULL,
    row_count_source INT NULL,
    row_count_staging INT NULL,
    row_count_dw INT NULL,
    load_ts DATETIME2(3) NOT NULL CONSTRAINT DF_load_history_load_ts DEFAULT SYSUTCDATETIME()
  );
END

IF OBJECT_ID('metadata.data_inventory') IS NULL
BEGIN
  CREATE TABLE metadata.data_inventory (
    id BIGINT IDENTITY(1,1) PRIMARY KEY,
    dataset_name NVARCHAR(100) NOT NULL,
    file_path NVARCHAR(400) NOT NULL,
    expected_columns NVARCHAR(MAX) NULL,
    actual_columns NVARCHAR(MAX) NULL,
    row_count INT NULL,
    profile_ts DATETIME2(3) NOT NULL CONSTRAINT DF_data_inventory_profile_ts DEFAULT SYSUTCDATETIME()
  );
END

IF OBJECT_ID('metadata.cdc_state') IS NULL
BEGIN
  CREATE TABLE metadata.cdc_state (
    table_name NVARCHAR(128) PRIMARY KEY,
    last_start_lsn VARBINARY(10) NULL,
    last_end_lsn VARBINARY(10) NULL,
    updated_at DATETIME2(3) NOT NULL CONSTRAINT DF_cdc_state_updated_at DEFAULT SYSUTCDATETIME()
  );
END

IF OBJECT_ID('metadata.universe_compare_log') IS NULL
BEGIN
  CREATE TABLE metadata.universe_compare_log (
    id BIGINT IDENTITY(1,1) PRIMARY KEY,
    table_name NVARCHAR(128) NOT NULL,
    compare_ts DATETIME2(3) NOT NULL CONSTRAINT DF_universe_compare_ts DEFAULT SYSUTCDATETIME(),
    source_count BIGINT NULL,
    dw_count BIGINT NULL,
    source_checksum BIGINT NULL,
    dw_checksum BIGINT NULL,
    status NVARCHAR(20) NOT NULL
  );
END

-- source tables
IF OBJECT_ID('source.customers') IS NULL
BEGIN
  CREATE TABLE source.customers (
    customer_id INT NOT NULL PRIMARY KEY,
    name NVARCHAR(100) NOT NULL,
    city NVARCHAR(100) NOT NULL,
    updated_at DATETIME2(3) NOT NULL
  );
END

IF OBJECT_ID('source.orders') IS NULL
BEGIN
  CREATE TABLE source.orders (
    order_id INT NOT NULL PRIMARY KEY,
    customer_id INT NOT NULL,
    amount DECIMAL(10,2) NOT NULL,
    status NVARCHAR(20) NOT NULL,
    updated_at DATETIME2(3) NOT NULL
  );
END

-- staging tables (append-only)
IF OBJECT_ID('staging.customers') IS NULL
BEGIN
  CREATE TABLE staging.customers (
    customer_id INT NOT NULL,
    name NVARCHAR(100) NOT NULL,
    city NVARCHAR(100) NOT NULL,
    updated_at DATETIME2(3) NOT NULL,
    batch_id UNIQUEIDENTIFIER NOT NULL,
    load_ts DATETIME2(3) NOT NULL CONSTRAINT DF_stg_customers_load_ts DEFAULT SYSUTCDATETIME()
  );
  CREATE INDEX IX_stg_customers_batch ON staging.customers(batch_id);
END

IF OBJECT_ID('staging.orders') IS NULL
BEGIN
  CREATE TABLE staging.orders (
    order_id INT NOT NULL,
    customer_id INT NOT NULL,
    amount DECIMAL(10,2) NOT NULL,
    status NVARCHAR(20) NOT NULL,
    updated_at DATETIME2(3) NOT NULL,
    batch_id UNIQUEIDENTIFIER NOT NULL,
    load_ts DATETIME2(3) NOT NULL CONSTRAINT DF_stg_orders_load_ts DEFAULT SYSUTCDATETIME()
  );
  CREATE INDEX IX_stg_orders_batch ON staging.orders(batch_id);
END

-- dw tables
IF OBJECT_ID('dw.customers') IS NULL
BEGIN
  CREATE TABLE dw.customers (
    customer_id INT NOT NULL PRIMARY KEY,
    name NVARCHAR(100) NOT NULL,
    city NVARCHAR(100) NOT NULL,
    is_active BIT NOT NULL CONSTRAINT DF_dw_customers_is_active DEFAULT 1,
    updated_at DATETIME2(3) NOT NULL
  );
END

IF OBJECT_ID('dw.orders') IS NULL
BEGIN
  CREATE TABLE dw.orders (
    order_id INT NOT NULL PRIMARY KEY,
    customer_id INT NOT NULL,
    amount DECIMAL(10,2) NOT NULL,
    status NVARCHAR(20) NOT NULL,
    is_active BIT NOT NULL CONSTRAINT DF_dw_orders_is_active DEFAULT 1,
    updated_at DATETIME2(3) NOT NULL
  );
END


