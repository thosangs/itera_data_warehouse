SET NOCOUNT ON;

-- Enable CDC at database level if not already enabled
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = DB_NAME() AND is_cdc_enabled = 0)
BEGIN
  EXEC sys.sp_cdc_enable_db;
END

-- Enable CDC for source tables
IF OBJECT_ID('source.customers') IS NOT NULL
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM cdc.change_tables WHERE source_object_id = OBJECT_ID('source.customers')
  )
  BEGIN
    EXEC sys.sp_cdc_enable_table
      @source_schema = N'source',
      @source_name = N'customers',
      @role_name = NULL,
      @supports_net_changes = 1;
  END
END

IF OBJECT_ID('source.orders') IS NOT NULL
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM cdc.change_tables WHERE source_object_id = OBJECT_ID('source.orders')
  )
  BEGIN
    EXEC sys.sp_cdc_enable_table
      @source_schema = N'source',
      @source_name = N'orders',
      @role_name = NULL,
      @supports_net_changes = 1;
  END
END


