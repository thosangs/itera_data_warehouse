SET NOCOUNT ON;

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'source') EXEC('CREATE SCHEMA source');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'staging') EXEC('CREATE SCHEMA staging');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'dw') EXEC('CREATE SCHEMA dw');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'metadata') EXEC('CREATE SCHEMA metadata');


