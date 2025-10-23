-- Adjust base path if you mount differently
DECLARE @base NVARCHAR(1024) = N'/opt/workspace/lectures/day_8/data';
DECLARE @sql  NVARCHAR(MAX);
DECLARE @file NVARCHAR(4000);

TRUNCATE TABLE day8_src.Clubs_Orchestra;
SET @file = @base + N'/clubs_orchestra.csv';
SET @sql = N'BULK INSERT day8_src.Clubs_Orchestra FROM ''' + REPLACE(@file, '''', '''''') + N''' WITH (FIRSTROW=2, FIELDTERMINATOR='','', ROWTERMINATOR=''\n'');';
EXEC sys.sp_executesql @sql;

TRUNCATE TABLE day8_src.Clubs_Business;
SET @file = @base + N'/clubs_business.csv';
SET @sql = N'BULK INSERT day8_src.Clubs_Business FROM ''' + REPLACE(@file, '''', '''''') + N''' WITH (FIRSTROW=2, FIELDTERMINATOR='','', ROWTERMINATOR=''\n'');';
EXEC sys.sp_executesql @sql;

TRUNCATE TABLE day8_src.Clubs_Japanese;
SET @file = @base + N'/clubs_japanese.csv';
SET @sql = N'BULK INSERT day8_src.Clubs_Japanese FROM ''' + REPLACE(@file, '''', '''''') + N''' WITH (FIRSTROW=2, FIELDTERMINATOR='','', ROWTERMINATOR=''\n'');';
EXEC sys.sp_executesql @sql;

TRUNCATE TABLE day8_src.Property_Inspection;
SET @file = @base + N'/property_inspection.csv';
SET @sql = N'BULK INSERT day8_src.Property_Inspection FROM ''' + REPLACE(@file, '''', '''''') + N''' WITH (FIRSTROW=2, FIELDTERMINATOR='','', ROWTERMINATOR=''\n'');';
EXEC sys.sp_executesql @sql;

TRUNCATE TABLE day8_src.Property_Auction;
SET @file = @base + N'/property_auction.csv';
SET @sql = N'BULK INSERT day8_src.Property_Auction FROM ''' + REPLACE(@file, '''', '''''') + N''' WITH (FIRSTROW=2, FIELDTERMINATOR='','', ROWTERMINATOR=''\n'');';
EXEC sys.sp_executesql @sql;

TRUNCATE TABLE day8_src.Property_Sales;
SET @file = @base + N'/property_sales.csv';
SET @sql = N'BULK INSERT day8_src.Property_Sales FROM ''' + REPLACE(@file, '''', '''''') + N''' WITH (FIRSTROW=2, FIELDTERMINATOR='','', ROWTERMINATOR=''\n'');';
EXEC sys.sp_executesql @sql;

TRUNCATE TABLE day8_src.Events;
SET @file = @base + N'/events/events.csv';
SET @sql = N'BULK INSERT day8_src.Events FROM ''' + REPLACE(@file, '''', '''''') + N''' WITH (FIRSTROW=2, FIELDTERMINATOR='','', ROWTERMINATOR=''\n'');';
EXEC sys.sp_executesql @sql;
