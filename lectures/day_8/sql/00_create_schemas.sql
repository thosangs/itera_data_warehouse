IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'day8_src') EXEC('CREATE SCHEMA day8_src');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'day8_dwh') EXEC('CREATE SCHEMA day8_dwh');
GO

-- Clubs (Vertical MIOD) - operational/source tables
IF OBJECT_ID('day8_src.Clubs_Orchestra') IS NOT NULL DROP TABLE day8_src.Clubs_Orchestra;
CREATE TABLE day8_src.Clubs_Orchestra (
  ClubName NVARCHAR(100),
  [Year] INT,
  TotalIncome DECIMAL(12,2),
  TotalExpenditure DECIMAL(12,2)
);

IF OBJECT_ID('day8_src.Clubs_Business') IS NOT NULL DROP TABLE day8_src.Clubs_Business;
CREATE TABLE day8_src.Clubs_Business (
  ClubName NVARCHAR(100),
  [Year] INT,
  TotalIncome DECIMAL(12,2),
  TotalExpenditure DECIMAL(12,2)
);

IF OBJECT_ID('day8_src.Clubs_Japanese') IS NOT NULL DROP TABLE day8_src.Clubs_Japanese;
CREATE TABLE day8_src.Clubs_Japanese (
  ClubName NVARCHAR(100),
  [Year] INT,
  TotalIncome DECIMAL(12,2),
  TotalExpenditure DECIMAL(12,2)
);
GO

-- Property (Horizontal MIOD) - operational/source tables
IF OBJECT_ID('day8_src.Property_Inspection') IS NOT NULL DROP TABLE day8_src.Property_Inspection;
CREATE TABLE day8_src.Property_Inspection (
  Suburb NVARCHAR(100),
  [Month] DATE,
  NumInspections INT
);

IF OBJECT_ID('day8_src.Property_Auction') IS NOT NULL DROP TABLE day8_src.Property_Auction;
CREATE TABLE day8_src.Property_Auction (
  Suburb NVARCHAR(100),
  [Month] DATE,
  NumAuctions INT,
  NumSuccessfulAuctions INT
);

IF OBJECT_ID('day8_src.Property_Sales') IS NOT NULL DROP TABLE day8_src.Property_Sales;
CREATE TABLE day8_src.Property_Sales (
  Suburb NVARCHAR(100),
  [Month] DATE,
  NumNewListings INT,
  NumSold INT,
  TotalSoldPrice DECIMAL(14,2)
);
GO

-- Events (Granularity) - Level-0 base (raw events)
IF OBJECT_ID('day8_src.Events') IS NOT NULL DROP TABLE day8_src.Events;
CREATE TABLE day8_src.Events (
  EventID BIGINT IDENTITY(1,1) PRIMARY KEY,
  UserID INT,
  EventTime DATETIME2,
  EventName NVARCHAR(50),
  Device NVARCHAR(20),
  Country NVARCHAR(50),
  AppVersion NVARCHAR(20)
);
GO
