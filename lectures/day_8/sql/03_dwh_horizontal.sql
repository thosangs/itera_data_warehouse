-- Level-0: base source tables exist in day8_src.*
-- Integrated fact by horizontal combination on (Suburb, Month)
IF OBJECT_ID('day8_dwh.Fact_Property') IS NOT NULL DROP VIEW day8_dwh.Fact_Property;
GO
CREATE VIEW day8_dwh.Fact_Property AS
SELECT
  COALESCE(pi.Suburb, pa.Suburb, ps.Suburb) AS Suburb,
  COALESCE(pi.[Month], pa.[Month], ps.[Month]) AS [Month],
  ISNULL(pi.NumInspections, 0) AS NumInspections,
  ISNULL(pa.NumAuctions, 0) AS NumAuctions,
  ISNULL(pa.NumSuccessfulAuctions, 0) AS NumSuccessfulAuctions,
  ISNULL(ps.NumNewListings, 0) AS NumNewListings,
  ISNULL(ps.NumSold, 0) AS NumSold,
  ISNULL(ps.TotalSoldPrice, 0) AS TotalSoldPrice
FROM day8_src.Property_Inspection pi
FULL OUTER JOIN day8_src.Property_Auction pa
  ON pi.Suburb = pa.Suburb AND pi.[Month] = pa.[Month]
FULL OUTER JOIN day8_src.Property_Sales ps
  ON COALESCE(pi.Suburb, pa.Suburb) = ps.Suburb AND COALESCE(pi.[Month], pa.[Month]) = ps.[Month];
GO

-- Level-1: add derived measures (clearance rate, avg sold price)
IF OBJECT_ID('day8_dwh.Fact_Property_L1') IS NOT NULL DROP VIEW day8_dwh.Fact_Property_L1;
GO
CREATE VIEW day8_dwh.Fact_Property_L1 AS
SELECT
  Suburb,
  [Month],
  NumInspections,
  NumAuctions,
  NumSuccessfulAuctions,
  NumNewListings,
  NumSold,
  TotalSoldPrice,
  CASE WHEN NumAuctions > 0 THEN CAST(NumSuccessfulAuctions AS DECIMAL(10,4)) / NumAuctions ELSE NULL END AS AuctionClearanceRate,
  CASE WHEN NumSold > 0 THEN CAST(TotalSoldPrice AS DECIMAL(14,2)) / NumSold ELSE NULL END AS AvgSoldPrice
FROM day8_dwh.Fact_Property;
GO

-- Level-2: aggregate to Month only (across suburbs)
IF OBJECT_ID('day8_dwh.Fact_Property_L2') IS NOT NULL DROP VIEW day8_dwh.Fact_Property_L2;
GO
CREATE VIEW day8_dwh.Fact_Property_L2 AS
SELECT
  [Month],
  SUM(NumInspections) AS NumInspections,
  SUM(NumAuctions) AS NumAuctions,
  SUM(NumSuccessfulAuctions) AS NumSuccessfulAuctions,
  SUM(NumNewListings) AS NumNewListings,
  SUM(NumSold) AS NumSold,
  SUM(TotalSoldPrice) AS TotalSoldPrice
FROM day8_dwh.Fact_Property_L1
GROUP BY [Month];
GO
