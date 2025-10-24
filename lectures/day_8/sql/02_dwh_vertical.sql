-- Level-0: treat each club table as already aggregated per year
-- Integrated fact by vertical stacking
IF OBJECT_ID('day8_dwh.Fact_Clubs') IS NOT NULL DROP VIEW day8_dwh.Fact_Clubs;
GO
CREATE VIEW day8_dwh.Fact_Clubs AS
SELECT 1 AS ClubID, ClubName, [Year], [Month], [Date], TotalIncome, TotalExpenditure FROM day8_src.Clubs_Orchestra
UNION ALL
SELECT 2 AS ClubID, ClubName, [Year], [Month], [Date], TotalIncome, TotalExpenditure FROM day8_src.Clubs_Business
UNION ALL
SELECT 3 AS ClubID, ClubName, [Year], [Month], [Date], TotalIncome, TotalExpenditure FROM day8_src.Clubs_Japanese;
GO

-- Level-1: aggregate by Year,Month,Date and derive NetIncome
IF OBJECT_ID('day8_dwh.Fact_Clubs_L1') IS NOT NULL DROP VIEW day8_dwh.Fact_Clubs_L1;
GO
CREATE VIEW day8_dwh.Fact_Clubs_L1 AS
SELECT
  [Year],
  [Month],
  [Date],
  SUM(TotalIncome) AS TotalIncome,
  SUM(TotalExpenditure) AS TotalExpenditure,
  (SUM(TotalIncome) - SUM(TotalExpenditure)) AS NetIncome
FROM day8_dwh.Fact_Clubs
GROUP BY [Year], [Month], [Date];
GO

-- Level-2: aggregate by Year (single dimension) and derive NetIncome
IF OBJECT_ID('day8_dwh.Fact_Clubs_L2') IS NOT NULL DROP VIEW day8_dwh.Fact_Clubs_L2;
GO
CREATE VIEW day8_dwh.Fact_Clubs_L2 AS
SELECT
  [Year],
  SUM(TotalIncome) AS TotalIncome,
  SUM(TotalExpenditure) AS TotalExpenditure,
  SUM(NetIncome) AS NetIncome
FROM day8_dwh.Fact_Clubs_L1
GROUP BY [Year];
GO
