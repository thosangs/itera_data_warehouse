-- Level-0: treat each club table as already aggregated per year
-- Integrated fact by vertical stacking
IF OBJECT_ID('day8_dwh.Fact_Clubs') IS NOT NULL DROP VIEW day8_dwh.Fact_Clubs;
GO
CREATE VIEW day8_dwh.Fact_Clubs AS
SELECT 1 AS ClubID, ClubName, [Year], TotalIncome, TotalExpenditure FROM day8_src.Clubs_Orchestra
UNION ALL
SELECT 2 AS ClubID, ClubName, [Year], TotalIncome, TotalExpenditure FROM day8_src.Clubs_Business
UNION ALL
SELECT 3 AS ClubID, ClubName, [Year], TotalIncome, TotalExpenditure FROM day8_src.Clubs_Japanese;
GO

-- Level-1: add calculated measures and keep Club, Year as dims
IF OBJECT_ID('day8_dwh.Fact_Clubs_L1') IS NOT NULL DROP VIEW day8_dwh.Fact_Clubs_L1;
GO
CREATE VIEW day8_dwh.Fact_Clubs_L1 AS
SELECT
  ClubID,
  ClubName,
  [Year],
  TotalIncome,
  TotalExpenditure,
  (TotalIncome - TotalExpenditure) AS NetIncome
FROM day8_dwh.Fact_Clubs;
GO

-- Level-2: aggregate to Year only (summed across clubs)
IF OBJECT_ID('day8_dwh.Fact_Clubs_L2') IS NOT NULL DROP VIEW day8_dwh.Fact_Clubs_L2;
GO
CREATE VIEW day8_dwh.Fact_Clubs_L2 AS
SELECT
  [Year],
  SUM(TotalIncome) AS TotalIncome,
  SUM(TotalExpenditure) AS TotalExpenditure,
  SUM(TotalIncome - TotalExpenditure) AS NetIncome
FROM day8_dwh.Fact_Clubs
GROUP BY [Year];
GO
