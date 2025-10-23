-- Clubs (Vertical MIOD)
SELECT TOP 10 * FROM day8_dwh.Fact_Clubs;
SELECT TOP 10 * FROM day8_dwh.Fact_Clubs_L1;
SELECT TOP 10 * FROM day8_dwh.Fact_Clubs_L2;

-- Property (Horizontal MIOD)
SELECT TOP 10 * FROM day8_dwh.Fact_Property;
SELECT TOP 10 * FROM day8_dwh.Fact_Property_L1 ORDER BY [Month], Suburb;
SELECT TOP 10 * FROM day8_dwh.Fact_Property_L2 ORDER BY [Month];

-- Auction clearance rate by suburb-month
SELECT Suburb, [Month], AuctionClearanceRate
FROM day8_dwh.Fact_Property_L1
ORDER BY [Month] DESC, Suburb;

-- Events (Granularity)
-- DAU (distinct active users per day)
SELECT EventDate, COUNT(*) AS DAU
FROM day8_dwh.Events_L1_UserDay
GROUP BY EventDate
ORDER BY EventDate DESC;

-- Weekly active users per user (example aggregation)
SELECT WeekStart, COUNT(DISTINCT UserID) AS WAU
FROM day8_dwh.Events_L2_UserWeek
GROUP BY WeekStart
ORDER BY WeekStart DESC;
