-- Level-0 is day8_src.Events (raw events)

-- Level-1: user-day aggregates
IF OBJECT_ID('day8_dwh.Events_L1_UserDay') IS NOT NULL DROP VIEW day8_dwh.Events_L1_UserDay;
GO
CREATE VIEW day8_dwh.Events_L1_UserDay AS
SELECT
  UserID,
  CAST(EventTime AS DATE) AS EventDate,
  COUNT(*) AS NumEvents,
  COUNT(DISTINCT EventName) AS NumEventTypes
FROM day8_src.Events
GROUP BY UserID, CAST(EventTime AS DATE);
GO

-- Level-2: user-week aggregates (ISO week)
IF OBJECT_ID('day8_dwh.Events_L2_UserWeek') IS NOT NULL DROP VIEW day8_dwh.Events_L2_UserWeek;
GO
CREATE VIEW day8_dwh.Events_L2_UserWeek AS
SELECT
  UserID,
  DATEADD(day, -((DATEPART(weekday, CAST(EventTime AS DATE)) + @@DATEFIRST - 2) % 7), CAST(EventTime AS DATE)) AS WeekStart,
  COUNT(*) AS NumEvents,
  COUNT(DISTINCT EventName) AS NumEventTypes
FROM day8_src.Events
GROUP BY UserID,
  DATEADD(day, -((DATEPART(weekday, CAST(EventTime AS DATE)) + @@DATEFIRST - 2) % 7), CAST(EventTime AS DATE));
GO
