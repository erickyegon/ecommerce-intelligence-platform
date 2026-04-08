-- 3-Month Revenue Forecast using Linear Extrapolation
-- Calculates trend from last 6 months and projects forward 3 months

WITH monthly_revenue AS (
  SELECT
    year_month,
    SUM(gross_revenue) as revenue,
    ROW_NUMBER() OVER (ORDER BY year_month) as month_index
  FROM workspace.mff_gold.monthly_channel_trends
  GROUP BY year_month
),
last_6_months AS (
  SELECT *
  FROM monthly_revenue
  WHERE year_month >= DATE_FORMAT(ADD_MONTHS((SELECT MAX(year_month) FROM monthly_revenue), -5), 'yyyy-MM')
),
trend_calc AS (
  SELECT
    COUNT(*) as n,
    SUM(month_index) as sum_x,
    SUM(revenue) as sum_y,
    SUM(month_index * revenue) as sum_xy,
    SUM(month_index * month_index) as sum_x2,
    (SELECT MAX(month_index) FROM last_6_months) as max_index,
    (SELECT MAX(year_month) FROM last_6_months) as last_month
  FROM last_6_months
),
slope_intercept AS (
  SELECT
    (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x * sum_x) as slope,
    (sum_y - ((n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x * sum_x)) * sum_x) / n as intercept,
    max_index,
    last_month
  FROM trend_calc
),
forecast AS (
  SELECT
    DATE_FORMAT(ADD_MONTHS(last_month, 1), 'yyyy-MM') as year_month,
    ROUND(intercept + slope * (max_index + 1), 2) as revenue,
    'Forecast' as type
  FROM slope_intercept
  UNION ALL
  SELECT
    DATE_FORMAT(ADD_MONTHS(last_month, 2), 'yyyy-MM') as year_month,
    ROUND(intercept + slope * (max_index + 2), 2) as revenue,
    'Forecast' as type
  FROM slope_intercept
  UNION ALL
  SELECT
    DATE_FORMAT(ADD_MONTHS(last_month, 3), 'yyyy-MM') as year_month,
    ROUND(intercept + slope * (max_index + 3), 2) as revenue,
    'Forecast' as type
  FROM slope_intercept
)
-- Combine historical and forecast data
SELECT year_month, revenue, 'Actual' as type
FROM monthly_revenue
UNION ALL
SELECT year_month, revenue, type
FROM forecast
ORDER BY year_month
