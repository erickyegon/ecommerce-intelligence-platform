-- Revenue Inflection Points (>20% Change)
-- Identifies months where revenue changed by more than 20% MoM
-- Use these as annotations on the revenue trend chart

WITH monthly_revenue AS (
  SELECT
    year_month,
    SUM(gross_revenue) as revenue
  FROM workspace.mff_gold.monthly_channel_trends
  GROUP BY year_month
),
revenue_with_change AS (
  SELECT
    year_month,
    revenue,
    LAG(revenue, 1) OVER (ORDER BY year_month) as prev_month_revenue,
    ROUND(100.0 * (revenue - LAG(revenue, 1) OVER (ORDER BY year_month)) / NULLIF(LAG(revenue, 1) OVER (ORDER BY year_month), 0), 1) as pct_change
  FROM monthly_revenue
)
SELECT
  year_month,
  revenue,
  prev_month_revenue,
  pct_change,
  CASE 
    WHEN pct_change > 20 THEN CONCAT('↑ Revenue up ', CAST(pct_change AS STRING), '%')
    WHEN pct_change < -20 THEN CONCAT('↓ Revenue down ', CAST(ABS(pct_change) AS STRING), '%')
  END as annotation_label,
  CASE 
    WHEN pct_change > 20 THEN 'Increase'
    WHEN pct_change < -20 THEN 'Decrease'
  END as direction
FROM revenue_with_change
WHERE ABS(pct_change) > 20
ORDER BY year_month
