-- KPIs with Month-over-Month Trends
-- Calculates current vs previous month revenue and MoM percentage change
-- Used for: Dynamic revenue trend indicator on Executive page

WITH latest_month AS (
  SELECT MAX(year_month) as current_month
  FROM workspace.mff_gold.monthly_channel_trends
),
current_metrics AS (
  SELECT 
    SUM(gross_revenue) as current_revenue,
    SUM(orders) as current_orders
  FROM workspace.mff_gold.monthly_channel_trends
  WHERE year_month = (SELECT current_month FROM latest_month)
),
prev_metrics AS (
  SELECT 
    SUM(gross_revenue) as prev_revenue,
    SUM(orders) as prev_orders
  FROM workspace.mff_gold.monthly_channel_trends
  WHERE year_month = DATE_FORMAT(
    DATE_ADD(
      TO_DATE(CONCAT((SELECT current_month FROM latest_month), '-01')),
      -1
    ),
    'yyyy-MM'
  )
)
SELECT 
  e.total_orders,
  e.total_gross_revenue,
  e.avg_net_margin_pct,
  e.overall_refund_rate_pct,
  e.cross_sell_rate_pct,
  c.current_revenue as month_revenue,
  p.prev_revenue as prev_month_revenue,
  ROUND(100.0 * (c.current_revenue - p.prev_revenue) / p.prev_revenue, 1) as revenue_mom_pct
FROM workspace.mff_gold.executive_kpis e 
CROSS JOIN current_metrics c 
CROSS JOIN prev_metrics p;
