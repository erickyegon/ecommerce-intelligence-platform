-- Product Monthly Performance
-- Source: workspace.mff_gold.product_monthly_performance
-- Time series showing each product's orders, revenue, margin, refunds by month
-- Filterable by: year_month, primary_product_name

SELECT 
  year_month,
  primary_product_name,
  orders,
  gross_revenue,
  avg_net_margin_pct,
  refund_rate_pct
FROM workspace.mff_gold.product_monthly_performance;
