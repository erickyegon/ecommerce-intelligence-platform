-- Product P&L Summary
-- Aggregates all products' performance: orders, revenue, margin, refunds
-- Used for: product_pl_table widget

SELECT
  primary_product_name,
  SUM(orders) as total_orders,
  SUM(gross_revenue) as total_revenue,
  ROUND(AVG(avg_net_margin_pct), 1) as avg_margin_pct,
  ROUND(AVG(refund_rate_pct), 1) as avg_refund_pct
FROM workspace.mff_gold.product_monthly_performance
GROUP BY primary_product_name
ORDER BY total_revenue DESC;
