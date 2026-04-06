-- Aggregated Totals Summary
-- Provides overall metrics for counter widgets (avoids expression truncation)
-- Aggregates: total_sessions, total_orders, total_revenue across all channels

SELECT
  SUM(sessions) as total_sessions,
  SUM(orders) as total_orders,
  SUM(gross_revenue) as total_revenue
FROM workspace.mff_gold.monthly_channel_trends;
