-- Channel Efficiency Table
-- Shows revenue per session and orders per session for every channel-device combination
-- Sorted by revenue per session descending

SELECT
  marketing_channel,
  device_type,
  SUM(sessions) as total_sessions,
  SUM(orders) as total_orders,
  SUM(gross_revenue) as total_revenue,
  ROUND(SUM(gross_revenue) / NULLIF(SUM(sessions), 0), 2) as revenue_per_session,
  ROUND(SUM(orders) * 1.0 / NULLIF(SUM(sessions), 0), 4) as orders_per_session,
  ROUND(SUM(orders) * 100.0 / NULLIF(SUM(sessions), 0), 1) as cvr_pct,
  ROUND(SUM(gross_revenue) / NULLIF(SUM(orders), 0), 2) as avg_order_value,
  CONCAT(marketing_channel, ' × ', device_type) as channel_device_combo
FROM workspace.mff_gold.monthly_channel_trends
GROUP BY marketing_channel, device_type
ORDER BY revenue_per_session DESC
