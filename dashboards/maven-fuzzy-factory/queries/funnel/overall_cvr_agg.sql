-- Overall CVR Aggregation
-- Calculates total conversion rate across all marketing channels and devices
-- Used for: Overall CVR counter widget

SELECT 
  SUM(total_orders) * 100.0 / SUM(total_sessions) AS cvr_pct
FROM workspace.mff_gold.channel_conversion_rates;
