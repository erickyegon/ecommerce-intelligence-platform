-- gsearch Nonbrand CVR Aggregation
-- Calculates CVR specifically for paid_search_gsearch_nonbrand channel
-- Used for: gsearch CVR counter widget

SELECT 
  SUM(total_orders) * 100.0 / SUM(total_sessions) AS cvr_pct
FROM workspace.mff_gold.channel_conversion_rates
WHERE marketing_channel = 'paid_search_gsearch_nonbrand';
