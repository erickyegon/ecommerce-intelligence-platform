-- Channel Conversion Rates
-- Source: workspace.mff_gold.channel_conversion_rates
-- CVR breakdown by marketing_channel and device_type
-- Filterable by: marketing_channel, device_type

-- CUSTOM CALCULATION: Aggregated CVR (for filtered contexts)
-- SUM(total_orders) * 100.0 / SUM(total_sessions)

SELECT 
  marketing_channel,
  device_type,
  total_sessions,
  total_orders,
  cvr_pct
FROM workspace.mff_gold.channel_conversion_rates;
