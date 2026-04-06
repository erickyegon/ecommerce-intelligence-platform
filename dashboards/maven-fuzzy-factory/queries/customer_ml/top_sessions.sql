-- Top High-Propensity Sessions
-- Source: workspace.mff_gold.session_conversion_predictions
-- Shows top 3 sessions with highest predicted conversion probability
-- Used to identify characteristics of high-converting sessions
-- Filterable by: marketing_channel, device_type

SELECT
  website_session_id,
  marketing_channel,
  device_type,
  ROUND(conversion_prob, 3) as conv_prob
FROM workspace.mff_gold.session_conversion_predictions
WHERE propensity_segment = 'HIGH'
ORDER BY conversion_prob DESC
LIMIT 3;
