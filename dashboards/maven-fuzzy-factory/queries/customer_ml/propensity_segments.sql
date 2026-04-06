-- ML Propensity Segments Distribution
-- Source: workspace.mff_gold.session_conversion_predictions
-- Groups sessions into HIGH/MEDIUM/LOW conversion probability segments
-- Shows session count, avg probability, and % of total sessions per segment

SELECT
  propensity_segment,
  COUNT(*) as sessions,
  ROUND(AVG(conversion_prob), 3) as avg_prob,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) as pct_sessions
FROM workspace.mff_gold.session_conversion_predictions
WHERE propensity_segment IS NOT NULL
GROUP BY propensity_segment
ORDER BY avg_prob DESC;
