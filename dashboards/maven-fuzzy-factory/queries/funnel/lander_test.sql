-- Landing Page A/B Test Results
-- Source: workspace.mff_gold.ab_lander_test_results
-- Compares session-to-order conversion rates for different landing page variants
-- Shows which lander design drives highest conversion

SELECT 
  landing_variant,
  sessions,
  orders,
  session_to_order_cvr_pct
FROM workspace.mff_gold.ab_lander_test_results
ORDER BY session_to_order_cvr_pct DESC;
