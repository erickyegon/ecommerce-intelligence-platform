-- Funnel Steps Unpivoted
-- Uses stack() to transform funnel columns into rows for visualization
-- Creates 7 rows (one per funnel step) with sessions_remaining and pct_remaining
-- Filtered to: channel_group = 'Paid Search', device_type = 'desktop'

SELECT
  channel_group, 
  device_type,
  funnel_step, 
  sessions_remaining, 
  pct_remaining, 
  step_order
FROM (
  SELECT
    channel_group, 
    device_type,
    stack(
      7,
      'Sessions entered', CAST(sessions_entered AS DOUBLE), CAST(100.0 AS DOUBLE), 1,
      'Reached products', CAST(reached_products AS DOUBLE), CAST(pct_to_products AS DOUBLE), 2,
      'Viewed product page', CAST(reached_product_page AS DOUBLE), CAST(pct_to_product_page AS DOUBLE), 3,
      'Added to cart', CAST(reached_cart AS DOUBLE), CAST(pct_to_cart AS DOUBLE), 4,
      'Reached shipping', CAST(reached_shipping AS DOUBLE), CAST(pct_to_shipping AS DOUBLE), 5,
      'Reached billing', CAST(reached_billing AS DOUBLE), CAST(pct_to_billing AS DOUBLE), 6,
      'Placed order', CAST(reached_thankyou AS DOUBLE), CAST(pct_to_order AS DOUBLE), 7
    ) AS (funnel_step, sessions_remaining, pct_remaining, step_order)
  FROM workspace.mff_gold.funnel_step_analysis
  WHERE channel_group = 'Paid Search' AND device_type = 'desktop'
)
ORDER BY step_order;
