-- Customer Segments Summary (KMeans K=4)
-- Source: workspace.mff_gold.customer_segments
-- KMeans clustering creates 4 customer segments with tier labels
-- Shows customers, avg LTV, avg orders per segment
-- Filterable by: customer_tier

SELECT
  customer_segment,
  customer_tier,
  COUNT(DISTINCT user_id) as customers,
  ROUND(AVG(gross_lifetime_value), 0) as avg_ltv,
  ROUND(AVG(total_orders), 1) as avg_orders
FROM workspace.mff_gold.customer_segments
GROUP BY customer_segment, customer_tier
ORDER BY customer_segment;
