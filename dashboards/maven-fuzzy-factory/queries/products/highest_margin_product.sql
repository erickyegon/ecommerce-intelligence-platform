-- Highest Margin Product
-- Identifies product with best average net margin percentage
-- Used for: highest_margin counter widget

SELECT 
  primary_product_name,
  ROUND(AVG(avg_net_margin_pct), 1) as avg_margin
FROM workspace.mff_gold.product_monthly_performance
GROUP BY primary_product_name
ORDER BY avg_margin DESC
LIMIT 1;
