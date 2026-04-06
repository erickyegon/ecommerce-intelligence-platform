-- Best Cross-Sell Pair
-- Identifies product pair with highest attach rate
-- Used for: best_cross_sell counter widget

SELECT 
  primary_product_name,
  cross_sell_product_name,
  attach_rate_pct
FROM workspace.mff_gold.cross_sell_matrix
ORDER BY attach_rate_pct DESC
LIMIT 1;
