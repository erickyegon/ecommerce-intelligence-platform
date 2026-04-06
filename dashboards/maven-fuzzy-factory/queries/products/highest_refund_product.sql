-- Highest Refund Rate Product
-- Identifies product with worst average refund rate
-- Used for: highest_refund counter widget (conditional formatting: red if ≥5%)

SELECT 
  product_name,
  ROUND(AVG(item_refund_rate_pct), 1) as avg_refund_rate
FROM workspace.mff_gold.refund_analysis
GROUP BY product_name
ORDER BY avg_refund_rate DESC
LIMIT 1;
