-- Top Product by Revenue
-- Identifies the product generating the most total revenue
-- Used for: top_revenue_product counter widget

SELECT 
  primary_product_name,
  SUM(gross_revenue) as total_revenue
FROM workspace.mff_gold.product_monthly_performance
GROUP BY primary_product_name
ORDER BY total_revenue DESC
LIMIT 1;
