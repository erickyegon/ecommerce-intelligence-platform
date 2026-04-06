-- Newest Product Launch
-- Identifies most recently launched product by earliest month appearance
-- Used for: newest_product text widget

SELECT 
  primary_product_name,
  MIN(year_month) as launch_date
FROM workspace.mff_gold.product_monthly_performance
GROUP BY primary_product_name
ORDER BY launch_date DESC
LIMIT 1;
