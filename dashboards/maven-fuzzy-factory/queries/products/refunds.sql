-- Refund Analysis
-- Source: workspace.mff_gold.refund_analysis
-- Monthly refund rates and counts by product
-- Filterable by: year_month, product_name

SELECT 
  year_month,
  product_name,
  items_sold,
  items_refunded,
  item_refund_rate_pct,
  refund_value,
  refund_value_pct
FROM workspace.mff_gold.refund_analysis;
