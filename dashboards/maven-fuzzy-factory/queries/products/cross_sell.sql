-- Cross-Sell Matrix
-- Source: workspace.mff_gold.cross_sell_matrix
-- Shows attach rates for product pairs (which products are bought together)
-- Columns: primary_product_name, cross_sell_product_name, primary_orders, cross_sell_orders, attach_rate_pct

SELECT 
  primary_product_name,
  cross_sell_product_name,
  primary_orders,
  cross_sell_orders,
  attach_rate_pct
FROM workspace.mff_gold.cross_sell_matrix
ORDER BY attach_rate_pct DESC;
