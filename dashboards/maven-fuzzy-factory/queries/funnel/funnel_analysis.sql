-- Conversion Funnel Step Analysis
-- Source: workspace.mff_gold.funnel_step_analysis
-- 7-step funnel: Sessions → Products → Product Page → Cart → Shipping → Billing → Order
-- Shows absolute sessions remaining and % remaining at each step
-- Filterable by: channel_group, device_type

SELECT 
  channel_group,
  device_type,
  sessions_entered,
  reached_products,
  pct_to_products,
  reached_product_page,
  pct_to_product_page,
  reached_cart,
  pct_to_cart,
  reached_shipping,
  pct_to_shipping,
  reached_billing,
  pct_to_billing,
  reached_thankyou,
  pct_to_order
FROM workspace.mff_gold.funnel_step_analysis;
