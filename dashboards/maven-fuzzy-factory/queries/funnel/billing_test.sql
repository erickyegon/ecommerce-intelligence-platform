-- Billing A/B Test Results
-- Source: workspace.mff_gold.billing_ab_test_results
-- Compares conversion rates from billing page to order completion
-- Shows sessions, orders, and CVR for /billing vs /billing-2 variants
-- Filterable by: device_type

SELECT 
  billing_variant,
  device_type,
  sessions,
  orders,
  billing_to_order_cvr_pct
FROM workspace.mff_gold.billing_ab_test_results;
