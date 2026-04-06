-- Lifetime Value by Acquisition Channel
-- Source: workspace.mff_gold.ltv_by_acquisition_channel
-- Shows avg_gross_ltv, avg_orders, customers per acquisition channel

SELECT 
  acquisition_channel,
  customers,
  avg_orders,
  ROUND(avg_gross_ltv, 0) as avg_gross_ltv
FROM workspace.mff_gold.ltv_by_acquisition_channel
ORDER BY avg_gross_ltv DESC;
