-- Funnel Biggest Drop-off Analysis
-- Identifies the single biggest drop in the conversion funnel and calculates revenue leakage

WITH funnel_data AS (
  SELECT
    channel_group,
    device_type,
    sessions_entered,
    reached_products,
    reached_product_page,
    reached_cart,
    reached_shipping,
    reached_billing,
    completed_order
  FROM workspace.mff_gold.funnel_step_analysis
  WHERE channel_group = 'paid_search' AND device_type = 'desktop'
),
step_dropoffs AS (
  SELECT
    'Products to Product Page' as step,
    reached_products - reached_product_page as sessions_lost,
    1 as step_order
  FROM funnel_data
  UNION ALL
  SELECT
    'Product Page to Cart' as step,
    reached_product_page - reached_cart as sessions_lost,
    2 as step_order
  FROM funnel_data
  UNION ALL
  SELECT
    'Cart to Shipping' as step,
    reached_cart - reached_shipping as sessions_lost,
    3 as step_order
  FROM funnel_data
  UNION ALL
  SELECT
    'Shipping to Billing' as step,
    reached_shipping - reached_billing as sessions_lost,
    4 as step_order
  FROM funnel_data
  UNION ALL
  SELECT
    'Billing to Order' as step,
    reached_billing - completed_order as sessions_lost,
    5 as step_order
  FROM funnel_data
),
avg_order_value AS (
  SELECT
    SUM(gross_revenue) / NULLIF(SUM(orders), 0) as aov
  FROM workspace.mff_gold.monthly_channel_trends
)
SELECT
  d.step,
  d.sessions_lost,
  ROUND(a.aov, 2) as avg_order_value,
  ROUND(d.sessions_lost * a.aov * 0.05, 2) as estimated_revenue_leakage,  -- Assuming 5% would have converted
  CONCAT('Lost ', CAST(d.sessions_lost AS STRING), ' sessions at ', d.step, ' → ~$', 
         CAST(ROUND(d.sessions_lost * a.aov * 0.05, 0) AS STRING), ' revenue leakage') as callout_text
FROM step_dropoffs d
CROSS JOIN avg_order_value a
ORDER BY d.sessions_lost DESC
LIMIT 1
