-- Monthly Channel Trends
-- Source: workspace.mff_gold.monthly_channel_trends
-- Time series data showing revenue, orders, sessions, CVR by channel/device/month
-- Filterable by: year_month, marketing_channel, device_type

-- CUSTOM CALCULATION: Channel Display (user-friendly channel names)
-- CASE
--   WHEN marketing_channel = 'paid_search_gsearch_nonbrand' THEN 'Paid Search - gsearch'
--   WHEN marketing_channel = 'paid_search_gsearch_brand' THEN 'Paid Search - Brand'
--   WHEN marketing_channel = 'paid_social_bsearch' THEN 'Paid Social - bsearch'
--   WHEN marketing_channel = 'direct' THEN 'Direct'
--   WHEN marketing_channel = 'organic_search' THEN 'Organic Search'
--   ELSE REPLACE(REPLACE(marketing_channel, '_', ' '), 'paid search', 'Paid Search')
-- END

SELECT 
  year_month,
  marketing_channel,
  device_type,
  sessions,
  orders,
  gross_revenue,
  cvr_pct
FROM workspace.mff_gold.monthly_channel_trends;
