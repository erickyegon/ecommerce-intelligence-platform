-- bsearch CVR Aggregation
-- Calculates CVR for paid_search_bsearch channels (brand + nonbrand combined)
-- Used for: bsearch CVR counter widget

SELECT 
  SUM(total_orders) * 100.0 / SUM(total_sessions) AS cvr_pct
FROM workspace.mff_gold.channel_conversion_rates
WHERE marketing_channel IN ('paid_search_bsearch_nonbrand', 'paid_search_bsearch_brand');
