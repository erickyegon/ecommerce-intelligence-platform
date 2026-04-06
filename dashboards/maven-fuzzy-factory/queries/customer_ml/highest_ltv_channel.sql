-- Highest LTV Channel
-- Identifies acquisition channel with highest average gross LTV
-- Used for: highest_ltv_channel counter widget

SELECT 
  acquisition_channel,
  ROUND(avg_gross_ltv, 0) as highest_ltv
FROM workspace.mff_gold.ltv_by_acquisition_channel
ORDER BY avg_gross_ltv DESC
LIMIT 1;
