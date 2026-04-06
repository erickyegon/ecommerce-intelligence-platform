-- 180-Day Retention Rate
-- Calculates average retention rate at 180 days across all acquisition cohorts
-- Used for: retention_180d counter widget

SELECT 
  ROUND(AVG(retention_rate_180d), 1) as retention_180d
FROM workspace.mff_gold.acquisition_cohorts;
