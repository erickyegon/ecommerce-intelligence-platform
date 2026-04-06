-- Cohort Retention Analysis
-- Source: workspace.mff_gold.acquisition_cohorts
-- Shows retention rates at 60, 90, 180, and 365 days for each monthly cohort
-- Filterable by: acquisition_cohort (year-month)

SELECT 
  acquisition_cohort,
  customers,
  retention_rate_60d,
  retention_rate_90d,
  retention_rate_180d,
  retention_rate_365d
FROM workspace.mff_gold.acquisition_cohorts
ORDER BY acquisition_cohort DESC;
