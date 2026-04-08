-- Fixed Cohort Retention Calculation
-- This query properly calculates repeat purchase retention rates within 60, 90, 180, and 365 days
-- Replace the existing cohort_retention dataset query with this

WITH first_orders AS (
  SELECT 
    u.user_id,
    DATE_FORMAT(u.first_order_ts, 'yyyy-MM') as acquisition_cohort,
    u.acquisition_channel,
    u.first_order_ts
  FROM workspace.mff_gold.user_order_summary u
  WHERE u.first_order_ts IS NOT NULL
),
repeat_purchases AS (
  SELECT 
    fo.user_id,
    fo.acquisition_cohort,
    fo.acquisition_channel,
    fo.first_order_ts,
    o.order_ts,
    DATEDIFF(o.order_ts, fo.first_order_ts) as days_since_first
  FROM first_orders fo
  INNER JOIN workspace.mff_silver.orders_enriched o ON fo.user_id = o.user_id
  WHERE DATEDIFF(o.order_ts, fo.first_order_ts) > 0  -- Only repeat purchases
),
cohort_stats AS (
  SELECT
    fo.acquisition_cohort,
    fo.acquisition_channel,
    COUNT(DISTINCT fo.user_id) as new_customers,
    COUNT(DISTINCT CASE WHEN rp.days_since_first <= 60 THEN rp.user_id END) as repeated_60d,
    COUNT(DISTINCT CASE WHEN rp.days_since_first <= 90 THEN rp.user_id END) as repeated_90d,
    COUNT(DISTINCT CASE WHEN rp.days_since_first <= 180 THEN rp.user_id END) as repeated_180d,
    COUNT(DISTINCT CASE WHEN rp.days_since_first <= 365 THEN rp.user_id END) as repeated_365d
  FROM first_orders fo
  LEFT JOIN repeat_purchases rp 
    ON fo.user_id = rp.user_id 
    AND fo.acquisition_cohort = rp.acquisition_cohort
    AND fo.acquisition_channel = rp.acquisition_channel
  GROUP BY fo.acquisition_cohort, fo.acquisition_channel
)
SELECT
  acquisition_cohort,
  acquisition_channel,
  new_customers,
  repeated_60d,
  repeated_90d,
  repeated_180d,
  repeated_365d,
  ROUND(100.0 * repeated_60d / NULLIF(new_customers, 0), 1) as retention_rate_60d,
  ROUND(100.0 * repeated_90d / NULLIF(new_customers, 0), 1) as retention_rate_90d,
  ROUND(100.0 * repeated_180d / NULLIF(new_customers, 0), 1) as retention_rate_180d,
  ROUND(100.0 * repeated_365d / NULLIF(new_customers, 0), 1) as retention_rate_365d
FROM cohort_stats
ORDER BY acquisition_cohort DESC, acquisition_channel
