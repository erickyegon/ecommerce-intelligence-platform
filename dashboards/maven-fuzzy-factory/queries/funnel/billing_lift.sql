-- Billing-2 Lift Calculation
-- Calculates the CVR improvement (in percentage points) from /billing-2 vs /billing
-- Used for: billing2_cvr KPI showing lift value

SELECT
  AVG(CASE WHEN billing_variant = '/billing-2' THEN billing_to_order_cvr_pct ELSE 0 END)
  - AVG(CASE WHEN billing_variant = '/billing' THEN billing_to_order_cvr_pct ELSE 0 END) 
  AS lift_pp
FROM workspace.mff_gold.billing_ab_test_results;
