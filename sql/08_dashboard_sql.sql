-- ══════════════════════════════════════════════════════════════════════════════
-- Maven Fuzzy Factory | E-Commerce Intelligence Platform
-- Notebook 08 — DATABRICKS SQL DASHBOARD QUERIES
-- ══════════════════════════════════════════════════════════════════════════════
--
--  4-PAGE DASHBOARD LAYOUT:
--    Page 1 — Executive Overview     (KPI tiles + revenue trend)
--    Page 2 — Marketing & Funnel     (channel CVR, funnel drop-off, A/B tests)
--    Page 3 — Product Analytics      (revenue by product, cross-sell, refunds)
--    Page 4 — Customer & ML Insights (cohorts, LTV by channel, propensity)
--
--  HOW TO USE:
--    1. Open Databricks SQL → SQL Editor
--    2. Paste each query block → Run → Save as named query (name in header)
--    3. Dashboard → Add visualisation → select saved query → pick chart type
-- ══════════════════════════════════════════════════════════════════════════════


-- ╔══════════════════════════════════════════════════════════════════╗
-- ║   PAGE 1 — EXECUTIVE OVERVIEW                                   ║
-- ╚══════════════════════════════════════════════════════════════════╝

-- [KPI-01]  Total Orders  (Counter tile)
SELECT total_orders AS "Total Orders"
FROM mff_gold.executive_kpis;

-- [KPI-02]  Total Gross Revenue  (Counter tile)
SELECT CONCAT('$', FORMAT_NUMBER(total_gross_revenue, 0)) AS "Gross Revenue"
FROM mff_gold.executive_kpis;

-- [KPI-03]  Net Margin %  (Counter tile)
SELECT avg_net_margin_pct AS "Avg Net Margin %"
FROM mff_gold.executive_kpis;

-- [KPI-04]  Overall Refund Rate  (Counter tile — red if > 5%)
SELECT overall_refund_rate_pct AS "Refund Rate %"
FROM mff_gold.executive_kpis;

-- [KPI-05]  Cross-Sell Rate  (Counter tile)
SELECT cross_sell_rate_pct AS "Cross-Sell Rate %"
FROM mff_gold.executive_kpis;

-- [CHART-01]  Monthly Revenue Trend — all channels combined  (Line chart)
--             x=year_month, y=gross_revenue + net_margin
SELECT
    year_month                    AS "Month",
    SUM(gross_revenue)            AS "Gross Revenue",
    SUM(net_margin)               AS "Net Margin",
    SUM(orders)                   AS "Orders",
    ROUND(AVG(cvr_pct), 2)        AS "Avg CVR %"
FROM mff_gold.monthly_channel_trends
GROUP BY year_month
ORDER BY year_month;

-- [CHART-02]  Revenue by Channel Group — stacked area  (Area chart)
--             x=year_month, y=gross_revenue, colour=channel_group
SELECT
    year_month        AS "Month",
    channel_group     AS "Channel",
    SUM(gross_revenue) AS "Revenue"
FROM mff_gold.monthly_channel_trends
GROUP BY year_month, channel_group
ORDER BY year_month, channel_group;

-- [CHART-03]  Sessions vs Orders over time  (Dual-axis line)
SELECT
    year_month                  AS "Month",
    SUM(sessions)               AS "Sessions",
    SUM(orders)                 AS "Orders",
    ROUND(SUM(orders) / SUM(sessions) * 100, 2) AS "CVR %"
FROM mff_gold.monthly_channel_trends
GROUP BY year_month
ORDER BY year_month;


-- ╔══════════════════════════════════════════════════════════════════╗
-- ║   PAGE 2 — MARKETING & FUNNEL                                   ║
-- ╚══════════════════════════════════════════════════════════════════╝

-- [CHART-04]  CVR by Marketing Channel × Device  (Grouped bar)
SELECT
    marketing_channel           AS "Channel",
    device_type                 AS "Device",
    total_sessions              AS "Sessions",
    total_orders                AS "Orders",
    conversion_rate_pct         AS "CVR %",
    revenue_per_session         AS "Revenue / Session"
FROM mff_gold.channel_conversion_rates
ORDER BY conversion_rate_pct DESC;

-- [CHART-05]  Funnel Drop-off — Desktop Paid Search  (Waterfall / Funnel chart)
--             For each step: how many sessions progressed?
SELECT
    channel_group,
    device_type,
    sessions_entered            AS "1. Sessions",
    reached_products            AS "2. Products",
    reached_product_page        AS "3. Product Page",
    reached_cart                AS "4. Cart",
    reached_shipping            AS "5. Shipping",
    reached_billing             AS "6. Billing",
    reached_thankyou            AS "7. Order Placed",
    pct_to_order                AS "End-to-End CVR %",
    dropoff_cart_to_shipping    AS "Cart Drop-off %",
    dropoff_billing_to_order    AS "Billing Drop-off %"
FROM mff_gold.funnel_step_analysis
ORDER BY channel_group, device_type;

-- [CHART-06]  Landing Page A/B Test  (Bar chart)
SELECT
    entry_page      AS "Landing Page",
    sessions        AS "Sessions",
    orders          AS "Orders",
    cvr_pct         AS "CVR %",
    avg_revenue_per_session AS "Revenue / Session",
    total_revenue   AS "Total Revenue"
FROM mff_gold.ab_lander_test_results
WHERE entry_page IN ('/home','/lander-1','/lander-2','/lander-3','/lander-4','/lander-5')
ORDER BY cvr_pct DESC;

-- [CHART-07]  Billing Page A/B Test — CVR lift  (Side-by-side bar)
SELECT
    billing_variant             AS "Billing Page",
    channel_group               AS "Channel",
    device_type                 AS "Device",
    sessions_at_billing         AS "Sessions at Billing",
    orders                      AS "Orders",
    billing_to_order_cvr_pct    AS "Billing → Order CVR %",
    avg_revenue_per_session     AS "Avg Revenue / Session"
FROM mff_gold.billing_ab_test_results
ORDER BY billing_variant, channel_group;

-- [CHART-08]  MoM CVR change by channel  (Heatmap / Table)
SELECT
    year_month                  AS "Month",
    channel_group               AS "Channel",
    device_type                 AS "Device",
    cvr_pct                     AS "CVR %",
    sessions_mom_pct_chg        AS "Sessions MoM %",
    orders_mom_pct_chg          AS "Orders MoM %"
FROM mff_gold.monthly_channel_trends
WHERE year_month >= '2013-01'
ORDER BY year_month, channel_group;


-- ╔══════════════════════════════════════════════════════════════════╗
-- ║   PAGE 3 — PRODUCT ANALYTICS                                    ║
-- ╚══════════════════════════════════════════════════════════════════╝

-- [CHART-09]  Product Monthly Revenue — all 4 products  (Line chart)
SELECT
    year_month                  AS "Month",
    primary_product_name        AS "Product",
    orders                      AS "Orders",
    gross_revenue               AS "Gross Revenue",
    net_margin                  AS "Net Margin",
    avg_gross_margin_pct        AS "Gross Margin %",
    refund_rate_pct             AS "Refund Rate %",
    cross_sell_rate_pct         AS "Cross-Sell Rate %"
FROM mff_gold.product_monthly_performance
WHERE primary_product_name IS NOT NULL
ORDER BY year_month, primary_product_name;

-- [CHART-10]  Product Gross Margin Comparison  (Grouped bar)
SELECT
    primary_product_name        AS "Product",
    SUM(orders)                 AS "Total Orders",
    ROUND(SUM(gross_revenue),2) AS "Total Revenue",
    ROUND(AVG(avg_gross_margin_pct),1) AS "Avg Gross Margin %",
    ROUND(AVG(avg_net_margin_pct),1)   AS "Avg Net Margin %",
    SUM(refunded_orders)        AS "Total Refunded",
    ROUND(AVG(refund_rate_pct),2) AS "Avg Refund Rate %"
FROM mff_gold.product_monthly_performance
WHERE primary_product_name IS NOT NULL
GROUP BY primary_product_name
ORDER BY SUM(gross_revenue) DESC;

-- [CHART-11]  Cross-Sell Matrix  (Heat table)
SELECT
    primary_product_name        AS "Primary Product",
    cross_sell_product_name     AS "Cross-Sell Product",
    cross_sell_orders           AS "Cross-Sell Orders",
    attach_rate_pct             AS "Attach Rate %",
    avg_order_value             AS "Avg Order Value ($)",
    avg_margin_pct              AS "Avg Margin %"
FROM mff_gold.cross_sell_matrix
ORDER BY primary_product_name, cross_sell_orders DESC;

-- [CHART-12]  Refund Rate Trend by Product  (Line chart)
SELECT
    year_month                  AS "Month",
    product_name                AS "Product",
    items_sold                  AS "Items Sold",
    items_refunded              AS "Items Refunded",
    item_refund_rate_pct        AS "Refund Rate %",
    net_revenue                 AS "Net Revenue"
FROM mff_gold.refund_analysis
WHERE product_name IS NOT NULL
ORDER BY year_month, product_name;

-- [CHART-13]  Product Launch Impact — ramp velocity  (Grouped bar)
SELECT
    primary_product_name        AS "Product",
    launch_period               AS "Months Since Launch",
    orders                      AS "Orders",
    gross_revenue               AS "Revenue",
    avg_margin_pct              AS "Margin %",
    refund_rate_pct             AS "Refund Rate %"
FROM mff_gold.product_launch_impact
ORDER BY primary_product_name, launch_period;


-- ╔══════════════════════════════════════════════════════════════════╗
-- ║   PAGE 4 — CUSTOMER & ML INSIGHTS                               ║
-- ╚══════════════════════════════════════════════════════════════════╝

-- [CHART-14]  LTV by Acquisition Channel  (Bar + line combo)
SELECT
    acquisition_channel         AS "Acquisition Channel",
    customers_acquired          AS "Customers",
    avg_gross_ltv               AS "Avg Gross LTV ($)",
    avg_net_ltv                 AS "Avg Net LTV ($)",
    avg_orders_per_customer     AS "Avg Orders / Customer",
    repeat_buyer_rate_pct       AS "Repeat Buyer Rate %",
    avg_customer_refund_rate    AS "Avg Refund Rate %"
FROM mff_gold.ltv_by_acquisition_channel
ORDER BY avg_gross_ltv DESC;

-- [CHART-15]  Monthly Cohort Retention Rates  (Heatmap)
SELECT
    acquisition_cohort          AS "Cohort (Month)",
    acquisition_channel         AS "Channel",
    new_customers               AS "New Customers",
    avg_first_order_value       AS "Avg 1st Order ($)",
    retention_rate_60d          AS "60-Day Retention %",
    retention_rate_90d          AS "90-Day Retention %",
    retention_rate_180d         AS "180-Day Retention %",
    retention_rate_365d         AS "365-Day Retention %",
    avg_days_to_repeat          AS "Avg Days to Repeat"
FROM mff_gold.acquisition_cohorts
ORDER BY acquisition_cohort, acquisition_channel;

-- [CHART-16]  New vs Repeat Session CVR  (Side-by-side bar)
SELECT
    CASE WHEN is_repeat_session THEN 'Repeat' ELSE 'New' END AS "Session Type",
    channel_group               AS "Channel Group",
    device_type                 AS "Device",
    sessions                    AS "Sessions",
    orders                      AS "Orders",
    cvr_pct                     AS "CVR %",
    avg_revenue_per_session     AS "Revenue / Session"
FROM mff_gold.repeat_session_analysis
ORDER BY is_repeat_session DESC, channel_group;

-- [CHART-17]  Propensity Segment Distribution  (Donut chart)
SELECT
    propensity_segment          AS "Propensity Segment",
    marketing_channel           AS "Channel",
    COUNT(*)                    AS "Sessions",
    ROUND(AVG(conversion_prob)*100, 2) AS "Avg Conversion Prob %",
    SUM(converted_label)        AS "Actual Conversions"
FROM mff_gold.session_conversion_predictions
GROUP BY propensity_segment, marketing_channel
ORDER BY propensity_segment, COUNT(*) DESC;

-- [CHART-18]  Customer Segment Profile  (Radar / Table)
SELECT
    customer_segment            AS "Segment",
    COUNT(*)                    AS "Customers",
    ROUND(AVG(total_orders),2)  AS "Avg Orders",
    ROUND(AVG(gross_lifetime_value),2) AS "Avg LTV ($)",
    acquisition_channel         AS "Primary Channel"
FROM mff_gold.customer_segments
GROUP BY customer_segment, acquisition_channel
ORDER BY customer_segment, COUNT(*) DESC;

-- [CHART-19]  Model Performance Summary  (KPI tiles)
SELECT
    propensity_segment          AS "Segment",
    COUNT(*)                    AS "Sessions",
    SUM(converted_label)        AS "Actual Orders",
    SUM(predicted_conversion)   AS "Predicted Orders",
    ROUND(AVG(conversion_prob)*100, 2) AS "Avg Prob %"
FROM mff_gold.session_conversion_predictions
GROUP BY propensity_segment
ORDER BY propensity_segment;
