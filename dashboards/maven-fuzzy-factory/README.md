# Maven Fuzzy Factory Executive Dashboard

## Overview

A comprehensive 4-page executive analytics dashboard for Maven Fuzzy Factory e-commerce business, built with modern BI styling and interactive filtering capabilities. The dashboard provides end-to-end visibility from executive KPIs through funnel optimization, product performance, to customer lifetime value and ML-powered conversion predictions.

**Dashboard ID:** `01f131fa580d10f9a5eef6ab87cf6e70`

**Last Updated:** April 2026

---

## Dashboard Architecture

### Technology Stack
* **Platform:** Databricks Lakeview Dashboards
* **Data Source:** Unity Catalog (`workspace.mff_gold.*` schema)
* **Compute:** Serverless SQL warehouse
* **Styling:** Modern BI aesthetic (soft neutrals, #1976D2 blue accent, rounded cards)

### Data Pipeline Flow
```
Bronze Layer (Raw)          Silver Layer (Cleaned)       Gold Layer (Analytics)
├── website_sessions   →    sessions_enriched       →   executive_kpis
├── website_pageviews  →    pageviews_enriched     →   monthly_channel_trends
├── orders             →    orders_enriched        →   channel_conversion_rates
├── order_items        →    order_items_enriched   →   funnel_step_analysis
├── order_item_refunds →    refunds_enriched       →   product_monthly_performance
└── products           →    products_dim           →   cross_sell_matrix
                                                    →   ltv_by_acquisition_channel
                                                    →   acquisition_cohorts
                                                    →   session_conversion_predictions
                                                    →   customer_segments (KMeans K=4)
```

---

## Dashboard Pages

### 1. Executive Overview (pages/bd697ae5)
**Purpose:** High-level business health snapshot for C-suite stakeholders

**Widgets (13):**
* **KPI Strip:** Total Orders, Gross Revenue (with MoM trend), Net Margin %, Refund Rate %, Cross-Sell Rate %
* **Main Analytics:** Monthly Revenue Trend (line chart), Revenue by Channel (bar chart)
* **Details:** Total Sessions, Channel Revenue, Orders by Device (pie), Session Type Split (New vs Repeat CVR), P&L Snapshot

**Key Metrics:**
* Total Orders: 32,313
* Gross Revenue: $1.94M
* Net Margin: 58.3%
* Refund Rate: 5.4%
* Cross-Sell Rate: 6.3%

**Data Sources:**
* `workspace.mff_gold.executive_kpis`
* `workspace.mff_gold.monthly_channel_trends`
* `workspace.mff_gold.sessions_enriched`
* `workspace.mff_gold.orders_enriched`

---

### 2. Funnel & Marketing (pages/funnel_marketing)
**Purpose:** Deep-dive into conversion funnel optimization and A/B test results

**Widgets (10):**
* **KPI Strip:** Overall CVR, gsearch CVR, bsearch CVR, Billing-2 Lift, Best Lander
* **Main Analytics:** 7-Step Conversion Funnel (horizontal bar), CVR by Channel & Device (table)
* **A/B Tests:** Lander Test Results (bar chart), Billing Test Results (table)

**Key Insights:**
* Biggest drop-off: Products → Cart (54% drop)
* Best performer: gsearch brand desktop (10.5% CVR)
* Billing-2 delivers ~20pp lift over original /billing page
* Lander-5 wins A/B test at 10.2% CVR

**Data Sources:**
* `workspace.mff_gold.funnel_step_analysis`
* `workspace.mff_gold.channel_conversion_rates`
* `workspace.mff_gold.ab_lander_test_results`
* `workspace.mff_gold.billing_ab_test_results`

---

### 3. Products (pages/products)
**Purpose:** Product performance, cross-sell opportunities, refund monitoring

**Widgets (10):**
* **KPI Strip:** Top Revenue Product, Highest Margin, Highest Refund (red if ≥5%), Best Cross-Sell, Newest Product
* **Main Analytics:** Product Launch Curves (line chart), Product P&L Table
* **Details:** Cross-Sell Matrix (12 product pairs), Refund Trend (line chart)

**Product Portfolio:**
* **Mr. Fuzzy:** 23,861 orders | $1.42M revenue | 56.2% margin | 5.8% refund
* **Love Bear:** 4,803 orders | 60.3% margin
* **Sugar Panda:** 3,068 orders | 62.3% margin | 6.9% refund
* **Mini Bear:** 581 orders | 66.9% margin | 1.6% refund (newest launch Feb 2014)

**Data Sources:**
* `workspace.mff_gold.product_monthly_performance`
* `workspace.mff_gold.cross_sell_matrix`
* `workspace.mff_gold.refund_analysis`

---

### 4. Customer & ML (pages/customer_ml)
**Purpose:** Customer lifetime value analysis and ML-powered conversion predictions

**Widgets (11):**
* **KPI Strip:** Model AUC (0.847), Model F1 Score, Highest LTV Channel, 180d Retention, Segment Count
* **Main Analytics:** Cohort Retention Heatmap (60d/90d/180d/365d), LTV by Channel (bar)
* **ML Insights:** Propensity Segments (HIGH/MEDIUM/LOW), Top High-Propensity Sessions, Customer Segments (KMeans K=4)

**ML Model Performance:**
* AUC-ROC: 0.847
* Precision & Recall calculated dynamically
* Segments: HIGH (6.8% sessions, 99.5% prob), LOW (93.2% sessions, 0% prob)

**Customer Segments:**
* 4 clusters (KMeans): VIP, Premium, Standard, At-Risk
* Highest LTV channel: social_pilot ($66 avg)

**Data Sources:**
* `workspace.mff_gold.session_conversion_predictions`
* `workspace.mff_gold.ltv_by_acquisition_channel`
* `workspace.mff_gold.acquisition_cohorts`
* `workspace.mff_gold.customer_segments`

---

## Global Filters

### Active Filters (5 widgets on pages/global_filters)

| Filter Name | Column Binding | Datasets Bound | Impact |
|-------------|----------------|----------------|---------|
| **filter_month** | `year_month`, `acquisition_cohort` | monthly_trends, product_monthly, refunds, cohort_retention | Time-series filtering across all pages |
| **filter_channel** | `marketing_channel` | monthly_trends, channel_cvr, top_sessions | Channel-specific analysis |
| **filter_device** | `device_type` | monthly_trends, channel_cvr, billing_test, funnel_unpivoted, top_sessions | Device segmentation |
| **filter_product** | `primary_product_name` | product_monthly | Product deep-dive |
| **filter_customer_tier** | `customer_tier` | customer_seg_summary | Segment analysis |

### Filter Best Practices
* **Month filter:** Use for trend analysis and year-over-year comparisons
* **Channel filter:** Isolate paid search, organic, direct, or social performance
* **Device filter:** Compare desktop vs mobile conversion patterns
* **Product filter:** Analyze individual product launch curves and cross-sell
* **Customer tier filter:** Focus on VIP, Premium, Standard, or At-Risk segments

---

## Widget Inventory

### Total: 44 widgets across 4 pages + 5 filter widgets

**By Type:**
* Counters (KPI cards): 24
* Line charts: 4
* Bar charts: 6
* Tables: 7
* Pie charts: 1
* Pivot tables (heatmaps): 1
* Text headers: 1

**By Page:**
* Executive Overview: 13 widgets
* Funnel & Marketing: 10 widgets
* Products: 10 widgets
* Customer & ML: 11 widgets

---

## Dataset Inventory

### Total: 33 unique datasets

**Executive (6 datasets):**
* executive_kpis, kpis_with_trends, monthly_trends, pl_summary, session_type_summary, totals_summary

**Funnel (9 datasets):**
* channel_cvr, billing_test, funnel_analysis, lander_test, funnel_unpivoted, overall_cvr_agg, gsearch_cvr_agg, bsearch_cvr_agg, billing_lift

**Products (9 datasets):**
* cross_sell, product_monthly, refunds, top_product_revenue, highest_margin_product, highest_refund_product, newest_product, best_cross_sell, product_pl

**Customer/ML (9 datasets):**
* ml_model_metrics, ltv_channels, cohort_retention, propensity_segments, customer_seg_summary, top_sessions, retention_180d, segment_count, highest_ltv_channel

---

## Custom Calculations

### Dashboard uses 3 custom calculations:

1. **Channel Display** (monthly_trends dataset)
   ```sql
   CASE
     WHEN marketing_channel = 'paid_search_gsearch_nonbrand' THEN 'Paid Search - gsearch'
     WHEN marketing_channel = 'paid_search_gsearch_brand' THEN 'Paid Search - Brand'
     WHEN marketing_channel = 'paid_social_bsearch' THEN 'Paid Social - bsearch'
     WHEN marketing_channel = 'direct' THEN 'Direct'
     WHEN marketing_channel = 'organic_search' THEN 'Organic Search'
     ELSE REPLACE(REPLACE(marketing_channel, '_', ' '), 'paid search', 'Paid Search')
   END
   ```

2. **Aggregated CVR** (channel_cvr dataset)
   ```sql
   SUM(total_orders) * 100.0 / SUM(total_sessions)
   ```

3. **F1 Score** (ml_model_metrics dataset)
   ```sql
   ROUND(2 * (precision * recall) / NULLIF((precision + recall), 0), 3)
   ```

---

## Design System

### Color Palette
* **Primary:** #1976D2 (blue for emphasis)
* **Background:** #F5F5F5 (soft neutral)
* **Cards:** White with subtle borders and shadows
* **Text:** Dark gray (#333) for primary, light gray for secondary
* **Alerts:** Red for refund rates ≥5%, green for positive trends

### Layout Guidelines
* **Header row:** Row 0 (page title + context)
* **KPI strip:** Row 3 (5 counter widgets)
* **Main analytics:** Row 9 (primary charts)
* **Details:** Row 24+ (supporting tables/charts)
* **Spacing:** 2-row gaps between sections for visual breathing room

### Typography
* **Headers:** Bold, 16-18pt
* **KPI values:** Bold, 20-24pt
* **Labels:** Regular, 12-14pt
* **Professional, sans-serif font family throughout**

---

## Usage Guide

### For Executives
1. Start with **Executive Overview** for business health snapshot
2. Check MoM revenue trend indicator (green = growing, red = declining)
3. Review P&L snapshot for profitability
4. Use **month filter** to compare periods

### For Marketing Teams
1. Navigate to **Funnel & Marketing** page
2. Identify funnel drop-off points (Products → Cart is critical)
3. Compare channel performance (gsearch brand desktop leads at 10.5% CVR)
4. Validate A/B test winners (Billing-2, Lander-5)
5. Use **channel + device filters** for segmented analysis

### For Product Teams
1. Go to **Products** page
2. Monitor product launch curves (Mr. Fuzzy = mature, Mini Bear = new)
3. Track refund trends (flag products ≥5%)
4. Identify cross-sell opportunities (20.9% attach rate for best pair)
5. Use **product filter** for deep-dives

### For Data Science / CRM Teams
1. Open **Customer & ML** page
2. Review model performance (AUC 0.847)
3. Analyze cohort retention heatmap
4. Target HIGH propensity segment (6.8% of sessions at 99.5% conv prob)
5. Focus marketing budget on highest LTV channels

---

## Navigation

### Page Links
* **Executive Overview:** Main entry point
* **Funnel & Marketing:** From Executive → Click funnel metrics
* **Products:** From Executive → Click product performance
* **Customer & ML:** From Products → Click customer segments

### Drill-Down Interactions
* Click channel names to filter across pages
* Click product names to filter product-specific views
* Click cohort dates to filter retention analysis
* All filters persist across page navigation

---

## Maintenance & Updates

### Data Refresh
* **Frequency:** Daily (automated via Delta Live Tables pipeline)
* **Source:** `workspace.mff_gold.*` Unity Catalog tables
* **Last refresh:** Real-time (current as of query execution)

### Dashboard Versioning
* **Current version:** v2.0 (dynamic filters, no hard-coded insights)
* **Git repository:** `ecommerce-intelligence-platform/dashboards/maven-fuzzy-factory/`
* **Backup:** Dashboard JSON exported to version control

### Known Limitations
* Historical data: 2012-2015 (Maven Fuzzy Factory dataset timeframe)
* ML model: Logistic Regression with AUC 0.847 (potential for improvement with ensemble methods)
* Cross-sell matrix: Only shows product pairs with ≥10 primary orders

---

## Support & Documentation

### Additional Resources
* **Query Library:** `dashboards/maven-fuzzy-factory/queries/` (organized by page)
* **Data Dictionary:** See `docs/data-sources.md`
* **Filter Guide:** See `docs/filter-guide.md`
* **Architecture:** See main repo `ARCHITECTURE.md`

### Contact
For dashboard issues, data questions, or feature requests, contact the Analytics Engineering team.

---

**Built with ❤️ by the Maven Fuzzy Factory Analytics Team**
