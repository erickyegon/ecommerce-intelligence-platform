# Dashboard Filter Guide

## Overview

The Maven Fuzzy Factory dashboard uses 5 global filters that dynamically update metrics across all pages. This guide explains how to use each filter effectively and which datasets they impact.

---

## Global Filters

### 1. Month Filter (`filter_month`)

**Purpose:** Time-series filtering for trend analysis

**Column Bindings:**
* `year_month` → monthly_trends, product_monthly, refunds
* `acquisition_cohort` → cohort_retention

**Affected Widgets:**
* Executive: Monthly Revenue Trend, Revenue by Channel
* Products: Product Launch Curves, Refund Trend, Product P&L
* Customer: Cohort Retention Heatmap

**Usage Tips:**
* Select single month for point-in-time analysis
* Select range for trend comparisons
* Use Q4 2014 for holiday seasonality insights
* Compare same months year-over-year

**Example Scenarios:**
* "Show me November 2014 performance" → Select `2014-11`
* "Compare Q1 2013 vs Q1 2014" → Select `2013-01` through `2013-03`, then repeat for 2014

---

### 2. Channel Filter (`filter_channel`)

**Purpose:** Marketing channel segmentation

**Column Binding:** `marketing_channel`

**Bound Datasets:**
* monthly_trends (Executive page)
* channel_cvr (Funnel page)
* top_sessions (Customer/ML page)

**Affected Widgets:**
* Executive: Revenue by Channel, Monthly Revenue Trend
* Funnel: CVR by Channel & Device table
* Customer/ML: Top High-Propensity Sessions

**Available Values:**
* `paid_search_gsearch_nonbrand` - Google Search nonbrand
* `paid_search_gsearch_brand` - Google Search brand
* `paid_search_bsearch_nonbrand` - Bing Search nonbrand
* `paid_search_bsearch_brand` - Bing Search brand
* `paid_social_bsearch` - Bing Social
* `organic_search` - Organic search
* `direct` - Direct traffic
* `social_pilot` - Social media pilot

**Usage Tips:**
* Isolate paid search to analyze SEM ROI
* Compare brand vs nonbrand performance
* Filter to `organic_search` to measure SEO impact
* Use with device filter for channel-device interactions

**Example Scenarios:**
* "How does gsearch nonbrand perform?" → Select `paid_search_gsearch_nonbrand`
* "Compare all paid channels" → Multi-select all `paid_search_*` and `paid_social_*`

---

### 3. Device Filter (`filter_device`)

**Purpose:** Device-type segmentation (desktop vs mobile)

**Column Binding:** `device_type`

**Bound Datasets:**
* monthly_trends (Executive page)
* channel_cvr (Funnel page)
* billing_test (Funnel page)
* funnel_unpivoted (Funnel page)
* top_sessions (Customer/ML page)

**Affected Widgets:**
* Executive: Monthly Revenue Trend, Orders by Device
* Funnel: CVR by Channel & Device, Conversion Funnel, Billing A/B Test
* Customer/ML: Top High-Propensity Sessions

**Available Values:**
* `desktop` - Desktop/laptop sessions
* `mobile` - Mobile device sessions

**Usage Tips:**
* Desktop typically has higher CVR (2-3x mobile)
* Mobile traffic volume often higher than desktop
* Use for responsive design optimization insights
* Combine with channel filter for granular analysis

**Example Scenarios:**
* "Show mobile-only performance" → Select `mobile`
* "Compare desktop vs mobile CVR" → View table without filter, then filter each device separately

---

### 4. Product Filter (`filter_product`)

**Purpose:** Individual product deep-dive

**Column Binding:** `primary_product_name`

**Bound Datasets:**
* product_monthly (Products page)

**Affected Widgets:**
* Products: Product Launch Curves, Product P&L Table

**Available Values:**
* `The Original Mr. Fuzzy` - Flagship product
* `The Forever Love Bear` - Second launch
* `The Birthday Sugar Panda` - Third launch
* `The Hudson River Mini bear` - Newest product (Feb 2014)

**Usage Tips:**
* Focus on single product for launch analysis
* Compare metrics across products by toggling filter
* Use with month filter to see product lifecycle stages
* Monitor refund trends for quality issues

**Example Scenarios:**
* "How is Mini Bear performing since launch?" → Select `The Hudson River Mini bear` + `2014-02` onwards
* "Compare Mr. Fuzzy to Love Bear" → Toggle between products to compare P&L

---

### 5. Customer Tier Filter (`filter_customer_tier`)

**Purpose:** Customer segment targeting

**Column Binding:** `customer_tier`

**Bound Datasets:**
* customer_seg_summary (Customer/ML page)

**Affected Widgets:**
* Customer/ML: Customer Segments (KMeans) table

**Available Values:**
* `VIP` - Highest LTV, most orders
* `Premium` - Above-average LTV
* `Standard` - Average customers
* `At-Risk` - Low engagement, potential churn

**Usage Tips:**
* Focus retention efforts on VIP + Premium tiers
* Analyze At-Risk tier for churn prediction
* Use to validate LTV channel effectiveness
* Segment marketing campaigns by tier

**Example Scenarios:**
* "Show VIP customer characteristics" → Select `VIP` tier
* "How many customers are at-risk?" → Select `At-Risk` tier, view count

---

## Filter Combinations

### Recommended Filter Combos

**1. Channel Performance Deep-Dive**
* Filters: Channel + Device + Month
* Pages: Executive, Funnel & Marketing
* Use Case: "How did gsearch nonbrand desktop perform in November 2014?"

**2. Product Launch Analysis**
* Filters: Product + Month (range)
* Pages: Products
* Use Case: "Track Mini Bear performance from launch to present"

**3. Customer Acquisition ROI**
* Filters: Channel + Customer Tier
* Pages: Customer & ML
* Use Case: "Which channels acquire VIP customers?"

**4. Seasonal Trend Analysis**
* Filters: Month (multi-select: Nov-Dec 2013, 2014)
* Pages: Executive, Products
* Use Case: "Compare holiday seasons year-over-year"

**5. Mobile Optimization**
* Filters: Device (mobile) + Channel
* Pages: Funnel & Marketing
* Use Case: "Identify mobile funnel drop-off points by channel"

---

## Filter Best Practices

### DO:
✅ Clear filters between analyses to avoid confusion  
✅ Use filter combinations for multi-dimensional insights  
✅ Document filter settings when sharing dashboard snapshots  
✅ Test filter impact by comparing filtered vs unfiltered views  
✅ Use month ranges for trend analysis, single months for point-in-time  

### DON'T:
❌ Leave filters active when switching to unrelated analysis  
❌ Over-filter (too many active filters = limited data)  
❌ Assume filters apply to all widgets (check dataset bindings)  
❌ Filter to single month when analyzing annual trends  
❌ Forget that filters persist across page navigation  

---

## Filter Coverage by Page

| Page | Month | Channel | Device | Product | Customer Tier |
|------|-------|---------|--------|---------|---------------|
| **Executive Overview** | ✅ | ✅ | ✅ | ❌ | ❌ |
| **Funnel & Marketing** | ❌ | ✅ | ✅ | ❌ | ❌ |
| **Products** | ✅ | ❌ | ❌ | ✅ | ❌ |
| **Customer & ML** | ✅ | ✅ | ✅ | ❌ | ✅ |

---

## Advanced: Adding New Filters

To extend filter coverage:

1. **Identify target dataset(s)** in dashboard
2. **Ensure column exists** in dataset query
3. **Add filter binding** in Global Filters page
4. **Test filter impact** on affected widgets
5. **Document** in this guide

### Example: Adding "Landing Page" Filter
```sql
-- Add to funnel_unpivoted dataset
WHERE landing_page_path IN ('{{ filter_landing_page }}')

-- Bind filter widget to:
-- - datasets/funnel_unpivoted.landing_page_path
-- - datasets/lander_test.landing_variant
```

---

## Troubleshooting

### Filter not working?
* Check dataset has the column in its SELECT statement
* Verify column name matches exactly (case-sensitive)
* Ensure filter widget is bound to dataset
* Clear cache and refresh dashboard

### Filter showing "No data"?
* Too many active filters narrowing results to zero
* Check data exists for filter combination
* Verify time range includes data (2012-2015 only)

### Filter affecting wrong widgets?
* Review dataset bindings on Global Filters page
* Ensure filter column name matches across datasets
* Check for dataset name collisions

---

**Last Updated:** April 2026
