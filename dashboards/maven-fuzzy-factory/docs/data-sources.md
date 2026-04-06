# Data Sources Reference

## Overview

This document describes all data tables used in the Maven Fuzzy Factory dashboard. All tables reside in the **Unity Catalog** under the `workspace.mff_gold` schema, representing the **Gold layer** (analytics-ready, aggregated data).

---

## Data Lineage Summary

```
workspace.mff_bronze.*   →   workspace.mff_silver.*   →   workspace.mff_gold.*
    (Raw sources)              (Cleaned, enriched)         (Analytics aggregations)
                                                                     ↓
                                                        Lakeview Dashboard Datasets
```

---

## Gold Tables Inventory

### Executive Overview Tables

#### 1. `workspace.mff_gold.executive_kpis`

**Purpose:** Top-level business KPIs for executive dashboard

**Grain:** Single-row summary (entire business)

**Columns:**
| Column | Type | Description |
|--------|------|-------------|
| `total_orders` | INT | Total orders placed across all time |
| `total_gross_revenue` | DECIMAL(10,2) | Total revenue before refunds/COGS |
| `total_net_margin` | DECIMAL(10,2) | Total profit after COGS and refunds |
| `total_refund_value` | DECIMAL(10,2) | Total $ value of refunds |
| `avg_net_margin_pct` | DECIMAL(5,2) | Average net margin percentage |
| `overall_refund_rate_pct` | DECIMAL(5,2) | % of orders that resulted in refund |
| `cross_sell_rate_pct` | DECIMAL(5,2) | % of orders with 2+ products |

**Update Frequency:** Daily

**Usage:** Executive KPI strip, P&L snapshot

---

#### 2. `workspace.mff_gold.monthly_channel_trends`

**Purpose:** Time-series performance by marketing channel and device

**Grain:** One row per (year_month, marketing_channel, device_type)

**Columns:**
| Column | Type | Description |
|--------|------|-------------|
| `year_month` | STRING | YYYY-MM format (e.g., '2014-11') |
| `marketing_channel` | STRING | Acquisition channel (e.g., 'paid_search_gsearch_nonbrand') |
| `device_type` | STRING | 'desktop' or 'mobile' |
| `sessions` | INT | Count of sessions |
| `orders` | INT | Count of orders |
| `gross_revenue` | DECIMAL(10,2) | Revenue before costs |
| `cvr_pct` | DECIMAL(5,2) | Conversion rate (orders/sessions * 100) |

**Update Frequency:** Daily

**Usage:** Monthly revenue trend chart, revenue by channel chart

**Filterable By:** year_month, marketing_channel, device_type

---

#### 3. `workspace.mff_gold.sessions_enriched`

**Purpose:** Session-level data with enrichments (repeat flag, pageviews, etc.)

**Grain:** One row per website_session_id

**Columns:**
| Column | Type | Description |
|--------|------|-------------|
| `website_session_id` | BIGINT | Unique session identifier |
| `user_id` | BIGINT | User identifier (links to customer_segments) |
| `is_repeat_session` | BOOLEAN | TRUE if user has prior sessions |
| `utm_source` | STRING | Traffic source |
| `utm_campaign` | STRING | Campaign name |
| `device_type` | STRING | 'desktop' or 'mobile' |
| `created_at` | TIMESTAMP | Session start timestamp |

**Update Frequency:** Daily

**Usage:** Session type summary (new vs repeat CVR)

---

#### 4. `workspace.mff_gold.orders_enriched`

**Purpose:** Order-level data with product, margin, refund info

**Grain:** One row per order_id

**Columns:**
| Column | Type | Description |
|--------|------|-------------|
| `order_id` | BIGINT | Unique order identifier |
| `website_session_id` | BIGINT | Links to sessions_enriched |
| `user_id` | BIGINT | Customer identifier |
| `created_at` | TIMESTAMP | Order timestamp |
| `price_usd` | DECIMAL(6,2) | Order total price |
| `cogs_usd` | DECIMAL(6,2) | Cost of goods sold |
| `items_purchased` | INT | Item count in order |

**Update Frequency:** Daily

**Usage:** Orders by device, session type summary

---

### Funnel & Marketing Tables

#### 5. `workspace.mff_gold.funnel_step_analysis`

**Purpose:** 7-step conversion funnel with drop-off rates

**Grain:** One row per (channel_group, device_type)

**Columns:**
| Column | Type | Description |
|--------|------|-------------|
| `channel_group` | STRING | 'Paid Search', 'Organic', 'Direct', 'Social' |
| `device_type` | STRING | 'desktop' or 'mobile' |
| `sessions_entered` | INT | Sessions that started funnel |
| `reached_products` | INT | Sessions that reached /products |
| `pct_to_products` | DECIMAL(5,2) | % of sessions reaching products |
| `reached_product_page` | INT | Sessions viewing product detail |
| `pct_to_product_page` | DECIMAL(5,2) | % reaching product page |
| `reached_cart` | INT | Sessions adding to cart |
| `pct_to_cart` | DECIMAL(5,2) | % reaching cart |
| `reached_shipping` | INT | Sessions reaching shipping page |
| `pct_to_shipping` | DECIMAL(5,2) | % reaching shipping |
| `reached_billing` | INT | Sessions reaching billing page |
| `pct_to_billing` | DECIMAL(5,2) | % reaching billing |
| `reached_thankyou` | INT | Sessions completing order |
| `pct_to_order` | DECIMAL(5,2) | % completing order |

**Update Frequency:** Daily

**Usage:** Conversion funnel visualization, drop-off analysis

**Filterable By:** channel_group, device_type

---

#### 6. `workspace.mff_gold.channel_conversion_rates`

**Purpose:** CVR by marketing channel and device type

**Grain:** One row per (marketing_channel, device_type)

**Columns:**
| Column | Type | Description |
|--------|------|-------------|
| `marketing_channel` | STRING | Detailed channel (e.g., 'paid_search_gsearch_brand') |
| `device_type` | STRING | 'desktop' or 'mobile' |
| `total_sessions` | INT | Session count |
| `total_orders` | INT | Order count |
| `cvr_pct` | DECIMAL(5,2) | Conversion rate (orders/sessions * 100) |

**Update Frequency:** Daily

**Usage:** CVR by channel & device table, channel-specific KPIs

**Filterable By:** marketing_channel, device_type

---

#### 7. `workspace.mff_gold.ab_lander_test_results`

**Purpose:** Landing page A/B test performance

**Grain:** One row per landing_variant

**Columns:**
| Column | Type | Description |
|--------|------|-------------|
| `landing_variant` | STRING | Landing page version (e.g., 'lander-5') |
| `sessions` | INT | Sessions in test |
| `orders` | INT | Orders from test |
| `session_to_order_cvr_pct` | DECIMAL(5,2) | Test CVR |

**Update Frequency:** Static (A/B test complete)

**Usage:** Landing page A/B test chart

---

#### 8. `workspace.mff_gold.billing_ab_test_results`

**Purpose:** Billing page A/B test performance

**Grain:** One row per (billing_variant, device_type)

**Columns:**
| Column | Type | Description |
|--------|------|-------------|
| `billing_variant` | STRING | '/billing' or '/billing-2' |
| `device_type` | STRING | 'desktop' or 'mobile' |
| `sessions` | INT | Sessions reaching billing page |
| `orders` | INT | Orders completed |
| `billing_to_order_cvr_pct` | DECIMAL(5,2) | Billing page CVR |

**Update Frequency:** Static (A/B test complete)

**Usage:** Billing page A/B test table, billing-2 lift KPI

**Filterable By:** device_type

---

### Products Tables

#### 9. `workspace.mff_gold.product_monthly_performance`

**Purpose:** Product-level metrics over time

**Grain:** One row per (year_month, primary_product_name)

**Columns:**
| Column | Type | Description |
|--------|------|-------------|
| `year_month` | STRING | YYYY-MM format |
| `primary_product_name` | STRING | Product name (e.g., 'The Original Mr. Fuzzy') |
| `orders` | INT | Orders containing this product |
| `gross_revenue` | DECIMAL(10,2) | Revenue from this product |
| `avg_net_margin_pct` | DECIMAL(5,2) | Average margin % |
| `refund_rate_pct` | DECIMAL(5,2) | % of items refunded |

**Update Frequency:** Daily

**Usage:** Product launch curves, product P&L table

**Filterable By:** year_month, primary_product_name

---

#### 10. `workspace.mff_gold.cross_sell_matrix`

**Purpose:** Product pair attach rates (which products sell together)

**Grain:** One row per (primary_product_name, cross_sell_product_name)

**Columns:**
| Column | Type | Description |
|--------|------|-------------|
| `primary_product_name` | STRING | Main product purchased |
| `cross_sell_product_name` | STRING | Additional product in same order |
| `primary_orders` | INT | Orders with primary product |
| `cross_sell_orders` | INT | Orders with both products |
| `attach_rate_pct` | DECIMAL(5,2) | % of primary orders that include cross-sell |

**Update Frequency:** Daily

**Usage:** Cross-sell matrix table, best cross-sell KPI

---

#### 11. `workspace.mff_gold.refund_analysis`

**Purpose:** Refund rates by product and month

**Grain:** One row per (year_month, product_name)

**Columns:**
| Column | Type | Description |
|--------|------|-------------|
| `year_month` | STRING | YYYY-MM format |
| `product_name` | STRING | Product name |
| `items_sold` | INT | Total items sold |
| `items_refunded` | INT | Items refunded |
| `item_refund_rate_pct` | DECIMAL(5,2) | % of items refunded |
| `refund_value` | DECIMAL(10,2) | $ value refunded |
| `refund_value_pct` | DECIMAL(5,2) | % of revenue refunded |

**Update Frequency:** Daily

**Usage:** Refund trend chart, highest refund product KPI

**Filterable By:** year_month, product_name

---

### Customer & ML Tables

#### 12. `workspace.mff_gold.session_conversion_predictions`

**Purpose:** ML model predictions for session-level conversion probability

**Grain:** One row per website_session_id

**Columns:**
| Column | Type | Description |
|--------|------|-------------|
| `website_session_id` | BIGINT | Session identifier |
| `marketing_channel` | STRING | Acquisition channel |
| `device_type` | STRING | 'desktop' or 'mobile' |
| `conversion_prob` | DECIMAL(5,3) | Predicted conversion probability (0-1) |
| `predicted_conversion` | INT | 0 or 1 (threshold: 0.5) |
| `converted_label` | INT | Actual conversion (0 or 1) |
| `predicted_label` | INT | Same as predicted_conversion |
| `propensity_segment` | STRING | 'HIGH', 'MEDIUM', or 'LOW' |

**Update Frequency:** Daily (real-time prediction)

**Usage:** ML model metrics, propensity segments, top high-propensity sessions

**Filterable By:** marketing_channel, device_type

**Model:** Logistic Regression, AUC-ROC 0.847

---

#### 13. `workspace.mff_gold.ltv_by_acquisition_channel`

**Purpose:** Customer lifetime value by acquisition channel

**Grain:** One row per acquisition_channel

**Columns:**
| Column | Type | Description |
|--------|------|-------------|
| `acquisition_channel` | STRING | First-touch channel |
| `customers` | INT | Customer count |
| `avg_orders` | DECIMAL(5,2) | Avg orders per customer |
| `avg_gross_ltv` | DECIMAL(8,2) | Avg lifetime value (gross revenue) |

**Update Frequency:** Daily

**Usage:** LTV by channel chart, highest LTV channel KPI

---

#### 14. `workspace.mff_gold.acquisition_cohorts`

**Purpose:** Customer retention rates by monthly cohort

**Grain:** One row per acquisition_cohort (month)

**Columns:**
| Column | Type | Description |
|--------|------|-------------|
| `acquisition_cohort` | STRING | YYYY-MM of first order |
| `customers` | INT | Cohort size |
| `retention_rate_60d` | DECIMAL(5,2) | % active at 60 days |
| `retention_rate_90d` | DECIMAL(5,2) | % active at 90 days |
| `retention_rate_180d` | DECIMAL(5,2) | % active at 180 days |
| `retention_rate_365d` | DECIMAL(5,2) | % active at 365 days |

**Update Frequency:** Daily

**Usage:** Cohort retention heatmap, 180d retention KPI

**Filterable By:** acquisition_cohort

---

#### 15. `workspace.mff_gold.customer_segments`

**Purpose:** Customer segmentation using KMeans clustering (K=4)

**Grain:** One row per user_id

**Columns:**
| Column | Type | Description |
|--------|------|-------------|
| `user_id` | BIGINT | Customer identifier |
| `customer_segment` | INT | Cluster ID (0, 1, 2, 3) |
| `customer_tier` | STRING | 'VIP', 'Premium', 'Standard', 'At-Risk' |
| `total_orders` | INT | Lifetime orders |
| `gross_lifetime_value` | DECIMAL(10,2) | Total revenue from customer |
| `avg_days_between_orders` | DECIMAL(6,2) | Purchase frequency |
| `recency_days` | INT | Days since last order |

**Update Frequency:** Weekly

**Usage:** Customer segments table, segment count KPI

**Filterable By:** customer_tier

**Model:** KMeans (K=4), trained on RFM features

---

## Data Quality & Freshness

### Update Schedule
* **Gold tables:** Daily refresh via Delta Live Tables (DLT) pipelines
* **Dashboard datasets:** Query-time execution (real-time as of query)
* **ML predictions:** Batch prediction daily

### Data Coverage
* **Time Period:** January 2012 - March 2015
* **Orders:** 32,313 total
* **Sessions:** 269,000+ tracked
* **Products:** 4 SKUs
* **Channels:** 8 acquisition channels

### Known Data Issues
* **Incomplete mobile attribution:** Early 2012 mobile sessions may have missing UTM params
* **Product launches:** Mini Bear launched Feb 2014 (limited history)
* **Test periods:** Lander A/B test ran June-July 2013, Billing test ran Sep-Nov 2013

---

## Query Examples

### Recreate Executive KPIs
```sql
SELECT 
  COUNT(DISTINCT order_id) as total_orders,
  SUM(price_usd) as total_gross_revenue,
  SUM(price_usd - cogs_usd) as total_net_margin,
  AVG((price_usd - cogs_usd) / price_usd * 100) as avg_net_margin_pct
FROM workspace.mff_gold.orders_enriched;
```

### Channel Performance This Month
```sql
SELECT 
  marketing_channel,
  SUM(sessions) as sessions,
  SUM(orders) as orders,
  AVG(cvr_pct) as avg_cvr
FROM workspace.mff_gold.monthly_channel_trends
WHERE year_month = '2014-11'
GROUP BY marketing_channel
ORDER BY orders DESC;
```

### High-Value Customer Count
```sql
SELECT 
  customer_tier,
  COUNT(*) as customers,
  AVG(gross_lifetime_value) as avg_ltv
FROM workspace.mff_gold.customer_segments
WHERE customer_tier IN ('VIP', 'Premium')
GROUP BY customer_tier;
```

---

## Access & Permissions

### Required Permissions
* **Unity Catalog:** `USE CATALOG workspace`
* **Schema:** `USE SCHEMA mff_gold`
* **Tables:** `SELECT` on all `workspace.mff_gold.*` tables

### Service Principal
Dashboard uses **Serverless SQL Warehouse** with automatic credential passthrough.

---

**Last Updated:** April 2026  
**Data Owner:** Analytics Engineering Team  
**Data Steward:** Maven Fuzzy Factory BI Team
