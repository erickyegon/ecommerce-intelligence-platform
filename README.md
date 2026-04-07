# E-Commerce Intelligence Platform
### End-to-End Lakehouse Analytics · Delta Lake · Apache Spark · MLflow · Databricks

[![Databricks](https://img.shields.io/badge/Databricks-Free%20Edition-FF3621?logo=databricks&logoColor=white)](https://databricks.com)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.0-00ADD8?logo=apachespark&logoColor=white)](https://delta.io)
[![MLflow](https://img.shields.io/badge/MLflow-Tracked-0194E2?logo=mlflow&logoColor=white)](https://mlflow.org)
[![Python](https://img.shields.io/badge/Python-3.10-3776AB?logo=python&logoColor=white)](https://python.org)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

---

## Overview

This platform demonstrates a **production-grade, end-to-end analytics engineering lifecycle** — from raw multi-source ingestion through governed transformation, business intelligence aggregation, machine learning, and executive dashboarding — built entirely on Databricks Free Edition.

The dataset is three years of transactional e-commerce data (Maven Fuzzy Factory, 2012–2015) comprising **1.73 million rows across 6 relational tables**. The architecture, governance patterns, and engineering decisions implemented here are domain-agnostic and directly transferable to any enterprise analytics environment — whether the domain is retail, healthcare, financial services, or operations.

The platform addresses a common organizational challenge: **transforming raw operational data into trusted, governed, decision-ready intelligence** — with full audit trails, standardized metric definitions, self-service semantic views, and ML-powered operational insights.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  SOURCE LAYER — Operational Database Extract (MySQL → CSV)                  │
│                                                                             │
│  website_sessions    472,871 rows   UTM attribution, device, repeat flag   │
│  website_pageviews 1,188,124 rows   Page-level clickstream per session     │
│  orders               32,313 rows   Completed transactions                  │
│  order_items          40,025 rows   Line items with product & margin data  │
│  order_item_refunds    1,731 rows   Refund events with timing               │
│  products                  4 rows   Product catalogue                       │
└──────────────────────────────┬──────────────────────────────────────────────┘
                               │
                ┌──────────────▼──────────────┐
                │   00_data_quality_framework  │  ← DQ audit before ingestion
                │   mff_audit.dq_results       │
                │   mff_audit.pipeline_runs    │
                └──────────────┬──────────────┘
                               │
┌──────────────────────────────▼──────────────────────────────────────────────┐
│  BRONZE LAYER — mff_bronze.*                                                │
│                                                                             │
│  Raw Delta tables. Explicit schema enforcement. No transformations.         │
│  Audit metadata columns: _ingest_ts, _source_table, _layer                 │
│  website_pageviews partitioned by pv_year / pv_month for query pruning     │
│  Full ACID transaction log. Time travel enabled.                            │
└──────────────────────────────┬──────────────────────────────────────────────┘
                               │
              ┌────────────────┴────────────────┐
              │  02_silver_sessions             │  03_silver_orders_revenue
              │  mff_silver.sessions_enriched   │  mff_silver.orders_enriched
              │  mff_silver.session_funnel_steps│  mff_silver.order_items_enriched
              └────────────────┬────────────────┘
                               │
┌──────────────────────────────▼──────────────────────────────────────────────┐
│  SILVER LAYER — mff_silver.*                                                │
│                                                                             │
│  Validated, conformed, enriched. DQ flags on every row.                    │
│                                                                             │
│  sessions_enriched:                                                         │
│    • 8-way channel taxonomy (paid search, brand, social, organic, direct)  │
│    • First-touch attribution per user via window functions                  │
│    • Temporal features: hour, day-of-week, quarter, is_weekend             │
│    • User session rank (acquisition vs repeat)                              │
│                                                                             │
│  session_funnel_steps:                                                      │
│    • 17 boolean page flags per session (pivot of 1.18M pageview rows)      │
│    • Funnel depth score (1=entry → 7=order)                                │
│    • Exit step label per session                                            │
│                                                                             │
│  orders_enriched:                                                           │
│    • 5-way join: orders × items × refunds × sessions × products            │
│    • Gross/net revenue, COGS, margin %, refund timing                      │
│    • Cross-sell detection, months-since-launch feature engineering         │
│    • Full marketing attribution carried from sessions                       │
└──────────────────────────────┬──────────────────────────────────────────────┘
                               │
┌──────────────────────────────▼──────────────────────────────────────────────┐
│  GOLD LAYER — mff_gold.*  (18 tables)                                       │
│                                                                             │
│  MARKETING & FUNNEL                                                         │
│    channel_conversion_rates      CVR × revenue by channel × device         │
│    monthly_channel_trends        36-month time series + MoM deltas         │
│    funnel_step_analysis          Drop-off % at each of 7 funnel steps      │
│    ab_lander_test_results        6 landing page variants on CVR             │
│    billing_ab_test_results       /billing vs /billing-2 lift               │
│    channel_revenue_roi           Last-touch revenue attribution             │
│    channel_efficiency_matrix     Composite channel efficiency ranking       │
│                                                                             │
│  PRODUCT                                                                    │
│    product_monthly_performance   Full P&L per product per month            │
│    product_launch_impact         Sales velocity post-launch                │
│    cross_sell_matrix             Product pair attach rates                  │
│    refund_analysis               Refund rate trends by product             │
│    product_channel_mix           Channel dependency per product             │
│                                                                             │
│  CUSTOMER                                                                   │
│    user_order_summary            LTV, tier, repeat status per user         │
│    acquisition_cohorts           60/90/180/365-day retention cohorts       │
│    repeat_session_analysis       New vs repeat CVR comparison              │
│    ltv_by_acquisition_channel    Channel LTV + repeat buyer rate           │
│    executive_kpis                Single-row company P&L summary            │
│                                                                             │
│  OPERATIONAL INTELLIGENCE                                                   │
│    rolling_kpis                  7/30/90-day rolling averages              │
│    period_over_period            WoW / MoM / YoY change per metric        │
│    executive_scorecard           Monthly RAG-flagged composite score       │
│                                                                             │
│  ML OUTPUT                                                                  │
│    session_conversion_predictions  Propensity scores per session           │
│    customer_segments               KMeans segments per user                │
│    ltv_predictions                 Predicted 90/180/365-day LTV            │
└──────────────────────────────┬──────────────────────────────────────────────┘
                               │
              ┌────────────────┴────────────────┐
              │   GOVERNANCE LAYER              │   SEMANTIC LAYER
              │   mff_governance.*              │   mff_semantic.*
              │   metric_registry               │   v_marketing_performance
              │   report_catalog                │   v_product_performance
              │   data_lineage                  │   v_customer_360
              └────────────────┬────────────────┘   v_executive_scorecard
                               │
┌──────────────────────────────▼──────────────────────────────────────────────┐
│  PRESENTATION LAYER — Databricks SQL Dashboard (5 pages, 24 named queries) │
│                                                                             │
│  Page 1: Executive Overview     KPI tiles, 3-year revenue trend            │
│  Page 2: Marketing & Funnel     CVR analysis, A/B tests, drop-off          │
│  Page 3: Product Analytics      P&L, cross-sell matrix, refund trends      │
│  Page 4: Customer & ML          Cohorts, LTV, propensity segments          │
│  Page 5: Operational Monitor    Anomaly detection, RAG health, alerts      │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Repository Structure

```
ecommerce-intelligence-platform/
│
├── notebooks/
│   ├── quality/
│   │   └── 00_data_quality_framework.py   ← DQ audit, pipeline observability
│   ├── bronze/
│   │   └── 01_bronze_ingestion.py         ← Raw ingestion, schema enforcement
│   ├── silver/
│   │   ├── 02_silver_sessions.py          ← Channel classification, funnel pivot
│   │   └── 03_silver_orders_revenue.py    ← Revenue, margin, refund enrichment
│   ├── gold/
│   │   ├── 04_gold_funnel_conversion.py   ← CVR, A/B tests, channel ROI
│   │   ├── 05_gold_product_revenue.py     ← Product P&L, cross-sell, launch
│   │   ├── 06_gold_customer_cohorts.py    ← Retention cohorts, LTV
│   │   └── 08_enhanced_gold_layer.py      ← Rolling KPIs, scorecards, anomalies
│   ├── ml/
│   │   ├── 07_ml_conversion_model.py      ← Conversion prediction (GBT + LR)
│   │   └── 07b_ml_ltv_prediction.py       ← LTV regression (RF, Ridge, GBT)
│   ├── pipeline/
│   │   └── 10_incremental_pipeline.py     ← Watermark-based incremental loads
│   └── advanced/
│       └── 09_delta_advanced.py           ← MERGE, CDF, ZORDER, time travel
│
├── dashboards/
│   └── maven-fuzzy-factory/
│       ├── queries/
│       │   ├── executive/                 ← 6 executive SQL queries
│       │   ├── funnel/                    ← 9 funnel & marketing queries
│       │   ├── products/                  ← 9 product analytics queries
│       │   └── customer_ml/              ← 9 customer & ML queries
│       └── dashboard-config.json
│
├── docs/
│   └── ARCHITECTURE.md
│
├── sql/
│   └── 08_dashboard_sql.sql              ← All 24 named dashboard queries
│
└── README.md
```

---

## Notebook Reference

| # | Notebook | Layer | Key Techniques | Output |
|---|---|---|---|---|
| 00 | Data Quality Framework | Audit | DQ rule engine, pipeline watermarks, health scoring | `mff_audit.dq_results` |
| 01 | Bronze Ingestion | Bronze | Explicit schema enforcement, partition pruning, audit metadata | 6 Delta tables |
| 02 | Silver Sessions | Silver | 8-way channel taxonomy, window functions, funnel pivot (17 flags) | `sessions_enriched`, `session_funnel_steps` |
| 03 | Silver Orders | Silver | 5-way distributed join, margin calculation, cross-sell detection | `orders_enriched`, `order_items_enriched` |
| 04 | Gold Funnel | Gold | CVR aggregation, A/B test analysis, MoM lag windows | 6 Gold tables |
| 05 | Gold Products | Gold | P&L aggregation, cross-sell attach rate, launch velocity | 5 Gold tables |
| 06 | Gold Cohorts | Gold | Self-join cohort retention, LTV attribution, repeat analysis | 5 Gold tables |
| 07 | ML Conversion | ML | GBT + LR baseline, RandomizedSearchCV, class imbalance, MLflow | `session_conversion_predictions` |
| 07b | ML LTV | ML | Ridge/RF/GBT regression, permutation importance, MLflow model registry | `ltv_predictions` |
| 08 | Enhanced Gold | Gold | Rolling windows, period-over-period, composite scoring, anomaly detection | 4 Gold tables |
| 09 | Delta Advanced | Advanced | MERGE, CDC, ZORDER, time travel, schema evolution, VACUUM | Demonstrates Delta features |
| 10 | Incremental Pipeline | Pipeline | Watermark pattern, soft deletes, idempotent MERGE, lineage tracking | `mff_audit.pipeline_runs` |

---

## Data Scale

| Table | Rows | Size | Notes |
|---|---|---|---|
| website_pageviews | 1,188,124 | ~57 MB | Partitioned by year/month |
| website_sessions | 472,871 | ~42 MB | Partitioned by year/month |
| order_items | 40,025 | ~2 MB | Line-item grain |
| orders | 32,313 | ~2 MB | Transaction grain |
| order_item_refunds | 1,731 | ~74 KB | Refund events |
| products | 4 | <1 KB | Catalogue |
| **Total** | **1,734,068** | **~103 MB** | 3 years, 2012–2015 |

---

## Engineering Decisions

Every architectural choice in this platform was deliberate. Below is the reasoning behind the key decisions.

### Why Delta Lake over plain Parquet?

Plain Parquet is immutable — once written, you cannot update or delete records without rewriting entire partitions. In production analytics pipelines, late-arriving data, corrections, and incremental loads are the rule, not the exception. Delta Lake provides ACID transactions, enabling `MERGE` operations that upsert only changed records, `UPDATE` for corrections, and soft `DELETE` without physical data removal. The transaction log also gives you time travel (read any historical version) and Change Data Feed (stream row-level changes to downstream consumers) — both essential for audit compliance in governed analytics environments.

### Why Medallion over a flat architecture?

A single-layer architecture conflates three fundamentally different concerns: data fidelity (Bronze), business logic (Silver), and analytical consumption (Gold). Mixing them creates brittle pipelines where a business rule change forces a full re-ingestion. The medallion pattern enforces separation of concerns: Bronze preserves the source record exactly as received; Silver applies validated, version-controlled business logic; Gold serves pre-aggregated, consumer-ready datasets. This means the 1.18M pageview rows are pivoted into 472K session-level funnel flags once in Silver — every Gold table reads the pre-computed Silver view, not raw pageviews.

### Why RandomizedSearchCV over GridSearchCV?

The original implementation used `GridSearchCV` with 486 hyperparameter combinations × 5-fold cross-validation = **2,430 model fits** on 378,000 rows. On single-node sklearn, this runs for 15–30 minutes. `RandomizedSearchCV` with `n_iter=12` and 3-fold CV produces **36 model fits** — a 67× reduction in compute with statistically comparable results, because hyperparameter space exploration follows diminishing returns beyond ~10–20 random samples.

### Why PR-AUC over ROC-AUC for the conversion model?

The session-to-order conversion rate is approximately 6.8%, creating a **13:1 class imbalance**. ROC-AUC weights both classes equally — a model predicting "no conversion" for every session achieves misleadingly high ROC-AUC because it correctly classifies 93% of negatives. Precision-Recall AUC focuses exclusively on the minority class — the converters — which is the population that drives business value.

### Why a stratified sample for ML training?

A stratified 20% sample (~94K rows) samples each class separately to preserve the exact 13:1 imbalance ratio. The resulting model generalizes identically to the full population — the learning curve for GBT flattens well before 94K samples on a 7-feature problem. The full 472K rows are used only for inference, where `predict_proba()` is fast and parallelizable.

### Why a semantic layer on top of Gold?

Business analysts and dashboard consumers should not need to understand `website_session_id` or `is_repeat_session`. The semantic layer (`mff_semantic.*`) exposes business-friendly views with renamed columns, pre-joined combinations, and documented business purposes — the same pattern as Power BI certified datasets, SSAS tabular models, or Looker LookML.

### Why partition by year/month on pageviews?

Without partitioning, every query against `website_pageviews` scans all 1.18M rows. With `pv_year` and `pv_month` partitions, a query filtering to a single month reads ~33K rows instead — a 36× I/O reduction. Combined with `ZORDER BY (marketing_channel, device_type)` on sessions, predicate pushdown skips the vast majority of files for channel/device filter queries.

---

## Machine Learning

### Model A — Session Conversion Prediction

| Component | Decision | Rationale |
|---|---|---|
| Algorithm | GradientBoostingClassifier | Handles non-linear interactions between channel, device, time |
| Baseline | LogisticRegression (C=0.1) | Fast sanity check — establishes signal floor before GBT |
| Imbalance | `compute_sample_weight('balanced')` | Penalizes minority class misclassification 13× more |
| Search | RandomizedSearchCV (n_iter=12, 3-fold) | 67× faster than exhaustive grid |
| Primary metric | PR-AUC (average precision) | Correct metric for 13:1 imbalanced classification |
| Features | Channel, device, hour, day-of-week, is_repeat | All available at session start — no target leakage |
| Excluded | funnel_depth, saw_cart, saw_billing | Post-outcome indicators — inflate AUC to ~1.0 artificially |
| Tracking | MLflow | Params, metrics, model artifact logged per run |

### Model B — Customer LTV Prediction

| Model | Notes |
|---|---|
| Ridge Regression | Interpretable baseline, low variance |
| Random Forest | Captures non-linear channel × product interactions |
| Gradient Boosting | Strongest generalization — selected for production |

All three logged to MLflow with a comparison run. Output: `predicted_ltv_90d`, `predicted_ltv_180d`, `predicted_ltv_365d`, `recommended_action` per customer.

### Model C — Customer Segmentation (KMeans k=4)

`customer_tier` excluded from features — it is `gross_lifetime_value` bucketed, making its inclusion circular. k=4 justified empirically via elbow method (k=2..6). Evaluated with silhouette score.

---

## BI Governance Framework

### Metric Registry
Every KPI has a certified definition: `calculation_logic`, `source_table`, `owner_team`, `refresh_cadence`, `approved_by`, `approval_date`, `is_certified`. Prevents metric fragmentation across teams.

### Data Lineage
Every transformation tracked: `source_table → transformation_type → target_table` with business rule and originating notebook. Enables immediate impact analysis when upstream logic changes.

### Data Quality Audit
Every pipeline run logs: `check_name`, `records_checked`, `records_failed`, `failure_rate_pct`, `severity`. Pipeline health score (0–100) computed as weighted average. CRITICAL failures above 1% threshold block Silver/Gold updates.

### Report Catalog
All 24 dashboard queries catalogued with governance tier, refresh SLA, certified metrics used, owner, and next review date. Overdue reviews flagged automatically.

---

## Delta Lake Capabilities Demonstrated

| Feature | Business Purpose |
|---|---|
| ACID transactions | Every pipeline run is atomic — partial failures don't corrupt tables |
| Time travel | Read any historical version for audit or rollback |
| MERGE (upsert) | Incremental loads without full table rewrites |
| OPTIMIZE + ZORDER | File compaction + co-location for fast filter queries |
| Change Data Feed | Row-level CDC for downstream incremental consumers |
| Schema evolution | Add columns without downtime or reprocessing |
| VACUUM | Storage cost management |
| ANALYZE TABLE | Column statistics for query planner optimization |
| Partition pruning | 36× I/O reduction on pageview queries |
| Watermark pattern | Incremental loads process only new records |

---

## Key Business Insights

**Conversion & Funnel**
- Biggest funnel drop-off: 28.8% of desktop paid search sessions never reach /products
- /lander-5 outperforms /home baseline by +2.5pp CVR
- /billing-2 delivers +4.3pp conversion lift over /billing
- Mobile converts at 3.9% vs desktop at 7.9% — 2× gap signals mobile UX opportunity

**Channel & Attribution**
- gsearch nonbrand drives 78% of gross revenue but acquires lowest-LTV customers ($50 average)
- Brand SEM acquires customers with 2.6× higher LTV ($82 average) — most capital-efficient channel
- Repeat sessions convert at 8.2% vs 3.8% for new sessions — 2.2× more valuable per session

**Product & Margin**
- Mr. Fuzzy: highest revenue ($1.22M), highest refund rate (8.9%) — net margin lower than gross suggests
- Love Bear: highest gross margin (64.1%), 3.7% refund rate — most profitable on net basis
- Mr. Fuzzy → Love Bear cross-sell attach rate: 7.2% — highest-value upsell pair

**Customer Retention**
- March 2012 cohort: 22.1% 365-day retention — highest in the dataset
- Champion segment (12% of users) generates 34% of total revenue — high concentration, high priority

---

## Technology Stack

| Layer | Technology |
|---|---|
| Compute | Databricks Free Edition (Serverless) |
| Storage format | Delta Lake 3.0 |
| Processing | Apache Spark 3.4 (PySpark) |
| ML tracking | MLflow 2.x |
| ML library | scikit-learn 1.3 |
| Data manipulation | pandas, numpy |
| SQL | Databricks SQL (ANSI + Delta extensions) |
| Version control | Git / GitHub |

---

## Connecting BI Tools

### Power BI Direct Query
```
Server:    <workspace-id>.azuredatabricks.net
HTTP Path: /sql/1.0/warehouses/<warehouse-id>
Database:  mff_gold
Auth:      Personal Access Token
```

Recommended certified datasets: `mff_semantic.v_marketing_performance`, `mff_semantic.v_executive_scorecard`, `mff_semantic.v_customer_360`

---

## Author

**Erick Kiprotich Yegon, PhD**
Independent AI & Data Science Consultant · Richmond, KY

[GitHub](https://github.com/erickyegon) · [LinkedIn](https://linkedin.com/in/erickyegon) · [ORCID](https://orcid.org/0000-0002-7055-4848)

17+ years in data science, analytics engineering, and implementation science. Former Global Director of Data Science & Analytics at Living Goods — 25-person team, Kenya/Uganda/Burkina Faso, 8.5M+ individuals served. 30+ peer-reviewed publications (h-index 10). EB-1A Extraordinary Ability designation.

---

*Built on Databricks Free Edition · Delta Lake · Apache Spark · MLflow · scikit-learn*
