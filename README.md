# Maven Fuzzy Factory | E-Commerce Intelligence Platform

A comprehensive end-to-end e-commerce analytics and machine learning platform built on Databricks, implementing the Medallion Architecture (Bronze → Silver → Gold) with advanced ML models for conversion prediction and customer segmentation.

## 🏗️ Architecture

```
Data Flow: Raw CSV → Bronze (Delta) → Silver (Enriched) → Gold (Analytics) → ML Models
```

### Medallion Layers

* **Bronze**: Raw data ingestion with schema validation
* **Silver**: Cleaned, enriched, and deduplicated data
* **Gold**: Business-ready aggregates and KPI tables
* **ML**: Predictive models for conversion and customer LTV

## 📁 Project Structure

```
ecommerce-intelligence-platform/
├── notebooks/
│   ├── bronze/          # Data ingestion layer
│   │   └── 01_bronze_ingestion.py
│   ├── silver/          # Data transformation layer
│   │   ├── 02_silver_sessions.py
│   │   └── 03_silver_orders_revenue.py
│   ├── gold/            # Analytics layer
│   │   ├── 04_gold_funnel_conversion.py
│   │   ├── 05_gold_product_revenue.py
│   │   └── 06_gold_customer_cohorts.py
│   ├── ml/              # Machine learning models
│   │   └── 07_ml_conversion_model.py
│   └── advanced/        # Advanced features
│       └── 09_delta_advanced.py
├── sql/
│   └── 08_dashboard_sql.sql
├── docs/
└── config/
```

## 🎯 Key Features

### 1. Bronze Layer - Data Ingestion
* Auto-Loader for streaming CSV ingestion
* Schema evolution and validation
* Raw data preservation with audit columns
* Checkpointing for exactly-once processing

### 2. Silver Layer - Data Quality
* **Sessions Enriched**:
  - Marketing channel attribution
  - Device type classification
  - Session time features (hour, day, weekend)
  - Repeat visitor identification
  - UTM parameter parsing
* **Orders Enriched**:
  - Revenue calculations (gross, net, margins)
  - Product revenue analysis
  - Cross-sell metrics
  - Refund tracking

### 3. Gold Layer - Business Analytics
* **Funnel Analysis**: Conversion rates by channel, device, campaign
* **Product Revenue**: Best/worst performers, cross-sell analysis
* **Customer Cohorts**: LTV, retention curves, RFM segmentation

### 4. Machine Learning Models

#### Model A: Session Conversion Prediction
* **Algorithm**: Gradient Boosted Trees (scikit-learn)
* **Target**: Binary classification (will session convert?)
* **Features**: 
  - Marketing channel, device type, channel group
  - Session hour, day of week, weekend indicator
  - Repeat session flag
* **Fixes Applied**:
  - ✅ Removed target leakage (funnel features)
  - ✅ Fixed preprocessing leakage (split before fit)
  - ✅ Handled class imbalance (13.6:1 ratio)
  - ✅ Strong regularization (486 hyperparameter combinations)
* **Performance**: ROC-AUC 0.65-0.80 (realistic, no leakage)
* **Use Case**: Real-time bid adjustment for high-propensity sessions

#### Model B: Customer Segmentation
* **Algorithm**: KMeans clustering (k=4)
* **Features**: Total orders, LTV, AOV, refund rate, products ordered
* **Output**: Customer segments for personalized campaigns

### 5. Advanced Features
* Delta Lake time travel and versioning
* Data quality validation
* Incremental processing with checkpoints
* Unity Catalog integration

## 🚀 Quick Start

### Prerequisites
* Databricks workspace (Serverless or standard cluster)
* Unity Catalog enabled
* CSV data files in DBFS/Volumes

### Setup

1. **Configure data paths** in `01_bronze_ingestion.py`:
```python
BRONZE_PATH = "/Volumes/main/default/raw_data/"
```

2. **Run notebooks in order**:
```bash
01_bronze_ingestion.py       # Ingest raw CSV files
02_silver_sessions.py        # Transform sessions
03_silver_orders_revenue.py  # Transform orders
04_gold_funnel_conversion.py # Create funnel metrics
05_gold_product_revenue.py   # Create product analytics
06_gold_customer_cohorts.py  # Create customer cohorts
07_ml_conversion_model.py    # Train ML models
08_dashboard_sql.sql         # Create dashboards
09_delta_advanced.py         # Advanced Delta features
```

3. **Query analytics tables**:
```sql
-- View conversion funnel
SELECT * FROM mff_gold.funnel_conversion_summary;

-- View product revenue
SELECT * FROM mff_gold.product_revenue_analysis;

-- View ML predictions
SELECT * FROM mff_gold.session_conversion_predictions;
```

## 📊 Key Tables

### Bronze Layer
* `mff_bronze.website_sessions_raw`
* `mff_bronze.orders_raw`
* `mff_bronze.website_pageviews_raw`
* `mff_bronze.order_items_raw`
* `mff_bronze.products_raw`
* `mff_bronze.order_item_refunds_raw`

### Silver Layer
* `mff_silver.sessions_enriched`
* `mff_silver.orders_enriched`

### Gold Layer
* `mff_gold.funnel_conversion_summary`
* `mff_gold.product_revenue_analysis`
* `mff_gold.user_order_summary`
* `mff_gold.session_conversion_predictions` (ML output)
* `mff_gold.customer_segments` (ML output)

## 🔍 Business Insights Enabled

1. **Marketing Performance**:
   - Which channels drive highest conversion?
   - ROI by campaign and UTM parameters
   - Device-specific conversion patterns

2. **Product Analytics**:
   - Best/worst performing products
   - Cross-sell opportunities
   - Margin analysis

3. **Customer Intelligence**:
   - Lifetime value segmentation
   - Repeat purchase behavior
   - Churn risk identification

4. **Predictive Analytics**:
   - Real-time conversion probability scoring
   - High-value customer identification
   - Personalized targeting segments

## 🛠️ Technical Highlights

* **Serverless Compatible**: Works on Databricks Serverless (no PySpark ML dependencies)
* **Data Leakage Prevention**: Proper train/test splitting, no outcome indicators in features
* **Class Imbalance Handling**: Sample weights, appropriate metrics (ROC-AUC, PR-AUC)
* **Delta Lake**: Time travel, ACID transactions, schema evolution
* **Unity Catalog**: Governed data access, lineage tracking

## 📈 Performance Metrics

### ML Model Performance (After Fixes)
* **ROC-AUC**: 0.65-0.80 (realistic, no leakage)
* **PR-AUC**: 0.40-0.60 (better for imbalanced data)
* **Precision**: 0.15-0.30 (when predicting conversion)
* **Recall**: 0.50-0.70 (% of converters caught)
* **Class Balance**: 6.83% positive (13.6:1 imbalance handled)

### Data Volume
* **Sessions**: ~473K
* **Orders**: ~32K (6.83% conversion rate)
* **Training Set**: 378K samples
* **Test Set**: 94K samples

## 🔄 Data Pipeline Flow

```
CSV Files (DBFS/Volumes)
    ↓
Bronze Layer (Raw Delta Tables)
    ↓
Silver Layer (Enriched, Cleaned)
    ↓
Gold Layer (Analytics KPIs)
    ↓
ML Models (Predictions)
    ↓
Dashboards & BI Tools
```

## 📝 Notes

* **Lower ML metrics are GOOD**: Previous perfect 1.0 scores were due to data leakage. Current realistic scores (0.65-0.80 ROC-AUC) represent honest, generalizable performance.
* **Serverless Restrictions**: Converted from PySpark ML to scikit-learn for compatibility
* **Class Imbalance**: Handled with sample weights and appropriate metrics (not just accuracy)

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test on Databricks workspace
5. Submit a pull request

## 📄 License

MIT License - See LICENSE file for details

## 👤 Author

**Erick Yegon**  
Email: erickkiprotichyegon61@gmail.com  
GitHub: [@erickyegon](https://github.com/erickyegon)

## 🙏 Acknowledgments

* Built on Databricks platform
* Maven Fuzzy Factory dataset
* Medallion Architecture best practices

---

**Last Updated**: April 6, 2026  
**Status**: ✅ All notebooks tested and validated  
**Environment**: Databricks Serverless / AWS
