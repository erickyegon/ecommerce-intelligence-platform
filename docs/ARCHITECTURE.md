# Maven Fuzzy Factory - Technical Architecture

## System Overview

This platform implements a modern data lakehouse architecture using Databricks, Delta Lake, and Unity Catalog.

## Medallion Architecture

### Bronze Layer (Raw)
- **Purpose**: Ingest raw data with minimal transformation
- **Technology**: Delta Lake with Auto-Loader
- **Features**:
  - Schema evolution enabled
  - Audit columns (_load_ts, _source_file)
  - Exactly-once processing via checkpointing
  - Full data lineage tracking

### Silver Layer (Enriched)
- **Purpose**: Cleaned, deduplicated, business-ready data
- **Transformations**:
  - Marketing attribution (UTM parsing)
  - Session enrichment (time features, device classification)
  - Revenue calculations (gross, net, margins)
  - Data quality validations

### Gold Layer (Analytics)
- **Purpose**: Aggregated business metrics and KPIs
- **Outputs**:
  - Conversion funnels
  - Product performance dashboards
  - Customer cohort analysis
  - RFM segmentation

## ML Pipeline Architecture

### Training Pipeline
1. **Feature Engineering**: Extract non-leaky features from silver tables
2. **Data Split**: Stratified train/test split (80/20) BEFORE preprocessing
3. **Preprocessing**: Fit transformers on training data only
4. **Model Training**: GridSearchCV with 5-fold CV
5. **Evaluation**: Comprehensive metrics (ROC-AUC, PR-AUC, confusion matrix)

### Inference Pipeline
1. **Feature Extraction**: Same pipeline as training
2. **Preprocessing**: Transform using training statistics
3. **Scoring**: Batch predictions on full dataset
4. **Output**: Propensity scores written to gold layer

### Key ML Fixes Applied
- ✅ **Target Leakage Removed**: Excluded funnel features that reveal outcome
- ✅ **Preprocessing Leakage Fixed**: Split data before fitting scalers
- ✅ **Class Imbalance Handled**: Sample weights for 13.6:1 ratio
- ✅ **Strong Regularization**: 486 hyperparameter combinations tested

## Data Flow

```
Source CSV Files
    ↓ (Auto-Loader streaming)
Bronze Tables (mff_bronze.*)
    ↓ (Spark transformations)
Silver Tables (mff_silver.*)
    ↓ (Aggregations)
Gold Tables (mff_gold.*)
    ↓ (scikit-learn models)
ML Predictions (mff_gold.session_conversion_predictions)
```

## Technology Stack

* **Compute**: Databricks Serverless
* **Storage**: Delta Lake
* **Catalog**: Unity Catalog
* **ML**: scikit-learn (serverless compatible)
* **Languages**: Python, SQL
* **BI**: Databricks SQL, Lakeview Dashboards

## Performance Considerations

* **Incremental Processing**: Checkpoints for streaming ingestion
* **Partitioning**: Not used (small dataset < 1GB)
* **Z-Ordering**: Applied on high-cardinality columns
* **Caching**: Avoided (serverless compatibility)
* **Broadcast Joins**: Used for small dimension tables

## Security & Governance

* **Unity Catalog**: All tables registered
* **Data Lineage**: Tracked via catalog
* **Access Control**: Table/column-level permissions
* **Audit Logs**: _load_ts columns for tracking
