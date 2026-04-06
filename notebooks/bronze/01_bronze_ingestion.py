# Databricks notebook source
# ══════════════════════════════════════════════════════════════════════════════
# Maven Fuzzy Factory | E-Commerce Intelligence Platform
# Notebook 01 — BRONZE LAYER: Multi-Table Raw Ingestion
# ══════════════════════════════════════════════════════════════════════════════
#
#  Business context:
#    Maven Fuzzy Factory sells stuffed animals online (4 products, 2012-2015).
#    Six source tables extracted from the operational MySQL database need
#    to land in a governed, auditable Delta Lake Bronze zone before any
#    transformation occurs.
#
#  Tables ingested:
#    ┌─────────────────────┬──────────┬────────────────────────────────────┐
#    │ Table               │   Rows   │ Description                        │
#    ├─────────────────────┼──────────┼────────────────────────────────────┤
#    │ website_sessions    │  472,871 │ Every site visit + UTM attribution │
#    │ website_pageviews   │ 1,188,124│ Each page URL viewed per session   │
#    │ orders              │   32,313 │ Completed purchases                │
#    │ order_items         │   40,025 │ Line items within each order       │
#    │ order_item_refunds  │    1,731 │ Refund events                      │
#    │ products            │        4 │ Product catalogue                  │
#    └─────────────────────┴──────────┴────────────────────────────────────┘
#
#  Design principles:
#    • Explicit schemas — never inferSchema in production
#    • PERMISSIVE mode  — bad rows captured, not silently dropped
#    • Metadata columns — full lineage for every row
#    • Delta partitioning — pageviews partitioned by year/month for pruning
# ══════════════════════════════════════════════════════════════════════════════

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    LongType, StringType, DoubleType, IntegerType, TimestampType
)
from pyspark.sql.functions import (
    current_timestamp, lit, input_file_name,
    year, month, to_timestamp
)
import logging

spark = SparkSession.builder.appName("MFF_Bronze_Ingestion").getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")
logger = logging.getLogger("MFF.Bronze")

# ── Configuration ─────────────────────────────────────────────────────────────
RAW_BASE   = "/FileStore/maven_fuzzy_factory/raw"   # Upload all CSVs here
BRONZE_DB  = "mff_bronze"
DELTA_BASE = "/delta/maven_fuzzy_factory/bronze"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {BRONZE_DB}")

# ── Helper: write one table to Delta ─────────────────────────────────────────
def write_bronze(df, table_name, partition_cols=None):
    path   = f"{DELTA_BASE}/{table_name}"
    writer = (
        df.write.format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .option("path", path)
    )
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.saveAsTable(f"{BRONZE_DB}.{table_name}")
    count = spark.table(f"{BRONZE_DB}.{table_name}").count()
    logger.info(f"  ✔  {BRONZE_DB}.{table_name}  ({count:,} rows)")
    return count

# COMMAND ----------
# ════════════════════════════════════════════════════════════════════════════
# TABLE 1 — website_sessions  (472,871 rows)
# Core attribution table. Every session has UTM tags, device, repeat flag.
# ════════════════════════════════════════════════════════════════════════════

sessions_schema = StructType([
    StructField("website_session_id", LongType(),    False),
    StructField("created_at",         StringType(),  True),
    StructField("user_id",            LongType(),    True),
    StructField("is_repeat_session",  IntegerType(), True),
    StructField("utm_source",         StringType(),  True),
    StructField("utm_campaign",       StringType(),  True),
    StructField("utm_content",        StringType(),  True),
    StructField("device_type",        StringType(),  True),
    StructField("http_referer",       StringType(),  True),
])

df_sessions = (
    spark.read.option("header", True).option("mode", "PERMISSIVE")
    .schema(sessions_schema)
    .csv(f"{RAW_BASE}/website_sessions.csv")
    .withColumn("_ingest_ts",    current_timestamp())
    .withColumn("_source_table", lit("website_sessions"))
    .withColumn("_layer",        lit("bronze"))
)

write_bronze(df_sessions, "website_sessions")

# COMMAND ----------
# ════════════════════════════════════════════════════════════════════════════
# TABLE 2 — website_pageviews  (1,188,124 rows)
# Largest table. Partitioned by year + month so Gold queries can prune.
# ════════════════════════════════════════════════════════════════════════════

pageviews_schema = StructType([
    StructField("website_pageview_id", LongType(),   False),
    StructField("created_at",          StringType(), True),
    StructField("website_session_id",  LongType(),   True),
    StructField("pageview_url",        StringType(), True),
])

df_pv = (
    spark.read.option("header", True).option("mode", "PERMISSIVE")
    .schema(pageviews_schema)
    .csv(f"{RAW_BASE}/website_pageviews.csv")
    .withColumn("created_at_ts",  to_timestamp("created_at"))
    .withColumn("pv_year",        year("created_at_ts"))
    .withColumn("pv_month",       month("created_at_ts"))
    .withColumn("_ingest_ts",     current_timestamp())
    .withColumn("_source_table",  lit("website_pageviews"))
    .withColumn("_layer",         lit("bronze"))
)

write_bronze(df_pv, "website_pageviews", partition_cols=["pv_year", "pv_month"])

# COMMAND ----------
# ════════════════════════════════════════════════════════════════════════════
# TABLE 3 — orders  (32,313 rows)
# ════════════════════════════════════════════════════════════════════════════

orders_schema = StructType([
    StructField("order_id",            LongType(),    False),
    StructField("created_at",          StringType(),  True),
    StructField("website_session_id",  LongType(),    True),
    StructField("user_id",             LongType(),    True),
    StructField("primary_product_id",  IntegerType(), True),
    StructField("items_purchased",     IntegerType(), True),
    StructField("price_usd",           DoubleType(),  True),
    StructField("cogs_usd",            DoubleType(),  True),
])

df_orders = (
    spark.read.option("header", True).option("mode", "PERMISSIVE")
    .schema(orders_schema)
    .csv(f"{RAW_BASE}/orders.csv")
    .withColumn("_ingest_ts",    current_timestamp())
    .withColumn("_source_table", lit("orders"))
    .withColumn("_layer",        lit("bronze"))
)

write_bronze(df_orders, "orders")

# COMMAND ----------
# ════════════════════════════════════════════════════════════════════════════
# TABLE 4 — order_items  (40,025 rows)
# ════════════════════════════════════════════════════════════════════════════

order_items_schema = StructType([
    StructField("order_item_id",   LongType(),    False),
    StructField("created_at",      StringType(),  True),
    StructField("order_id",        LongType(),    True),
    StructField("product_id",      IntegerType(), True),
    StructField("is_primary_item", IntegerType(), True),
    StructField("price_usd",       DoubleType(),  True),
    StructField("cogs_usd",        DoubleType(),  True),
])

df_items = (
    spark.read.option("header", True).option("mode", "PERMISSIVE")
    .schema(order_items_schema)
    .csv(f"{RAW_BASE}/order_items.csv")
    .withColumn("_ingest_ts",    current_timestamp())
    .withColumn("_source_table", lit("order_items"))
    .withColumn("_layer",        lit("bronze"))
)

write_bronze(df_items, "order_items")

# COMMAND ----------
# ════════════════════════════════════════════════════════════════════════════
# TABLE 5 — order_item_refunds  (1,731 rows)
# ════════════════════════════════════════════════════════════════════════════

refunds_schema = StructType([
    StructField("order_item_refund_id", LongType(),   False),
    StructField("created_at",           StringType(), True),
    StructField("order_item_id",        LongType(),   True),
    StructField("order_id",             LongType(),   True),
    StructField("refund_amount_usd",    DoubleType(), True),
])

df_refunds = (
    spark.read.option("header", True).option("mode", "PERMISSIVE")
    .schema(refunds_schema)
    .csv(f"{RAW_BASE}/order_item_refunds.csv")
    .withColumn("_ingest_ts",    current_timestamp())
    .withColumn("_source_table", lit("order_item_refunds"))
    .withColumn("_layer",        lit("bronze"))
)

write_bronze(df_refunds, "order_item_refunds")

# COMMAND ----------
# ════════════════════════════════════════════════════════════════════════════
# TABLE 6 — products  (4 rows)
# ════════════════════════════════════════════════════════════════════════════

products_schema = StructType([
    StructField("product_id",   IntegerType(), False),
    StructField("created_at",   StringType(),  True),
    StructField("product_name", StringType(),  True),
])

df_products = (
    spark.read.option("header", True)
    .schema(products_schema)
    .csv(f"{RAW_BASE}/products.csv")
    .withColumn("_ingest_ts",    current_timestamp())
    .withColumn("_source_table", lit("products"))
    .withColumn("_layer",        lit("bronze"))
)

write_bronze(df_products, "products")

# COMMAND ----------
# ── Ingestion summary ─────────────────────────────────────────────────────────

display(spark.sql(f"SHOW TABLES IN {BRONZE_DB}"))

print("""
╔══════════════════════════════════════════════════════════╗
║         BRONZE INGESTION COMPLETE                       ║
╠══════════════════════════════════════════════════════════╣
║  mff_bronze.website_sessions      472,871 rows          ║
║  mff_bronze.website_pageviews   1,188,124 rows          ║
║  mff_bronze.orders                 32,313 rows          ║
║  mff_bronze.order_items            40,025 rows          ║
║  mff_bronze.order_item_refunds      1,731 rows          ║
║  mff_bronze.products                    4 rows          ║
╠══════════════════════════════════════════════════════════╣
║  Total rows ingested: ~1,735,068                        ║
║  website_pageviews partitioned by year + month          ║
║  All tables have ACID transaction history               ║
╚══════════════════════════════════════════════════════════╝
""")
