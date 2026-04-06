# Databricks notebook source
# ══════════════════════════════════════════════════════════════════════════════
# Maven Fuzzy Factory | E-Commerce Intelligence Platform
# Notebook 03 — SILVER LAYER: Orders, Revenue & Refund Enrichment
# ══════════════════════════════════════════════════════════════════════════════
#
#  Reads  : mff_bronze.orders
#           mff_bronze.order_items
#           mff_bronze.order_item_refunds
#           mff_bronze.products
#           mff_silver.sessions_enriched  (for attribution join)
#
#  Writes : mff_silver.orders_enriched        — one row per order
#           mff_silver.order_items_enriched   — one row per line item
#
#  Enrichments:
#    ① Gross revenue, COGS, gross margin, margin %
#    ② Refund joins → net revenue, net margin
#    ③ Cross-sell detection (multi-item orders)
#    ④ Marketing attribution joined from sessions
#    ⑤ Days-to-refund calculation
#    ⑥ Product name join (human-readable)
# ══════════════════════════════════════════════════════════════════════════════

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, lit, coalesce,
    when, round as spark_round,
    datediff, year, month, quarter,
    sum as spark_sum, count, max as spark_max,
    row_number, current_timestamp
)
from pyspark.sql.window import Window
import logging

spark = SparkSession.builder.appName("MFF_Silver_Orders").getOrCreate()
logger = logging.getLogger("MFF.Silver.Orders")

SILVER_DB   = "mff_silver"
SILVER_PATH = "/delta/maven_fuzzy_factory/silver"

# COMMAND ----------

# ════════════════════════════════════════════════════════════════════════════
# STEP 1 — Load all source tables
# ════════════════════════════════════════════════════════════════════════════

df_orders   = spark.table("mff_bronze.orders").withColumn("order_ts", to_timestamp("created_at"))
df_items    = spark.table("mff_bronze.order_items").withColumn("item_ts", to_timestamp("created_at"))
df_refunds  = spark.table("mff_bronze.order_item_refunds").withColumn("refund_ts", to_timestamp("created_at"))
df_products = spark.table("mff_bronze.products").withColumn("product_launch_ts", to_timestamp("created_at"))
df_sessions = spark.table(f"{SILVER_DB}.sessions_enriched")

print(f"Orders     : {df_orders.count():,}")
print(f"Line items : {df_items.count():,}")
print(f"Refunds    : {df_refunds.count():,}")

# COMMAND ----------

# ════════════════════════════════════════════════════════════════════════════
# STEP 2 — Order-level refund aggregation
#   A single order can have multiple refunded items. Aggregate to order level.
# ════════════════════════════════════════════════════════════════════════════

refunds_by_order = (
    df_refunds.groupBy("order_id")
    .agg(
        spark_sum("refund_amount_usd")        .alias("total_refund_usd"),
        count("order_item_refund_id")         .alias("refund_item_count"),
        spark_max("refund_ts")                .alias("last_refund_ts"),
    )
    .withColumn("has_refund", lit(True))
)

# COMMAND ----------

# ════════════════════════════════════════════════════════════════════════════
# STEP 3 — Order-level cross-sell flag
#   Multi-item orders reveal which products are bought together.
# ════════════════════════════════════════════════════════════════════════════

items_by_order = (
    df_items.groupBy("order_id")
    .agg(
        count("order_item_id")            .alias("line_item_count"),
        spark_sum("price_usd")            .alias("items_gross_revenue"),
        spark_sum("cogs_usd")             .alias("items_total_cogs"),
        spark_max(when(col("is_primary_item") == 1, col("product_id")))
            .alias("primary_product_id_items"),
        spark_max(when(col("is_primary_item") == 0, col("product_id")))
            .alias("cross_sell_product_id"),
    )
    .withColumn("is_cross_sell_order",
        col("cross_sell_product_id").isNotNull())
)

# COMMAND ----------

# DBTITLE 1,Cell 6
# ════════════════════════════════════════════════════════════════════════════
# STEP 4 — Build enriched orders table
#   Join orders → items rollup → refunds → sessions (attribution)
# ════════════════════════════════════════════════════════════════════════════

# Session attribution columns we want to carry
session_cols = df_sessions.select(
    "website_session_id",
    col("marketing_channel").alias("session_channel"),
    col("channel_group").alias("session_channel_group"),
    col("utm_source").alias("session_utm_source"),
    col("utm_campaign").alias("session_utm_campaign"),
    col("device_type").alias("session_device_type"),
    col("is_repeat_session").alias("session_is_repeat"),
    col("acquisition_channel"),
)

df_orders_enriched = (
    df_orders
    # Join items rollup
    .join(items_by_order, on="order_id", how="left")
    # Join refunds rollup
    .join(refunds_by_order, on="order_id", how="left")
    # Join session attribution
    .join(session_cols.hint("broadcast"), on="website_session_id", how="left")
    # Join product name for primary product
    .join(
        df_products.select(
            col("product_id").alias("primary_product_id"),
            col("product_name").alias("primary_product_name"),
            col("product_launch_ts")
        ),
        on="primary_product_id", how="left"
    )
    # Fill nulls from left joins
    .withColumn("total_refund_usd",    coalesce(col("total_refund_usd"),    lit(0.0)))
    .withColumn("refund_item_count",   coalesce(col("refund_item_count"),   lit(0)))
    .withColumn("has_refund",          coalesce(col("has_refund"),          lit(False)))
    .withColumn("cross_sell_product_id", coalesce(col("cross_sell_product_id"), lit(None)))

    # Revenue & margin calculations
    .withColumn("gross_revenue",       spark_round(col("price_usd"),                           2))
    .withColumn("gross_cogs",          spark_round(col("cogs_usd"),                             2))
    .withColumn("gross_margin",        spark_round(col("price_usd") - col("cogs_usd"),          2))
    .withColumn("gross_margin_pct",    spark_round(
        (col("price_usd") - col("cogs_usd")) / col("price_usd") * 100, 1))
    .withColumn("net_revenue",         spark_round(col("price_usd") - col("total_refund_usd"),  2))
    .withColumn("net_margin",          spark_round(
        col("price_usd") - col("cogs_usd") - col("total_refund_usd"),  2))
    .withColumn("net_margin_pct",      spark_round(
        when(col("price_usd") > 0,
            (col("price_usd") - col("cogs_usd") - col("total_refund_usd")) / col("price_usd") * 100
        ).otherwise(lit(0.0)), 1))

    # Refund timing
    .withColumn("days_to_refund",
        when(col("has_refund"),
            datediff(col("last_refund_ts"), col("order_ts"))
        ).otherwise(lit(None)))

    # Temporal
    .withColumn("order_year",    year("order_ts"))
    .withColumn("order_quarter", quarter("order_ts"))
    .withColumn("order_month",   month("order_ts"))

    # Items after launch: how many months after product launch was this order?
    .withColumn("months_after_product_launch",
        when(col("product_launch_ts").isNotNull(),
            (
                (year("order_ts") * 12 + month("order_ts")) -
                (year("product_launch_ts") * 12 + month("product_launch_ts"))
            )
        ).otherwise(lit(None)))
)

# Final column selection
order_final_cols = [
    "order_id", "order_ts", "order_year", "order_quarter", "order_month",
    "website_session_id", "user_id",
    "primary_product_id", "primary_product_name", "product_launch_ts",
    "months_after_product_launch",
    "items_purchased", "line_item_count", "is_cross_sell_order", "cross_sell_product_id",
    "gross_revenue", "gross_cogs", "gross_margin", "gross_margin_pct",
    "total_refund_usd", "refund_item_count", "has_refund", "days_to_refund",
    "net_revenue", "net_margin", "net_margin_pct",
    "session_channel", "session_channel_group",
    "session_utm_source", "session_utm_campaign", "session_device_type",
    "session_is_repeat", "acquisition_channel",
]

df_orders_final = df_orders_enriched.select(order_final_cols)

(df_orders_final.write.format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .partitionBy("order_year", "order_month")
    .saveAsTable(f"{SILVER_DB}.orders_enriched"))

print(f"✔  {SILVER_DB}.orders_enriched  ({df_orders_final.count():,} rows)")

# COMMAND ----------

# DBTITLE 1,Cell 7
# ════════════════════════════════════════════════════════════════════════════
# STEP 5 — Line-item level enrichment (for product-level refund analysis)
# ════════════════════════════════════════════════════════════════════════════

df_items_enriched = (
    df_items
    .join(
        df_products.select("product_id", "product_name", col("created_at").alias("product_launch_date")),
        on="product_id", how="left"
    )
    .join(
        df_refunds.select(
            "order_item_id",
            col("refund_amount_usd").alias("item_refund_usd"),
            col("refund_ts")
        ),
        on="order_item_id", how="left"
    )
    .withColumn("is_refunded",      col("item_refund_usd").isNotNull())
    .withColumn("item_net_revenue", spark_round(
        col("price_usd") - coalesce(col("item_refund_usd"), lit(0.0)), 2))
    .withColumn("item_net_margin",  spark_round(
        col("price_usd") - col("cogs_usd") - coalesce(col("item_refund_usd"), lit(0.0)), 2))
    .withColumn("item_margin_pct",  spark_round(
        (col("price_usd") - col("cogs_usd")) / col("price_usd") * 100, 1))
    .withColumn("is_primary_item",  col("is_primary_item").cast("boolean"))
    .withColumn("item_year",        year("item_ts"))
    .withColumn("item_month",       month("item_ts"))
)

item_final_cols = [
    "order_item_id", "item_ts", "item_year", "item_month",
    "order_id", "product_id", "product_name", "product_launch_date",
    "is_primary_item",
    "price_usd", "cogs_usd",
    "item_refund_usd", "refund_ts", "is_refunded",
    "item_net_revenue", "item_net_margin", "item_margin_pct",
]

(df_items_enriched.select(item_final_cols)
    .write.format("delta")
    .mode("overwrite")
    .saveAsTable(f"{SILVER_DB}.order_items_enriched"))

print(f"✔  {SILVER_DB}.order_items_enriched  ({df_items_enriched.count():,} rows)")

# COMMAND ----------

# Quick sanity checks
display(spark.sql(f"""
    SELECT primary_product_name,
           COUNT(*)                        AS orders,
           ROUND(AVG(gross_margin_pct),1)  AS avg_margin_pct,
           SUM(CASE WHEN has_refund THEN 1 ELSE 0 END) AS refund_count,
           ROUND(AVG(net_margin),2)        AS avg_net_margin
    FROM {SILVER_DB}.orders_enriched
    GROUP BY primary_product_name
    ORDER BY orders DESC
"""))

print("""
✅  Notebook 03 — Silver Orders COMPLETE
    mff_silver.orders_enriched       → Revenue, margin, refund, attribution per order
    mff_silver.order_items_enriched  → Line-item refund & margin analysis
    Next → Run 04_gold_funnel_conversion.py
""")