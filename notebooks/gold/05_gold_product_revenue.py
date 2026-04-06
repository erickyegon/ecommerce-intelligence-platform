# Databricks notebook source
# ══════════════════════════════════════════════════════════════════════════════
# Maven Fuzzy Factory | E-Commerce Intelligence Platform
# Notebook 05 — GOLD LAYER: Product Revenue, Cross-Sell & Refund Analytics
# ══════════════════════════════════════════════════════════════════════════════
#
#  Gold tables produced:
#    ① product_monthly_performance  — Revenue, margin, refunds per product per month
#    ② product_launch_impact        — Sales velocity in N months after each launch
#    ③ cross_sell_matrix            — Which products are bought together most?
#    ④ refund_analysis              — Refund rate trends, days-to-refund, by product
#    ⑤ product_channel_mix          — Which channels drive each product's sales?
# ══════════════════════════════════════════════════════════════════════════════

# COMMAND ----------

# DBTITLE 1,Cell 2
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, count, sum as spark_sum,
    avg, round as spark_round, when, coalesce,
    year, month, date_format, lag,
    max as spark_max, min as spark_min,
    current_timestamp, row_number
)
from pyspark.sql.window import Window
import logging

spark = SparkSession.builder.appName("MFF_Gold_Products").getOrCreate()
logger = logging.getLogger("MFF.Gold.Products")

SILVER_DB = "mff_silver"
GOLD_DB   = "mff_gold"

orders = spark.table(f"{SILVER_DB}.orders_enriched")
items  = spark.table(f"{SILVER_DB}.order_items_enriched")

def save_gold(df, name):
    (df.write.format("delta").mode("overwrite")
        .option("mergeSchema","true")
        .saveAsTable(f"{GOLD_DB}.{name}"))
    print(f"  ✔  {GOLD_DB}.{name}  ({df.count():,} rows)")

# COMMAND ----------

# DBTITLE 1,Cell 3
# ════════════════════════════════════════════════════════════════════════════
# GOLD TABLE ① — Monthly Product Performance
#   Full P&L view per product per month: volume, revenue, margin, refunds
# ════════════════════════════════════════════════════════════════════════════

product_monthly = (
    orders
    .withColumn("year_month", date_format("order_ts", "yyyy-MM"))
    .groupBy("year_month", "order_year", "order_month",
             "primary_product_id", "primary_product_name")
    .agg(
        count("order_id")                                    .alias("orders"),
        spark_sum("items_purchased")                         .alias("items_sold"),
        spark_round(spark_sum("gross_revenue"),       2)     .alias("gross_revenue"),
        spark_round(spark_sum("gross_cogs"),          2)     .alias("total_cogs"),
        spark_round(spark_sum("gross_margin"),        2)     .alias("gross_margin"),
        spark_round(avg("gross_margin_pct"),          1)     .alias("avg_gross_margin_pct"),
        spark_round(spark_sum("total_refund_usd"),    2)     .alias("total_refunds"),
        spark_sum(when(col("has_refund"), 1).otherwise(0))   .alias("refunded_orders"),
        spark_round(spark_sum("net_revenue"),         2)     .alias("net_revenue"),
        spark_round(spark_sum("net_margin"),          2)     .alias("net_margin"),
        spark_round(avg("net_margin_pct"),            1)     .alias("avg_net_margin_pct"),
        spark_sum(when(col("is_cross_sell_order"), 1).otherwise(0)).alias("cross_sell_orders"),
    )
    .withColumn("refund_rate_pct",
        spark_round(col("refunded_orders") / col("orders") * 100, 2))
    .withColumn("cross_sell_rate_pct",
        spark_round(col("cross_sell_orders") / col("orders") * 100, 2))
    .orderBy("primary_product_name", "year_month")
)

# Add MoM revenue growth per product
window_prod_mom = Window.partitionBy("primary_product_name").orderBy("year_month")
product_monthly_mom = (
    product_monthly
    .withColumn("gross_revenue_prev_month", lag("gross_revenue", 1).over(window_prod_mom))
    .withColumn("revenue_mom_pct",
        spark_round(
            (col("gross_revenue") - col("gross_revenue_prev_month"))
            / col("gross_revenue_prev_month") * 100, 1))
    .withColumn("_gold_ts", current_timestamp())
)

save_gold(product_monthly_mom, "product_monthly_performance")
display(product_monthly_mom.filter(col("primary_product_name").isNotNull()).limit(20))

# COMMAND ----------

# ════════════════════════════════════════════════════════════════════════════
# GOLD TABLE ① — Monthly Product Performance
# ════════════════════════════════════════════════════════════════════════════

def save_gold(df, name):
    (df.write.format("delta").mode("overwrite")
        .option("mergeSchema","true")
        .saveAsTable(f"{GOLD_DB}.{name}"))
    print(f"  ✔  {GOLD_DB}.{name}  ({df.count():,} rows)")

product_monthly = (
    orders
    .withColumn("year_month", date_format("order_ts", "yyyy-MM"))
    .groupBy("year_month", "order_year", "order_month",
             "primary_product_id", "primary_product_name")
    .agg(
        count("order_id")                                    .alias("orders"),
        spark_sum("items_purchased")                         .alias("items_sold"),
        spark_round(spark_sum("gross_revenue"),       2)     .alias("gross_revenue"),
        spark_round(spark_sum("gross_cogs"),          2)     .alias("total_cogs"),
        spark_round(spark_sum("gross_margin"),        2)     .alias("gross_margin"),
        spark_round(avg("gross_margin_pct"),          1)     .alias("avg_gross_margin_pct"),
        spark_round(spark_sum("total_refund_usd"),    2)     .alias("total_refunds"),
        spark_sum(when(col("has_refund"), 1).otherwise(0))   .alias("refunded_orders"),
        spark_round(spark_sum("net_revenue"),         2)     .alias("net_revenue"),
        spark_round(spark_sum("net_margin"),          2)     .alias("net_margin"),
        spark_round(avg("net_margin_pct"),            1)     .alias("avg_net_margin_pct"),
        spark_sum(when(col("is_cross_sell_order"), 1).otherwise(0)).alias("cross_sell_orders"),
    )
    .withColumn("refund_rate_pct",
        spark_round(col("refunded_orders") / col("orders") * 100, 2))
    .withColumn("cross_sell_rate_pct",
        spark_round(col("cross_sell_orders") / col("orders") * 100, 2))
    .orderBy("primary_product_name", "year_month")
)

# Add MoM revenue growth per product
window_prod_mom = Window.partitionBy("primary_product_name").orderBy("year_month")
product_monthly_mom = (
    product_monthly
    .withColumn("gross_revenue_prev_month", lag("gross_revenue", 1).over(window_prod_mom))
    .withColumn("revenue_mom_pct",
        spark_round(
            (col("gross_revenue") - col("gross_revenue_prev_month"))
            / col("gross_revenue_prev_month") * 100, 1))
    .withColumn("_gold_ts", current_timestamp())
)

# Using the requested save function
save_gold(product_monthly_mom, "product_monthly_performance")

display(product_monthly_mom.filter(col("primary_product_name").isNotNull()).limit(20))

# COMMAND ----------



# COMMAND ----------

# ════════════════════════════════════════════════════════════════════════════
# GOLD TABLE ③ — Cross-Sell Matrix
#   For every primary product, which cross-sell product is purchased with it?
#   Computes attach rate and incremental revenue per cross-sell pair.
# ════════════════════════════════════════════════════════════════════════════

# Get cross-sell product name
products = spark.table("mff_bronze.products").select(
    col("product_id").alias("cross_sell_product_id"),
    col("product_name").alias("cross_sell_product_name")
)

cross_sell = (
    orders
    .filter(col("is_cross_sell_order") == True)
    .join(products, on="cross_sell_product_id", how="left")
    .groupBy("primary_product_name", "cross_sell_product_name")
    .agg(
        count("order_id")                          .alias("cross_sell_orders"),
        spark_round(spark_sum("gross_revenue"), 2) .alias("total_order_revenue"),
        spark_round(avg("gross_revenue"), 2)       .alias("avg_order_value"),
        spark_round(avg("gross_margin_pct"), 1)    .alias("avg_margin_pct"),
    )
    .withColumn("_gold_ts", current_timestamp())
    .orderBy("primary_product_name", col("cross_sell_orders").desc())
)

# Add attach rate: cross-sell orders / total orders for that primary product
primary_totals = (
    orders.groupBy("primary_product_name")
    .agg(count("order_id").alias("total_orders_for_product"))
)

cross_sell_with_rate = (
    cross_sell
    .join(primary_totals, on="primary_product_name", how="left")
    .withColumn("attach_rate_pct",
        spark_round(col("cross_sell_orders") / col("total_orders_for_product") * 100, 2))
    .orderBy("primary_product_name", col("cross_sell_orders").desc())
)

save_gold(cross_sell_with_rate, "cross_sell_matrix")
display(cross_sell_with_rate)

# COMMAND ----------

# ════════════════════════════════════════════════════════════════════════════
# GOLD TABLE ④ — Refund Deep-Dive
#   Refund rate by product × month, days-to-refund distribution,
#   channel-level refund patterns
# ════════════════════════════════════════════════════════════════════════════

# Item-level refund analysis (most granular)
refund_detail = (
    items
    .withColumn("year_month", date_format("item_ts", "yyyy-MM"))
    .groupBy("year_month", "product_name")
    .agg(
        count("order_item_id")                                .alias("items_sold"),
        spark_sum(col("is_refunded").cast("integer"))         .alias("items_refunded"),
        spark_round(spark_sum("item_refund_usd"), 2)          .alias("total_refund_usd"),
        spark_round(avg("price_usd"), 2)                      .alias("avg_sale_price"),
        spark_round(spark_sum("item_net_revenue"), 2)         .alias("net_revenue"),
        spark_round(spark_sum("item_net_margin"), 2)          .alias("net_margin"),
    )
    .withColumn("item_refund_rate_pct",
        spark_round(col("items_refunded") / col("items_sold") * 100, 2))
    .withColumn("_gold_ts", current_timestamp())
    .orderBy("product_name", "year_month")
)

save_gold(refund_detail, "refund_analysis")

# Days-to-refund stats per product
days_to_refund = (
    orders
    .filter(col("has_refund") == True)
    .groupBy("primary_product_name")
    .agg(
        count("order_id")                           .alias("refunded_orders"),
        spark_round(avg("days_to_refund"), 1)       .alias("avg_days_to_refund"),
        spark_min("days_to_refund")                 .alias("min_days_to_refund"),
        spark_max("days_to_refund")                 .alias("max_days_to_refund"),
        spark_round(spark_sum("total_refund_usd"),2).alias("total_refund_value"),
    )
    .withColumn("_gold_ts", current_timestamp())
)

print("\n── Days to Refund by Product ─────────────────────")
display(days_to_refund)

# COMMAND ----------

# ════════════════════════════════════════════════════════════════════════════
# GOLD TABLE ⑤ — Product × Channel Mix
#   Which channels drive which products? Are some products dominated by
#   a single paid channel (high dependency risk)?
# ════════════════════════════════════════════════════════════════════════════

product_channel_mix = (
    orders
    .filter(col("primary_product_name").isNotNull())
    .groupBy("primary_product_name", "session_channel", "session_channel_group", "session_device_type")
    .agg(
        count("order_id")                                    .alias("orders"),
        spark_round(spark_sum("gross_revenue"),      2)      .alias("gross_revenue"),
        spark_round(spark_sum("net_margin"),         2)      .alias("net_margin"),
        spark_round(avg("gross_margin_pct"),         1)      .alias("avg_margin_pct"),
        spark_sum(when(col("has_refund"), 1).otherwise(0))   .alias("refund_count"),
    )
    .withColumn("_gold_ts", current_timestamp())
    .orderBy("primary_product_name", col("orders").desc())
)

# Add within-product channel share
total_by_product = (
    orders.groupBy("primary_product_name")
    .agg(count("order_id").alias("product_total_orders"))
)

product_channel_final = (
    product_channel_mix
    .join(total_by_product, on="primary_product_name", how="left")
    .withColumn("channel_order_share_pct",
        spark_round(col("orders") / col("product_total_orders") * 100, 1))
)

save_gold(product_channel_final, "product_channel_mix")
display(product_channel_final.orderBy("primary_product_name", col("orders").desc()))

# COMMAND ----------

print("""
✅  Notebook 05 — Gold Products COMPLETE

    mff_gold.product_monthly_performance  → Full P&L per product per month + MoM growth
    mff_gold.product_launch_impact        → Sales velocity binned by months after launch
    mff_gold.cross_sell_matrix            → Cross-sell pairs + attach rate %
    mff_gold.refund_analysis              → Refund rates, days-to-refund by product
    mff_gold.product_channel_mix          → Channel dependency per product

    Next → Run 06_gold_customer_cohorts.py
""")