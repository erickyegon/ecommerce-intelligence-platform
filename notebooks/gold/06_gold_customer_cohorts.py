# Databricks notebook source
# ══════════════════════════════════════════════════════════════════════════════
# Maven Fuzzy Factory | E-Commerce Intelligence Platform
# Notebook 06 — GOLD LAYER: Customer Cohorts, Retention & Lifetime Value
# ══════════════════════════════════════════════════════════════════════════════
#
#  Gold tables produced:
#    ① user_order_summary        — Every user's order history: orders, spend, LTV
#    ② acquisition_cohorts       — Monthly cohorts: how many repeat within 60/90/180 days?
#    ③ repeat_session_analysis   — Repeat session behaviour vs new-session behaviour
#    ④ ltv_by_acquisition_channel— Which channel acquires the highest-LTV customers?
#    ⑤ executive_kpis            — Single-row company-wide summary for exec dashboard
# ══════════════════════════════════════════════════════════════════════════════

# COMMAND ----------

# DBTITLE 1,Cell 2
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, count, countDistinct,
    sum as spark_sum, avg, min as spark_min, max as spark_max,
    round as spark_round, when, coalesce,
    date_format, datediff, year, month,
    row_number, lag, first, current_date,
    current_timestamp, to_date
)
from pyspark.sql.window import Window
import logging

spark = SparkSession.builder.appName("MFF_Gold_Cohorts").getOrCreate()
logger = logging.getLogger("MFF.Gold.Cohorts")

SILVER_DB = "mff_silver"
GOLD_DB   = "mff_gold"

sessions = spark.table(f"{SILVER_DB}.sessions_enriched")
orders   = spark.table(f"{SILVER_DB}.orders_enriched")

def save_gold(df, name):
    (df.write.format("delta").mode("overwrite")
        .option("mergeSchema","true")
        .saveAsTable(f"{GOLD_DB}.{name}"))
    print(f"  ✔  {GOLD_DB}.{name}  ({df.count():,} rows)")

# COMMAND ----------

# ════════════════════════════════════════════════════════════════════════════
# GOLD TABLE ① — User Order History Summary
#   One row per user: all-time orders, spend, net value, first/last order
# ════════════════════════════════════════════════════════════════════════════

user_orders = (
    orders
    .groupBy("user_id")
    .agg(
        count("order_id")                                   .alias("total_orders"),
        spark_round(spark_sum("gross_revenue"), 2)          .alias("gross_lifetime_value"),
        spark_round(spark_sum("net_revenue"), 2)            .alias("net_lifetime_value"),
        spark_round(spark_sum("net_margin"), 2)             .alias("lifetime_margin"),
        spark_round(avg("gross_revenue"), 2)                .alias("avg_order_value"),
        spark_round(avg("net_margin_pct"), 1)               .alias("avg_net_margin_pct"),
        spark_min("order_ts")                               .alias("first_order_ts"),
        spark_max("order_ts")                               .alias("last_order_ts"),
        spark_sum(when(col("has_refund"),1).otherwise(0))   .alias("orders_with_refund"),
        spark_round(spark_sum("total_refund_usd"), 2)       .alias("total_refunds_received"),
        spark_sum(when(col("is_cross_sell_order"),1).otherwise(0)).alias("cross_sell_orders"),
        # Track which products they've bought
        countDistinct("primary_product_id")                 .alias("distinct_products_ordered"),
        first("acquisition_channel", ignorenulls=True)      .alias("acquisition_channel"),
    )
    .withColumn("days_since_first_order",
        datediff(col("last_order_ts"), col("first_order_ts")))
    .withColumn("is_repeat_buyer",
        (col("total_orders") > 1).cast("boolean"))
    .withColumn("customer_tier",
        when(col("gross_lifetime_value") >= 100, lit("High Value"))
        .when(col("gross_lifetime_value") >= 50,  lit("Mid Value"))
        .otherwise(lit("Single Purchase")))
    .withColumn("refund_rate_pct",
        spark_round(col("orders_with_refund") / col("total_orders") * 100, 1))
    .withColumn("_gold_ts", current_timestamp())
)

save_gold(user_orders, "user_order_summary")

print("\nCustomer Tier Distribution:")
display(user_orders.groupBy("customer_tier", "is_repeat_buyer")
    .agg(count("user_id").alias("customers"),
         spark_round(avg("gross_lifetime_value"),2).alias("avg_ltv"))
    .orderBy("customer_tier"))

# COMMAND ----------

# ════════════════════════════════════════════════════════════════════════════
# GOLD TABLE ② — Monthly Acquisition Cohorts
#   Standard cohort analysis: for users acquired in month X,
#   how many placed a 2nd order within 60, 90, 180, 365 days?
# ════════════════════════════════════════════════════════════════════════════

# Get first order per user (= acquisition event)
window_first_order = Window.partitionBy("user_id").orderBy("order_ts")

first_orders = (
    orders
    .withColumn("order_rank", row_number().over(window_first_order))
    .filter(col("order_rank") == 1)
    .select(
        "user_id",
        col("order_ts").alias("first_order_ts"),
        col("gross_revenue").alias("first_order_value"),
        col("acquisition_channel"),
        date_format("order_ts", "yyyy-MM").alias("acquisition_cohort"),
    )
)

# All orders (for repeat detection)
all_orders_slim = orders.select(
    "user_id",
    col("order_ts").alias("subsequent_order_ts"),
    col("gross_revenue").alias("subsequent_revenue")
)

# Self-join: for each user, find orders that came AFTER the first
repeat_orders = (
    first_orders
    .join(all_orders_slim, on="user_id", how="left")
    .filter(col("subsequent_order_ts") > col("first_order_ts"))
    .withColumn("days_to_repeat", datediff("subsequent_order_ts", "first_order_ts"))
)

# Flag repeats within windows
repeat_flags = (
    repeat_orders
    .groupBy("user_id", "acquisition_cohort", "first_order_ts",
             "first_order_value", "acquisition_channel")
    .agg(
        spark_min("days_to_repeat")  .alias("days_to_first_repeat"),
        count("subsequent_order_ts") .alias("repeat_order_count"),
        spark_sum("subsequent_revenue").alias("repeat_revenue"),
    )
    .withColumn("repeated_within_60",  (col("days_to_first_repeat") <= 60) .cast("integer"))
    .withColumn("repeated_within_90",  (col("days_to_first_repeat") <= 90) .cast("integer"))
    .withColumn("repeated_within_180", (col("days_to_first_repeat") <= 180).cast("integer"))
    .withColumn("repeated_within_365", (col("days_to_first_repeat") <= 365).cast("integer"))
)

# Cohort aggregation
cohort_summary = (
    first_orders
    .join(
        repeat_flags.select(
            "user_id","repeated_within_60","repeated_within_90",
            "repeated_within_180","repeated_within_365",
            "repeat_order_count","repeat_revenue","days_to_first_repeat"
        ),
        on="user_id", how="left"
    )
    .withColumn("repeated_within_60",  coalesce(col("repeated_within_60"),  lit(0)))
    .withColumn("repeated_within_90",  coalesce(col("repeated_within_90"),  lit(0)))
    .withColumn("repeated_within_180", coalesce(col("repeated_within_180"), lit(0)))
    .withColumn("repeated_within_365", coalesce(col("repeated_within_365"), lit(0)))
    .groupBy("acquisition_cohort", "acquisition_channel")
    .agg(
        count("user_id")                                   .alias("new_customers"),
        spark_round(spark_sum("first_order_value"),   2)   .alias("cohort_first_revenue"),
        spark_round(avg("first_order_value"),         2)   .alias("avg_first_order_value"),
        spark_sum("repeated_within_60")                    .alias("repeated_60d"),
        spark_sum("repeated_within_90")                    .alias("repeated_90d"),
        spark_sum("repeated_within_180")                   .alias("repeated_180d"),
        spark_sum("repeated_within_365")                   .alias("repeated_365d"),
        spark_round(avg("days_to_first_repeat"), 1)        .alias("avg_days_to_repeat"),
    )
    .withColumn("retention_rate_60d",
        spark_round(col("repeated_60d")  / col("new_customers") * 100, 1))
    .withColumn("retention_rate_90d",
        spark_round(col("repeated_90d")  / col("new_customers") * 100, 1))
    .withColumn("retention_rate_180d",
        spark_round(col("repeated_180d") / col("new_customers") * 100, 1))
    .withColumn("retention_rate_365d",
        spark_round(col("repeated_365d") / col("new_customers") * 100, 1))
    .orderBy("acquisition_cohort")
    .withColumn("_gold_ts", current_timestamp())
)

save_gold(cohort_summary, "acquisition_cohorts")
display(cohort_summary.limit(12))

# COMMAND ----------

# ════════════════════════════════════════════════════════════════════════════
# GOLD TABLE ③ — Repeat vs New Session Behaviour
#   Do repeat visitors convert more? Do they spend more?
# ════════════════════════════════════════════════════════════════════════════

repeat_vs_new = (
    sessions
    .join(
        orders.select("website_session_id", "gross_revenue", "net_margin"),
        on="website_session_id", how="left"
    )
    .withColumn("placed_order",   col("gross_revenue").isNotNull().cast("integer"))
    .withColumn("gross_revenue",  coalesce(col("gross_revenue"), lit(0.0)))
    .groupBy("is_repeat_session", "channel_group", "device_type")
    .agg(
        count("website_session_id")         .alias("sessions"),
        spark_sum("placed_order")           .alias("orders"),
        spark_round(
            spark_sum("placed_order") / count("website_session_id") * 100, 2)
                                            .alias("cvr_pct"),
        spark_round(avg("gross_revenue"), 4).alias("avg_revenue_per_session"),
        spark_round(spark_sum("gross_revenue"), 2).alias("total_revenue"),
    )
    .withColumn("_gold_ts", current_timestamp())
    .orderBy("is_repeat_session", "channel_group")
)

save_gold(repeat_vs_new, "repeat_session_analysis")
display(repeat_vs_new)

# COMMAND ----------

# ════════════════════════════════════════════════════════════════════════════
# GOLD TABLE ④ — LTV by Acquisition Channel
#   Which channel acquires customers with the highest 12-month LTV?
# ════════════════════════════════════════════════════════════════════════════

ltv_by_channel = (
    user_orders
    .filter(col("acquisition_channel").isNotNull())
    .groupBy("acquisition_channel")
    .agg(
        count("user_id")                                   .alias("customers_acquired"),
        spark_sum(when(col("is_repeat_buyer"), 1).otherwise(0)).alias("repeat_buyers"),
        spark_round(avg("gross_lifetime_value"),    2)     .alias("avg_gross_ltv"),
        spark_round(avg("net_lifetime_value"),      2)     .alias("avg_net_ltv"),
        spark_round(avg("lifetime_margin"),         2)     .alias("avg_lifetime_margin"),
        spark_round(avg("total_orders"),            2)     .alias("avg_orders_per_customer"),
        spark_round(avg("avg_order_value"),         2)     .alias("avg_order_value"),
        spark_round(avg("refund_rate_pct"),         1)     .alias("avg_customer_refund_rate"),
        spark_round(spark_sum("gross_lifetime_value"), 2)  .alias("total_channel_ltv"),
    )
    .withColumn("repeat_buyer_rate_pct",
        spark_round(col("repeat_buyers") / col("customers_acquired") * 100, 1))
    .withColumn("_gold_ts", current_timestamp())
    .orderBy(col("avg_gross_ltv").desc())
)

save_gold(ltv_by_channel, "ltv_by_acquisition_channel")
display(ltv_by_channel)

# COMMAND ----------

# DBTITLE 1,Cell 7
# ════════════════════════════════════════════════════════════════════════════
# GOLD TABLE ⑤ — Executive KPI Dashboard (single-row summary)
# ════════════════════════════════════════════════════════════════════════════

from pyspark.sql.functions import stddev, percentile_approx

exec_kpis = orders.agg(
    count("order_id")                                    .alias("total_orders"),
    countDistinct("user_id")                             .alias("total_customers"),
    spark_round(spark_sum("gross_revenue"),         2)   .alias("total_gross_revenue"),
    spark_round(spark_sum("net_revenue"),           2)   .alias("total_net_revenue"),
    spark_round(spark_sum("net_margin"),            2)   .alias("total_net_margin"),
    spark_round(avg("gross_margin_pct"),            1)   .alias("avg_gross_margin_pct"),
    spark_round(avg("net_margin_pct"),              1)   .alias("avg_net_margin_pct"),
    spark_round(avg("gross_revenue"),               2)   .alias("avg_order_value"),
    spark_sum(when(col("has_refund"),1).otherwise(0))    .alias("total_refunded_orders"),
    spark_round(spark_sum("total_refund_usd"),      2)   .alias("total_refund_value"),
    spark_round(
        spark_sum(when(col("has_refund"),1).otherwise(0)) / count("order_id") * 100, 2)
                                                         .alias("overall_refund_rate_pct"),
    spark_sum(when(col("is_cross_sell_order"),1).otherwise(0)).alias("cross_sell_orders"),
    spark_round(
        spark_sum(when(col("is_cross_sell_order"),1).otherwise(0)) / count("order_id") * 100, 2)
                                                         .alias("cross_sell_rate_pct"),
    spark_round(spark_sum("items_purchased") / count("order_id"), 2)
                                                         .alias("avg_items_per_order"),
    spark_min("order_ts")                                .alias("first_order_date"),
    spark_max("order_ts")                                .alias("last_order_date"),
).withColumn("_gold_ts", current_timestamp())

save_gold(exec_kpis, "executive_kpis")

print("\n── EXECUTIVE KPIs ───────────────────────")
display(exec_kpis)

# Session-level summary
session_kpis = sessions.agg(
    count("website_session_id")                          .alias("total_sessions"),
    countDistinct("user_id")                             .alias("unique_visitors"),
    spark_sum(col("is_repeat_session").cast("integer"))  .alias("repeat_sessions"),
    spark_round(
        spark_sum(col("is_repeat_session").cast("integer")) /
        count("website_session_id") * 100, 1)            .alias("repeat_session_pct"),
    countDistinct("marketing_channel")                   .alias("distinct_channels"),
).withColumn("_gold_ts", current_timestamp())

print("\n── SESSION KPIs ─────────────────────────")
display(session_kpis)

# COMMAND ----------

print("""
✅  Notebook 06 — Gold Customer Cohorts COMPLETE

    mff_gold.user_order_summary          → Per-user LTV, tier, refund rate
    mff_gold.acquisition_cohorts         → Monthly cohorts: 60/90/180/365d retention
    mff_gold.repeat_session_analysis     → New vs repeat session CVR comparison
    mff_gold.ltv_by_acquisition_channel  → Channel-level LTV & repeat buyer rate
    mff_gold.executive_kpis              → Single-row company P&L summary

    Next → Run 07_ml_conversion_model.py
""")