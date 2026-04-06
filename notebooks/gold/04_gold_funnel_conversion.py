# Databricks notebook source
# ══════════════════════════════════════════════════════════════════════════════
# Maven Fuzzy Factory | E-Commerce Intelligence Platform
# Notebook 04 — GOLD LAYER: Conversion Funnel & Marketing ROI
# ══════════════════════════════════════════════════════════════════════════════
#
#  Gold tables produced:
#    ① channel_conversion_rates    — CVR, revenue per session by channel & device
#    ② monthly_channel_trends      — MoM sessions, orders, CVR, revenue by channel
#    ③ funnel_step_analysis        — Drop-off at every funnel step (overall + by channel)
#    ④ ab_lander_test_results      — Statistical A/B comparison of landing pages
#    ⑤ billing_ab_test_results     — /billing vs /billing-2 conversion lift
#    ⑥ channel_revenue_roi         — Revenue attributed per channel (last-touch + first-touch)
# ══════════════════════════════════════════════════════════════════════════════

# COMMAND ----------

# DBTITLE 1,Cell 2
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, count, countDistinct,
    sum as spark_sum, avg, round as spark_round,
    when, year, month, quarter,
    min as spark_min, max as spark_max,
    concat_ws, date_format, to_date,
    current_timestamp, coalesce
)
from pyspark.sql.window import Window
from pyspark.sql.functions import lag, row_number
import math
import logging

spark = SparkSession.builder.appName("MFF_Gold_Funnel").getOrCreate()
logger = logging.getLogger("MFF.Gold.Funnel")

SILVER_DB = "mff_silver"
GOLD_DB   = "mff_gold"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {GOLD_DB}")

# Load silver tables
sessions  = spark.table(f"{SILVER_DB}.sessions_enriched")
funnel    = spark.table(f"{SILVER_DB}.session_funnel_steps")
orders    = spark.table(f"{SILVER_DB}.orders_enriched")

# Helper to write gold tables
def save_gold(df, name):
    (df.write.format("delta").mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable(f"{GOLD_DB}.{name}"))
    print(f"  ✔  {GOLD_DB}.{name}  ({df.count():,} rows)")
    return df

# COMMAND ----------

from pyspark.sql.functions import coalesce, col, lit, count, sum as spark_sum, avg, round as spark_round, current_timestamp# ════════════════════════════════════════════════════════════════════════════

# GOLD TABLE ① — Channel Conversion Rates
#   Join sessions → funnel steps → orders to compute CVR per channel × device
# ════════════════════════════════════════════════════════════════════════════

# Order flag per session (1 if converted, else 0)
sessions_with_order = (
    sessions
    .join(funnel, on="website_session_id", how="left")
    .join(
        orders.select(
            "website_session_id",
            col("gross_revenue").alias("order_revenue"),
            col("net_margin").alias("order_margin"),
            col("has_refund")
        ),
        on="website_session_id", how="left"
    )
    .withColumn("placed_order",   col("order_revenue").isNotNull().cast("integer"))
    .withColumn("order_revenue",  coalesce(col("order_revenue"), lit(0.0)))
    .withColumn("order_margin",   coalesce(col("order_margin"),  lit(0.0)))
)

# Macro CVR by channel × device
channel_cvr = (
    sessions_with_order
    .groupBy("marketing_channel", "channel_group", "device_type")
    .agg(
        count("website_session_id")                        .alias("total_sessions"),
        spark_sum("placed_order")                          .alias("total_orders"),
        spark_sum("order_revenue")                         .alias("total_gross_revenue"),
        spark_sum("order_margin")                          .alias("total_net_margin"),
        spark_round(avg("total_pageviews_in_session"), 2)  .alias("avg_pageviews_per_session"),
    )
    .withColumn("conversion_rate_pct",
        spark_round(col("total_orders") / col("total_sessions") * 100, 2))
    .withColumn("revenue_per_session",
        spark_round(col("total_gross_revenue") / col("total_sessions"), 4))
    .withColumn("margin_per_session",
        spark_round(col("total_net_margin") / col("total_sessions"), 4))
    .orderBy(col("total_sessions").desc())
    .withColumn("_gold_ts", current_timestamp())
)

save_gold(channel_cvr, "channel_conversion_rates")
display(channel_cvr)

# COMMAND ----------

# DBTITLE 1,Cell 4
# ════════════════════════════════════════════════════════════════════════════
# GOLD TABLE ② — Monthly Channel Trends (time-series)
#   Shows growth/decline of each channel over 3 years
# ════════════════════════════════════════════════════════════════════════════

monthly_trends = (
    sessions_with_order
    .withColumn("year_month", date_format("session_ts", "yyyy-MM"))
    .groupBy("year_month", "channel_group", "marketing_channel", "device_type")
    .agg(
        count("website_session_id")    .alias("sessions"),
        spark_sum("placed_order")      .alias("orders"),
        spark_sum("order_revenue")     .alias("gross_revenue"),
        spark_sum("order_margin")      .alias("net_margin"),
    )
    .withColumn("cvr_pct",
        spark_round(col("orders") / col("sessions") * 100, 2))
    .withColumn("revenue_per_session",
        spark_round(col("gross_revenue") / col("sessions"), 4))
    .orderBy("year_month", "channel_group")
    .withColumn("_gold_ts", current_timestamp())
)

# Add MoM change in sessions per channel (window function)
window_mom = Window.partitionBy("marketing_channel", "device_type").orderBy("year_month")
monthly_trends_mom = (
    monthly_trends
    .withColumn("sessions_prev_month",  lag("sessions",  1).over(window_mom))
    .withColumn("orders_prev_month",    lag("orders",    1).over(window_mom))
    .withColumn("sessions_mom_pct_chg",
        when((col("sessions_prev_month").isNotNull()) & (col("sessions_prev_month") != 0),
            spark_round(
                (col("sessions") - col("sessions_prev_month")) / col("sessions_prev_month") * 100,
            1)
        ).otherwise(lit(None)))
    .withColumn("orders_mom_pct_chg",
        when((col("orders_prev_month").isNotNull()) & (col("orders_prev_month") != 0),
            spark_round(
                (col("orders") - col("orders_prev_month")) / col("orders_prev_month") * 100,
            1)
        ).otherwise(lit(None)))
)

save_gold(monthly_trends_mom, "monthly_channel_trends")

# COMMAND ----------

# ════════════════════════════════════════════════════════════════════════════
# GOLD TABLE ③ — Funnel Step Drop-off Analysis
#   For each funnel step: how many sessions reached it vs the prior step?
# ════════════════════════════════════════════════════════════════════════════

# Join sessions → funnel, then group by channel to see drop-off per channel
funnel_with_channel = (
    sessions.select("website_session_id","marketing_channel","channel_group","device_type")
    .join(funnel, on="website_session_id", how="inner")
)

def funnel_rates(df, group_cols):
    return (
        df.groupBy(*group_cols)
        .agg(
            count("website_session_id")               .alias("sessions_entered"),
            spark_sum(col("saw_products").cast("int")).alias("reached_products"),
            spark_sum(col("saw_product_page").cast("int")).alias("reached_product_page"),
            spark_sum(col("saw_cart").cast("int"))    .alias("reached_cart"),
            spark_sum(col("saw_shipping").cast("int")).alias("reached_shipping"),
            spark_sum(col("saw_billing_any").cast("int")).alias("reached_billing"),
            spark_sum(col("converted").cast("int"))   .alias("reached_thankyou"),
        )
        .withColumn("pct_to_products",    spark_round(col("reached_products")     / col("sessions_entered") * 100, 2))
        .withColumn("pct_to_product_page",spark_round(col("reached_product_page") / col("sessions_entered") * 100, 2))
        .withColumn("pct_to_cart",        spark_round(col("reached_cart")         / col("sessions_entered") * 100, 2))
        .withColumn("pct_to_shipping",    spark_round(col("reached_shipping")     / col("sessions_entered") * 100, 2))
        .withColumn("pct_to_billing",     spark_round(col("reached_billing")      / col("sessions_entered") * 100, 2))
        .withColumn("pct_to_order",       spark_round(col("reached_thankyou")     / col("sessions_entered") * 100, 2))
        # Step-over-step drop-off
        .withColumn("dropoff_entry_to_products",
            spark_round(100 - col("pct_to_products"), 2))
        .withColumn("dropoff_products_to_page",
            spark_round(col("pct_to_products") - col("pct_to_product_page"), 2))
        .withColumn("dropoff_page_to_cart",
            spark_round(col("pct_to_product_page") - col("pct_to_cart"), 2))
        .withColumn("dropoff_cart_to_shipping",
            spark_round(col("pct_to_cart") - col("pct_to_shipping"), 2))
        .withColumn("dropoff_shipping_to_billing",
            spark_round(col("pct_to_shipping") - col("pct_to_billing"), 2))
        .withColumn("dropoff_billing_to_order",
            spark_round(col("pct_to_billing") - col("pct_to_order"), 2))
        .withColumn("_gold_ts", current_timestamp())
    )

# Overall funnel
funnel_overall = funnel_rates(funnel_with_channel, ["channel_group","device_type"])
save_gold(funnel_overall, "funnel_step_analysis")
display(funnel_overall.orderBy("channel_group","device_type"))

# COMMAND ----------

# ════════════════════════════════════════════════════════════════════════════
# GOLD TABLE ④ — Landing Page A/B Test Results
#   Compare /home vs /lander-1 vs /lander-2 ... on CVR & revenue
# ════════════════════════════════════════════════════════════════════════════

# For each session identify which entry lander it saw (first URL viewed)
from pyspark.sql.functions import first

# Get entry page per session (lowest pageview_id = first page seen)
pv = spark.table("mff_bronze.website_pageviews")
window_entry = Window.partitionBy("website_session_id").orderBy("website_pageview_id")

entry_pages = (
    pv
    .withColumn("rn", row_number().over(window_entry))
    .filter(col("rn") == 1)
    .select("website_session_id", col("pageview_url").alias("entry_page"))
)

# Join to sessions + order outcome
ab_lander_data = (
    sessions.select("website_session_id", "marketing_channel", "session_ts")
    .join(entry_pages, on="website_session_id", how="inner")
    .join(funnel.select("website_session_id", "converted"), on="website_session_id", how="left")
    .join(
        orders.select("website_session_id", "gross_revenue", "net_margin"),
        on="website_session_id", how="left"
    )
    .withColumn("converted",      coalesce(col("converted"), lit(False)))
    .withColumn("gross_revenue",  coalesce(col("gross_revenue"), lit(0.0)))
    .withColumn("placed_order",   col("converted").cast("integer"))
)

lander_results = (
    ab_lander_data
    .groupBy("entry_page")
    .agg(
        count("website_session_id")       .alias("sessions"),
        spark_sum("placed_order")         .alias("orders"),
        spark_round(
            spark_sum("placed_order") / count("website_session_id") * 100, 2)
                                          .alias("cvr_pct"),
        spark_round(avg("gross_revenue"), 4) .alias("avg_revenue_per_session"),
        spark_round(spark_sum("gross_revenue"), 2) .alias("total_revenue"),
    )
    .orderBy(col("sessions").desc())
    .withColumn("_gold_ts", current_timestamp())
)

save_gold(lander_results, "ab_lander_test_results")
display(lander_results)

# COMMAND ----------

# ════════════════════════════════════════════════════════════════════════════
# GOLD TABLE ⑤ — Billing Page A/B Test (/billing vs /billing-2)
#   Among sessions that REACHED billing, how many converted? CVR lift?
# ════════════════════════════════════════════════════════════════════════════

billing_ab = (
    funnel_with_channel
    .filter(col("saw_billing_any") == True)   # Only sessions that got to billing
    .withColumn("billing_variant",
        when(col("saw_billing_v2"), lit("/billing-2"))
        .otherwise(lit("/billing")))
    .join(
        orders.select("website_session_id", "gross_revenue", "net_margin"),
        on="website_session_id", how="left"
    )
    .withColumn("placed_order",  (col("gross_revenue").isNotNull()).cast("integer"))
    .withColumn("gross_revenue", coalesce(col("gross_revenue"), lit(0.0)))
    .groupBy("billing_variant", "channel_group", "device_type")
    .agg(
        count("website_session_id")    .alias("sessions_at_billing"),
        spark_sum("placed_order")      .alias("orders"),
        spark_round(
            spark_sum("placed_order") / count("website_session_id") * 100, 2)
                                       .alias("billing_to_order_cvr_pct"),
        spark_round(avg("gross_revenue"), 4).alias("avg_revenue_per_session"),
    )
    .orderBy("billing_variant", "channel_group")
    .withColumn("_gold_ts", current_timestamp())
)

save_gold(billing_ab, "billing_ab_test_results")
display(billing_ab)

# COMMAND ----------

# ════════════════════════════════════════════════════════════════════════════
# GOLD TABLE ⑥ — Channel Revenue & ROI Summary
#   Last-touch attribution: revenue credited to the session's channel
#   First-touch attribution: revenue credited to the user's acquisition channel
# ════════════════════════════════════════════════════════════════════════════

# Last-touch
last_touch = (
    orders
    .groupBy("session_channel", "session_channel_group", "session_device_type")
    .agg(
        count("order_id")                          .alias("orders"),
        spark_round(spark_sum("gross_revenue"), 2) .alias("gross_revenue_last_touch"),
        spark_round(spark_sum("net_revenue"), 2)   .alias("net_revenue_last_touch"),
        spark_round(spark_sum("net_margin"), 2)    .alias("net_margin_last_touch"),
        spark_round(avg("gross_margin_pct"), 1)    .alias("avg_gross_margin_pct"),
        spark_round(avg("net_margin_pct"), 1)      .alias("avg_net_margin_pct"),
        spark_sum(when(col("has_refund"), 1).otherwise(0)).alias("refund_count"),
        spark_round(spark_sum("total_refund_usd"), 2).alias("total_refunds"),
    )
    .withColumn("refund_rate_pct",
        spark_round(col("refund_count") / col("orders") * 100, 2))
    .withColumn("avg_order_value",
        spark_round(col("gross_revenue_last_touch") / col("orders"), 2))
    .withColumn("_gold_ts", current_timestamp())
    .withColumnRenamed("session_channel",       "marketing_channel")
    .withColumnRenamed("session_channel_group", "channel_group")
    .withColumnRenamed("session_device_type",   "device_type")
)

save_gold(last_touch, "channel_revenue_roi")
display(last_touch.orderBy(col("gross_revenue_last_touch").desc()))

# COMMAND ----------

print("""
✅  Notebook 04 — Gold Funnel & Marketing COMPLETE

    mff_gold.channel_conversion_rates   → CVR, revenue/session by channel × device
    mff_gold.monthly_channel_trends     → 3-year time series + MoM change
    mff_gold.funnel_step_analysis       → Step-by-step drop-off + %age rates
    mff_gold.ab_lander_test_results     → All lander variants compared
    mff_gold.billing_ab_test_results    → /billing vs /billing-2 lift
    mff_gold.channel_revenue_roi        → Last-touch revenue attribution

    Next → Run 05_gold_product_revenue.py
""")