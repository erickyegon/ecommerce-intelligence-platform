# Databricks notebook source
# ══════════════════════════════════════════════════════════════════════════════
# Maven Fuzzy Factory | E-Commerce Intelligence Platform
# Notebook 02 — SILVER LAYER: Session & Attribution Enrichment
# ══════════════════════════════════════════════════════════════════════════════
#
#  Reads  : mff_bronze.website_sessions
#           mff_bronze.website_pageviews
#
#  Writes : mff_silver.sessions_enriched
#           mff_silver.session_funnel_steps
#
#  What happens here:
#    ① Type-cast timestamps, clean nulls
#    ② Classify every session into a marketing channel (8 categories)
#    ③ Build session-level funnel flags (which pages did the session hit?)
#    ④ First-touch attribution per user (acquisition channel)
#    ⑤ Identify A/B test cohorts (lander variants, billing variants)
#    ⑥ Data Quality flagging
# ══════════════════════════════════════════════════════════════════════════════

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, trim, lower, upper,
    when, lit, coalesce,
    year, month, dayofweek, hour, quarter,
    min as spark_min, max as spark_max,
    count, countDistinct,
    row_number, dense_rank,
    current_timestamp, regexp_replace,
    collect_list, array_contains,
    sum as spark_sum, round as spark_round
)
from pyspark.sql.window import Window
import logging

spark = SparkSession.builder.appName("MFF_Silver_Sessions").getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")
logger = logging.getLogger("MFF.Silver.Sessions")

BRONZE_DB = "mff_bronze"
SILVER_DB = "mff_silver"
SILVER_PATH = "/delta/maven_fuzzy_factory/silver"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {SILVER_DB}")

# COMMAND ----------

# ════════════════════════════════════════════════════════════════════════════
# STEP 1 — Load Bronze sessions & cast types
# ════════════════════════════════════════════════════════════════════════════

df_raw = spark.table(f"{BRONZE_DB}.website_sessions")

df_typed = (
    df_raw
    .withColumn("session_ts",     to_timestamp("created_at"))
    .withColumn("utm_source",     lower(trim(coalesce(col("utm_source"),     lit("direct")))))
    .withColumn("utm_campaign",   lower(trim(coalesce(col("utm_campaign"),   lit("none")))))
    .withColumn("utm_content",    lower(trim(coalesce(col("utm_content"),    lit("none")))))
    .withColumn("device_type",    lower(trim(col("device_type"))))
    .withColumn("http_referer",   lower(trim(coalesce(col("http_referer"),   lit("none")))))
    .withColumn("is_repeat_session", col("is_repeat_session").cast("boolean"))
)

# COMMAND ----------

# ════════════════════════════════════════════════════════════════════════════
# STEP 2 — Marketing Channel Classification (8-bucket model)
#
#  Channel taxonomy mirrors real-world paid search / analytics logic:
#   gsearch nonbrand   → Paid Search – Non-Brand (highest volume, lowest intent)
#   gsearch brand      → Paid Search – Brand     (mid intent, defending brand)
#   bsearch nonbrand   → Bing Non-Brand
#   bsearch brand      → Bing Brand
#   socialbook desktop → Social – Desktop
#   socialbook pilot   → Social – Pilot (new channel test)
#   organic            → Organic Search (referer present, no UTM)
#   direct             → Direct / Type-in
# ════════════════════════════════════════════════════════════════════════════

df_channels = df_typed.withColumn("marketing_channel",
    when((col("utm_source") == "gsearch") & (col("utm_campaign") == "nonbrand"),
         lit("paid_search_gsearch_nonbrand"))
    .when((col("utm_source") == "gsearch") & (col("utm_campaign") == "brand"),
          lit("paid_search_gsearch_brand"))
    .when((col("utm_source") == "bsearch") & (col("utm_campaign") == "nonbrand"),
          lit("paid_search_bsearch_nonbrand"))
    .when((col("utm_source") == "bsearch") & (col("utm_campaign") == "brand"),
          lit("paid_search_bsearch_brand"))
    .when((col("utm_source") == "socialbook") & (col("utm_campaign") == "desktop_targeted"),
          lit("social_desktop"))
    .when((col("utm_source") == "socialbook") & (col("utm_campaign") == "pilot"),
          lit("social_pilot"))
    .when(col("utm_source") == "direct",
          lit("direct"))
    .when((col("http_referer").contains("gsearch") | col("http_referer").contains("bsearch"))
           & col("utm_source").isin("direct","none"),
          lit("organic_search"))
    .otherwise(lit("other"))
).withColumn("channel_group",
    when(col("marketing_channel").startswith("paid_search"), lit("Paid Search"))
    .when(col("marketing_channel").startswith("social"),     lit("Social"))
    .when(col("marketing_channel") == "organic_search",      lit("Organic"))
    .when(col("marketing_channel") == "direct",              lit("Direct"))
    .otherwise(lit("Other"))
)

# COMMAND ----------

# ════════════════════════════════════════════════════════════════════════════
# STEP 3 — Temporal feature engineering
# ════════════════════════════════════════════════════════════════════════════

df_temporal = (
    df_channels
    .withColumn("session_year",    year("session_ts"))
    .withColumn("session_quarter", quarter("session_ts"))
    .withColumn("session_month",   month("session_ts"))
    .withColumn("session_hour",    hour("session_ts"))
    .withColumn("day_of_week",     dayofweek("session_ts"))   # 1=Sun, 7=Sat
    .withColumn("is_weekend",
        (dayofweek("session_ts").isin(1, 7)).cast("boolean"))
)

# COMMAND ----------

# ════════════════════════════════════════════════════════════════════════════
# STEP 4 — First-touch acquisition channel per user
#   Identifies which channel FIRST acquired each user, used for LTV attribution.
# ════════════════════════════════════════════════════════════════════════════

window_first = Window.partitionBy("user_id").orderBy("session_ts")

df_user_acquisition = (
    df_temporal
    .withColumn("user_session_rank", row_number().over(window_first))
    .withColumn("is_acquisition_session",
        (col("user_session_rank") == 1).cast("boolean"))
)

# First-touch channel per user (broadcast-join candidate — only 4 unique values at user level)
first_touch = (
    df_user_acquisition
    .filter(col("is_acquisition_session"))
    .select(
        col("user_id"),
        col("marketing_channel").alias("acquisition_channel"),
        col("session_ts").alias("first_session_ts")
    )
)

df_with_acq = df_user_acquisition.join(
    first_touch.hint("broadcast"), on="user_id", how="left"
)

# COMMAND ----------

# ════════════════════════════════════════════════════════════════════════════
# STEP 5 — Data Quality flagging
# ════════════════════════════════════════════════════════════════════════════

df_dq = df_with_acq.withColumn("dq_flag",
    when(col("website_session_id").isNull(), lit("MISSING_SESSION_ID"))
    .when(col("session_ts").isNull(),         lit("NULL_TIMESTAMP"))
    .when(col("user_id").isNull(),            lit("MISSING_USER_ID"))
    .otherwise(lit("PASS"))
)

# DQ summary
print("── Session DQ Summary ────────────────")
df_dq.groupBy("dq_flag").count().show()

# COMMAND ----------

# DBTITLE 1,Cell 8
# ════════════════════════════════════════════════════════════════════════════
# STEP 6 — Select final columns & write Silver sessions
# ════════════════════════════════════════════════════════════════════════════

sessions_silver_cols = [
    "website_session_id", "session_ts", "user_id",
    "is_repeat_session", "user_session_rank", "is_acquisition_session",
    "utm_source", "utm_campaign", "utm_content",
    "device_type", "http_referer",
    "marketing_channel", "channel_group",
    "acquisition_channel", "first_session_ts",
    "session_year", "session_quarter", "session_month",
    "session_hour", "day_of_week", "is_weekend",
    "dq_flag"
]

df_sessions_silver = (
    df_dq.select(sessions_silver_cols)
    .filter(col("dq_flag") == "PASS")
)

(df_sessions_silver.write.format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .partitionBy("session_year", "session_month")
    .saveAsTable(f"{SILVER_DB}.sessions_enriched"))

print(f"✔  {SILVER_DB}.sessions_enriched  ({df_sessions_silver.count():,} rows)")

# COMMAND ----------

# DBTITLE 1,Cell 9
# ════════════════════════════════════════════════════════════════════════════
# STEP 7 — Session Funnel Steps
#   For every session, pivot the pages it visited into boolean flags.
#   The conversion funnel:
#     /home or /lander-* → /products → /product-page → /cart
#     → /shipping → /billing or /billing-2 → /thank-you
# ════════════════════════════════════════════════════════════════════════════

df_pv = spark.table(f"{BRONZE_DB}.website_pageviews")

# For each session, collect all URLs visited and create boolean page flags
from pyspark.sql.functions import max as spark_max

df_funnel = (
    df_pv
    .groupBy("website_session_id")
    .agg(
        # Entry pages
        spark_max(when(col("pageview_url") == "/home",         1).otherwise(0))
            .cast("boolean").alias("saw_home"),
        spark_max(when(col("pageview_url").like("/lander%"),   1).otherwise(0))
            .cast("boolean").alias("saw_lander"),
        spark_max(when(col("pageview_url") == "/lander-1",     1).otherwise(0))
            .cast("boolean").alias("saw_lander_1"),
        spark_max(when(col("pageview_url") == "/lander-2",     1).otherwise(0))
            .cast("boolean").alias("saw_lander_2"),
        spark_max(when(col("pageview_url") == "/lander-3",     1).otherwise(0))
            .cast("boolean").alias("saw_lander_3"),
        spark_max(when(col("pageview_url") == "/lander-4",     1).otherwise(0))
            .cast("boolean").alias("saw_lander_4"),
        spark_max(when(col("pageview_url") == "/lander-5",     1).otherwise(0))
            .cast("boolean").alias("saw_lander_5"),
        # Funnel steps
        spark_max(when(col("pageview_url") == "/products",     1).otherwise(0))
            .cast("boolean").alias("saw_products"),
        spark_max(when(col("pageview_url").like("/the-%"),     1).otherwise(0))
            .cast("boolean").alias("saw_product_page"),
        spark_max(when(col("pageview_url") == "/the-original-mr-fuzzy",   1).otherwise(0))
            .cast("boolean").alias("saw_mr_fuzzy"),
        spark_max(when(col("pageview_url") == "/the-forever-love-bear",   1).otherwise(0))
            .cast("boolean").alias("saw_love_bear"),
        spark_max(when(col("pageview_url") == "/the-birthday-sugar-panda",1).otherwise(0))
            .cast("boolean").alias("saw_sugar_panda"),
        spark_max(when(col("pageview_url") == "/the-hudson-river-mini-bear",1).otherwise(0))
            .cast("boolean").alias("saw_mini_bear"),
        spark_max(when(col("pageview_url") == "/cart",         1).otherwise(0))
            .cast("boolean").alias("saw_cart"),
        spark_max(when(col("pageview_url") == "/shipping",     1).otherwise(0))
            .cast("boolean").alias("saw_shipping"),
        spark_max(when(col("pageview_url").like("/billing%"),  1).otherwise(0))
            .cast("boolean").alias("saw_billing_any"),
        spark_max(when(col("pageview_url") == "/billing",      1).otherwise(0))
            .cast("boolean").alias("saw_billing_v1"),
        spark_max(when(col("pageview_url") == "/billing-2",    1).otherwise(0))
            .cast("boolean").alias("saw_billing_v2"),
        spark_max(when(col("pageview_url") == "/thank-you-for-your-order", 1).otherwise(0))
            .cast("boolean").alias("converted"),
        # Funnel depth (max step reached: 1=entry, 2=products, 3=product, 4=cart, 5=ship, 6=bill, 7=order)
        spark_max(
            when(col("pageview_url") == "/thank-you-for-your-order", 7)
            .when(col("pageview_url").like("/billing%"),               6)
            .when(col("pageview_url") == "/shipping",                  5)
            .when(col("pageview_url") == "/cart",                      4)
            .when(col("pageview_url").like("/the-%"),                   3)
            .when(col("pageview_url") == "/products",                  2)
            .otherwise(1)
        ).alias("funnel_depth"),
        count("*").alias("total_pageviews_in_session"),
    )
)

# Add funnel step label
df_funnel_labeled = df_funnel.withColumn("funnel_exit_step",
    when(col("funnel_depth") == 7, lit("Converted"))
    .when(col("funnel_depth") == 6, lit("Billing"))
    .when(col("funnel_depth") == 5, lit("Shipping"))
    .when(col("funnel_depth") == 4, lit("Cart"))
    .when(col("funnel_depth") == 3, lit("Product Page"))
    .when(col("funnel_depth") == 2, lit("Products"))
    .otherwise(lit("Entry Page"))
)

(df_funnel_labeled.write.format("delta")
    .mode("overwrite")
    .saveAsTable(f"{SILVER_DB}.session_funnel_steps"))

print(f"✔  {SILVER_DB}.session_funnel_steps  ({df_funnel_labeled.count():,} rows)")
display(df_funnel_labeled.limit(5))

# COMMAND ----------

print("""
✅  Notebook 02 — Silver Sessions COMPLETE

    mff_silver.sessions_enriched      → 8-way channel classification,
                                         first-touch attribution, temporal features
    mff_silver.session_funnel_steps   → Boolean flags for all 17 funnel pages,
                                         funnel depth & exit step per session
    Next → Run 03_silver_orders_revenue.py
""")