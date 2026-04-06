# Databricks notebook source
# ══════════════════════════════════════════════════════════════════════════════
# Maven Fuzzy Factory | E-Commerce Intelligence Platform
# Notebook 09 — DELTA LAKE ADVANCED: Production Engineering Patterns
# ══════════════════════════════════════════════════════════════════════════════
#
#  Demonstrates the enterprise Delta Lake capabilities that justify
#  Databricks over a plain Spark + Parquet stack:
#
#   ① ACID audit trail & time travel
#   ② Incremental MERGE (simulated daily session batch)
#   ③ Structured Streaming simulation (real-time session ingestion)
#   ④ OPTIMIZE + ZORDER for query acceleration
#   ⑤ VACUUM (storage reclamation)
#   ⑥ Delta Change Data Feed (CDC) — track row-level changes
#   ⑦ Schema evolution without downtime
# ══════════════════════════════════════════════════════════════════════════════

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, to_timestamp,
    when, rand, expr
)
from delta.tables import DeltaTable
import logging

spark = SparkSession.builder \
    .appName("MFF_Delta_Advanced") \
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
    .getOrCreate()

logger = logging.getLogger("MFF.Delta")

SILVER_SESSIONS = "mff_silver.sessions_enriched"
SILVER_PATH     = "/delta/maven_fuzzy_factory/silver/sessions_enriched"

# COMMAND ----------

# ════════════════════════════════════════════════════════════════════════════
# FEATURE ① — DELTA HISTORY: Full ACID transaction audit trail
#   Every INSERT, UPDATE, DELETE, MERGE, OPTIMIZE is logged.
#   This is what gives you the ability to roll back or audit "what changed?"
# ════════════════════════════════════════════════════════════════════════════

print("═" * 60)
print("FEATURE 1 — DELTA TRANSACTION HISTORY")
print("═" * 60)
display(spark.sql(f"DESCRIBE HISTORY {SILVER_SESSIONS}"))

# COMMAND ----------

# ════════════════════════════════════════════════════════════════════════════
# FEATURE ② — TIME TRAVEL
#   Read a previous version of the table by version number or timestamp.
#   Use case: "Show me the data before the pipeline ran this morning."
# ════════════════════════════════════════════════════════════════════════════

print("FEATURE 2 — TIME TRAVEL")

# Read version 0 (initial load state)
df_v0 = spark.read.format("delta").option("versionAsOf", 0).load(SILVER_PATH)
print(f"  Version 0 row count : {df_v0.count():,}")

# Compare versions
current_count = spark.table(SILVER_SESSIONS).count()
print(f"  Current version count : {current_count:,}")
print(f"  Delta (new rows since v0) : {current_count - df_v0.count():,}")

# Timestamp-based (comment out and set your own ts):
# df_ts = spark.read.format("delta") \
#     .option("timestampAsOf", "2024-06-01T00:00:00") \
#     .load(SILVER_PATH)

display(df_v0.limit(3))

# COMMAND ----------

# ════════════════════════════════════════════════════════════════════════════
# FEATURE ③ — INCREMENTAL MERGE (Daily batch upsert pattern)
#   Simulates a new batch of sessions arriving overnight.
#   MERGE handles: existing sessions that need updating + brand new sessions.
#   This replaces the brittle "overwrite entire table" approach.
# ════════════════════════════════════════════════════════════════════════════

print("FEATURE 3 — INCREMENTAL MERGE (Daily upsert)")

from pyspark.sql.types import (
    StructType, StructField, LongType, StringType,
    BooleanType, TimestampType, IntegerType
)

# Simulate incoming daily batch from source system
# - Session 1 → existing session that got utm_source corrected
# - Session 2 → brand new session from today
new_batch = spark.createDataFrame([
    (1, "2012-03-19 08:04:16", 1, False, "gsearch", "nonbrand", "g_ad_1",
     "desktop", "https://www.gsearch.com",
     "paid_search_gsearch_nonbrand", "Paid Search",
     "paid_search_gsearch_nonbrand", "2012-03-19 08:04:16",
     2012, 1, 1, 8, 2, False, False, "PASS"),
    (999999999, "2025-01-15 14:23:11", 999999, False, "gsearch", "brand", "g_ad_b",
     "desktop", "https://www.gsearch.com",
     "paid_search_gsearch_brand", "Paid Search",
     "paid_search_gsearch_brand", "2025-01-15 14:23:11",
     2025, 1, 1, 14, 3, False, False, "PASS"),
], schema=StructType([
    StructField("website_session_id",       LongType(),    False),
    StructField("session_ts",               StringType(),  True),
    StructField("user_id",                  LongType(),    True),
    StructField("is_repeat_session",        BooleanType(), True),
    StructField("utm_source",               StringType(),  True),
    StructField("utm_campaign",             StringType(),  True),
    StructField("utm_content",              StringType(),  True),
    StructField("device_type",              StringType(),  True),
    StructField("http_referer",             StringType(),  True),
    StructField("marketing_channel",        StringType(),  True),
    StructField("channel_group",            StringType(),  True),
    StructField("acquisition_channel",      StringType(),  True),
    StructField("first_session_ts",         StringType(),  True),
    StructField("session_year",             IntegerType(), True),
    StructField("session_quarter",          IntegerType(), True),
    StructField("session_month",            IntegerType(), True),
    StructField("session_hour",             IntegerType(), True),
    StructField("day_of_week",              IntegerType(), True),
    StructField("is_weekend",               BooleanType(), True),
    StructField("is_acquisition_session",   BooleanType(), True),
    StructField("dq_flag",                  StringType(),  True),
]))

delta_sessions = DeltaTable.forName(spark, SILVER_SESSIONS)

(
    delta_sessions.alias("target")
    .merge(
        new_batch.alias("source"),
        "target.website_session_id = source.website_session_id"
    )
    .whenMatchedUpdate(set={
        "utm_source":      "source.utm_source",
        "utm_campaign":    "source.utm_campaign",
        "device_type":     "source.device_type",
        "marketing_channel": "source.marketing_channel",
    })
    .whenNotMatchedInsert(values={
        "website_session_id":    "source.website_session_id",
        "session_ts":            "to_timestamp(source.session_ts)",
        "user_id":               "source.user_id",
        "is_repeat_session":     "source.is_repeat_session",
        "utm_source":            "source.utm_source",
        "utm_campaign":          "source.utm_campaign",
        "device_type":           "source.device_type",
        "marketing_channel":     "source.marketing_channel",
        "channel_group":         "source.channel_group",
        "acquisition_channel":   "source.acquisition_channel",
        "session_year":          "source.session_year",
        "session_month":         "source.session_month",
        "dq_flag":               "source.dq_flag",
    })
    .execute()
)

print(f"  MERGE complete. New row count: {spark.table(SILVER_SESSIONS).count():,}")

# Verify the upsert
display(spark.sql(f"""
    SELECT website_session_id, utm_source, utm_campaign, device_type, marketing_channel
    FROM {SILVER_SESSIONS}
    WHERE website_session_id IN (1, 999999999)
"""))

# COMMAND ----------

# ════════════════════════════════════════════════════════════════════════════
# FEATURE ④ — OPTIMIZE + ZORDER
#   Compacts many small Parquet files into optimal-size files.
#   ZORDER co-locates rows with the same channel/device physically on disk
#   so filter queries read far fewer files (data skipping).
# ════════════════════════════════════════════════════════════════════════════

print("FEATURE 4 — OPTIMIZE + ZORDER BY (marketing_channel, device_type)")
spark.sql(f"""
    OPTIMIZE {SILVER_SESSIONS}
    ZORDER BY (marketing_channel, device_type, session_year)
""")
print("  OPTIMIZE complete — files compacted, ZORDER index built")

# Stats before/after are visible in DESCRIBE HISTORY
display(spark.sql(f"DESCRIBE HISTORY {SILVER_SESSIONS} LIMIT 3"))

# COMMAND ----------

# ════════════════════════════════════════════════════════════════════════════
# FEATURE ⑤ — ANALYZE TABLE (query planner statistics)
#   Computes column-level stats so the Spark optimizer can choose the best
#   join strategy (e.g. broadcast vs. sort-merge).
# ════════════════════════════════════════════════════════════════════════════

print("FEATURE 5 — ANALYZE TABLE COMPUTE STATISTICS")
spark.sql(f"ANALYZE TABLE {SILVER_SESSIONS} COMPUTE STATISTICS FOR ALL COLUMNS")
print("  Column statistics computed ✔")

# Inspect stats for a key column
display(spark.sql(f"DESCRIBE EXTENDED {SILVER_SESSIONS} marketing_channel"))

# COMMAND ----------

# ════════════════════════════════════════════════════════════════════════════
# FEATURE ⑥ — CHANGE DATA FEED (CDC)
#   Delta CDF tracks which rows were inserted, updated, or deleted
#   in every table version. Enables downstream incremental pipelines.
# ════════════════════════════════════════════════════════════════════════════

print("FEATURE 6 — CHANGE DATA FEED (CDC)")

# Enable CDF on the table
spark.sql(f"""
    ALTER TABLE {SILVER_SESSIONS}
    SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

# Simulate an update (data correction)
spark.sql(f"""
    UPDATE {SILVER_SESSIONS}
    SET device_type = 'desktop'
    WHERE website_session_id = 999999999
""")

# Read the change log: what rows changed and how?
try:
    cdf_df = (
        spark.read.format("delta")
        .option("readChangeData", True)
        .option("startingVersion", 1)
        .load(SILVER_PATH)
    )
    print("  CDC rows captured:")
    display(cdf_df.select(
        "website_session_id", "_change_type", "_commit_version", "_commit_timestamp"
    ).filter(col("website_session_id") == 999999999))
except Exception as e:
    print(f"  CDF note: {e} — enable CDF before the first write in production")

# COMMAND ----------

# ════════════════════════════════════════════════════════════════════════════
# FEATURE ⑦ — SCHEMA EVOLUTION
#   Add a new column to production table without downtime or full rewrite.
# ════════════════════════════════════════════════════════════════════════════

print("FEATURE 7 — SCHEMA EVOLUTION")

# Add 'propensity_label' column derived from ML predictions
df_with_propensity = (
    spark.table(SILVER_SESSIONS)
    .join(
        spark.table("mff_gold.session_conversion_predictions")
            .select("website_session_id", "propensity_segment"),
        on="website_session_id", how="left"
    )
)

(df_with_propensity.write.format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")    # ← adds propensity_segment without error
    .option("path", SILVER_PATH)
    .saveAsTable(SILVER_SESSIONS))

print("  New column 'propensity_segment' added via mergeSchema=True ✔")
display(spark.sql(f"""
    SELECT website_session_id, marketing_channel, propensity_segment
    FROM {SILVER_SESSIONS}
    WHERE propensity_segment IS NOT NULL
    LIMIT 5
"""))

# COMMAND ----------

# ════════════════════════════════════════════════════════════════════════════
# FEATURE ⑧ — VACUUM (storage reclamation)
#   Removes old Parquet files no longer referenced by any version.
#   Default retention: 7 days. In pilot mode we set RETAIN 0 HOURS.
# ════════════════════════════════════════════════════════════════════════════

print("FEATURE 8 — VACUUM")
# DRY RUN first — lists files that WOULD be deleted
spark.sql(f"VACUUM {SILVER_SESSIONS} RETAIN 168 HOURS DRY RUN")
print("  VACUUM DRY RUN complete — review files before running without DRY RUN")
# In production: spark.sql(f"VACUUM {SILVER_SESSIONS} RETAIN 168 HOURS")

# COMMAND ----------

print("""
✅  Notebook 09 — Delta Advanced Features COMPLETE

    ①  DESCRIBE HISTORY   → Full ACID audit trail
    ②  Time Travel        → Read any past version
    ③  MERGE              → Incremental upserts, no full overwrites
    ④  OPTIMIZE + ZORDER  → File compaction + co-location for fast filters
    ⑤  ANALYZE TABLE      → Column stats for optimal query plans
    ⑥  Change Data Feed   → Row-level CDC for downstream incremental pipelines
    ⑦  mergeSchema        → Zero-downtime schema evolution
    ⑧  VACUUM             → Storage cost management

    These capabilities directly answer the 3 roadmap requirements:
      Scalable → ZORDER + OPTIMIZE handle petabyte-scale efficiently
      Agile    → MERGE replaces brittle full-overwrite ETL
      Unified  → CDF powers streaming, batch & ML from the same table
""")