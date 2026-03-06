"""
ULK Final Project — Part 3: Analytics Integration
==================================================
Integrates data across MongoDB, HBase (simulated via Parquet), and Spark
to answer four complex business questions:

  Q1. Customer Lifetime Value (CLV) Estimation
      Combines user profiles + transaction history + session engagement.

  Q2. Product Affinity & Recommendation
      Uses co-purchase data + browsing behaviour to rank product suggestions.

  Q3. Funnel Conversion Analysis
      Tracks the full journey: page view → cart → purchase, per channel/device.

  Q4. Seasonal Trend Identification
      Analyses weekly/monthly revenue trends enriched with product view data.

Run AFTER part2a_data_cleaning.py and part2b_batch_analytics.py:
    spark-submit part3_analytics_integration.py

Outputs (Parquet + CSV):
    integration/clv_scores/
    integration/product_recommendations/
    integration/funnel_analysis/
    integration/seasonal_trends/
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType
import os

# ── Spark session ─────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("ULK_ECommerce_AnalyticsIntegration")
    .config("spark.sql.shuffle.partitions", "50")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

CLEAN_DIR       = "cleaned"
ANALYTICS_DIR   = "analytics"
INTEGRATION_DIR = "integration"


# ── Helpers ───────────────────────────────────────────────────────────────────

def load(name, base=CLEAN_DIR):
    return spark.read.parquet(os.path.join(base, name))


def save(df, name: str, show_n: int = 10):
    out = os.path.join(INTEGRATION_DIR, name)
    df.write.mode("overwrite").parquet(out)
    csv_out = out + "_csv"
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(csv_out)
    print(f"\n✔  [{name}]  {df.count():,} rows  →  {out}")
    df.show(show_n, truncate=False)


# ═══════════════════════════════════════════════════════════════════════════════
# Q1 — CUSTOMER LIFETIME VALUE (CLV) ESTIMATION
# ═══════════════════════════════════════════════════════════════════════════════
#
# Business Question:
#   Which users are our most valuable, and what drives their value?
#
# Data sources:
#   • users (MongoDB proxy)       → demographics, registration date
#   • transactions (MongoDB proxy)→ purchase history, revenue
#   • sessions (HBase proxy)      → engagement: session frequency, avg duration
#
# Formula:
#   CLV_score = (avg_order_value × purchase_frequency × engagement_score)
#   engagement_score = normalised(avg_session_duration × session_count)
# ═══════════════════════════════════════════════════════════════════════════════

def clv_estimation():
    print("\n" + "═" * 60)
    print("  Q1: Customer Lifetime Value (CLV) Estimation")
    print("═" * 60)

    # ── 1a. Transaction metrics per user (from MongoDB / transactions) ────────
    txn = load("transactions")

    txn_items = (
        txn
        .filter(F.col("status").isin("completed", "shipped"))
        .select(
            "user_id", "transaction_id", "total", "timestamp",
            F.explode("items").alias("item")
        )
        .select(
            "user_id", "transaction_id", "total", "timestamp",
            F.col("item.product_id").alias("product_id"),
            F.col("item.quantity").alias("quantity")
        )
    )

    txn_metrics = (
        txn_items
        .groupBy("user_id")
        .agg(
            F.count("transaction_id").alias("purchase_frequency"),
            F.round(F.sum("total"), 2).alias("total_revenue"),
            F.round(F.avg("total"), 2).alias("avg_order_value"),
            F.sum("quantity").alias("total_items_bought"),
            F.max("timestamp").alias("last_purchase_date"),
            F.min("timestamp").alias("first_purchase_date"),
            F.countDistinct("product_id").alias("unique_products_bought")
        )
    )

    # Recency: days since last purchase (lower = better)
    txn_metrics = txn_metrics.withColumn(
        "recency_days",
        F.datediff(F.current_date(), F.to_date("last_purchase_date"))
    )

    # ── 1b. Session engagement metrics (from HBase / sessions) ───────────────
    sessions = load("sessions")

    session_metrics = (
        sessions
        .groupBy("user_id")
        .agg(
            F.count("session_id").alias("session_count"),
            F.round(F.avg("duration_seconds"), 1).alias("avg_session_duration_s"),
            F.round(F.avg("page_view_count"), 2).alias("avg_page_views"),
            F.round(F.avg("viewed_product_count"), 2).alias("avg_products_viewed"),
            F.sum(
                F.when(F.col("conversion_status") == "converted", 1).otherwise(0)
            ).alias("converted_sessions"),
            F.sum(
                F.when(F.col("conversion_status") == "abandoned", 1).otherwise(0)
            ).alias("abandoned_sessions")
        )
        .withColumn(
            "session_conversion_rate",
            F.round(
                F.when(F.col("session_count") > 0,
                       F.col("converted_sessions") / F.col("session_count"))
                 .otherwise(0.0), 4
            )
        )
    )

    # ── 1c. User demographics (from MongoDB / users) ──────────────────────────
    users = load("users").select(
        "user_id", "age", "gender", "state", "is_premium",
        "days_since_registration", "preferred_payment"
    )

    # ── 1d. Join all three sources ────────────────────────────────────────────
    clv = (
        txn_metrics
        .join(session_metrics, on="user_id", how="left")
        .join(users,           on="user_id", how="left")
        .fillna(0, subset=["session_count", "avg_session_duration_s",
                            "avg_page_views", "avg_products_viewed",
                            "session_conversion_rate"])
    )

    # ── 1e. Compute normalised engagement score ───────────────────────────────
    max_duration = clv.agg(F.max("avg_session_duration_s")).collect()[0][0] or 1
    max_sessions = clv.agg(F.max("session_count")).collect()[0][0] or 1

    clv = (
        clv
        .withColumn(
            "engagement_score",
            F.round(
                (F.col("avg_session_duration_s") / max_duration * 0.5)
                + (F.col("session_count")         / max_sessions * 0.3)
                + (F.col("session_conversion_rate")               * 0.2),
                4
            )
        )
        # CLV score: monetary × frequency × engagement
        .withColumn(
            "clv_score",
            F.round(
                F.col("avg_order_value")
                * F.col("purchase_frequency")
                * (1 + F.col("engagement_score")),
                2
            )
        )
        # Segment users into tiers
        .withColumn(
            "clv_tier",
            F.when(F.col("clv_score") >= F.expr("percentile_approx(clv_score, 0.90) OVER ()"), "Platinum")
             .when(F.col("clv_score") >= F.expr("percentile_approx(clv_score, 0.70) OVER ()"), "Gold")
             .when(F.col("clv_score") >= F.expr("percentile_approx(clv_score, 0.40) OVER ()"), "Silver")
             .otherwise("Bronze")
        )
        .orderBy(F.desc("clv_score"))
    )

    save(clv, "clv_scores", show_n=15)

    # ── 1f. CLV tier summary via Spark SQL ────────────────────────────────────
    clv.createOrReplaceTempView("clv")
    tier_summary = spark.sql("""
        SELECT
            clv_tier,
            COUNT(*)                          AS num_users,
            ROUND(AVG(clv_score), 2)          AS avg_clv_score,
            ROUND(AVG(total_revenue), 2)      AS avg_revenue,
            ROUND(AVG(purchase_frequency), 2) AS avg_purchases,
            ROUND(AVG(engagement_score), 4)   AS avg_engagement,
            ROUND(AVG(recency_days), 1)       AS avg_recency_days,
            SUM(CASE WHEN is_premium THEN 1 ELSE 0 END) AS premium_count
        FROM clv
        GROUP BY clv_tier
        ORDER BY avg_clv_score DESC
    """)
    print("\n  CLV Tier Summary:")
    tier_summary.show(truncate=False)
    tier_summary.coalesce(1).write.mode("overwrite") \
        .option("header", True).csv(os.path.join(INTEGRATION_DIR, "clv_tier_summary_csv"))

    return clv


# ═══════════════════════════════════════════════════════════════════════════════
# Q2 — PRODUCT AFFINITY & RECOMMENDATION
# ═══════════════════════════════════════════════════════════════════════════════
#
# Business Question:
#   For each product, what are the top-5 products to recommend next?
#
# Data sources:
#   • analytics/product_cobuys   (Spark batch — transaction co-purchases)
#   • analytics/product_coviews  (Spark batch — session co-views)
#   • products (MongoDB proxy)   → product names and categories
#
# Method:
#   Combined affinity score = 0.6 × co_buy_score + 0.4 × co_view_score
#   (co-purchase weighted higher — stronger buying signal)
# ═══════════════════════════════════════════════════════════════════════════════

def product_recommendations():
    print("\n" + "═" * 60)
    print("  Q2: Product Affinity & Recommendations")
    print("═" * 60)

    # Load co-buy and co-view scores
    cobuys  = load("product_cobuys",  base=ANALYTICS_DIR)
    coviews = load("product_coviews", base=ANALYTICS_DIR)
    products = load("products").select(
        "product_id", "name", "category_id", "current_price", "is_active"
    )

    # Normalise co-buy counts to [0,1]
    max_cobuy  = cobuys.agg(F.max("co_purchase_count")).collect()[0][0] or 1
    max_coview = coviews.agg(F.max("co_view_count")).collect()[0][0] or 1

    cobuys  = cobuys.withColumn(
        "cobuy_score", F.col("co_purchase_count") / max_cobuy
    )
    coviews = coviews.withColumn(
        "coview_score", F.col("co_view_count") / max_coview
    )

    # Full outer join on the pair (product_a, product_b)
    affinity = (
        cobuys.select("product_a", "product_b", "cobuy_score")
        .join(
            coviews.select("product_a", "product_b", "coview_score"),
            on=["product_a", "product_b"],
            how="outer"
        )
        .fillna(0.0, subset=["cobuy_score", "coview_score"])
        .withColumn(
            "affinity_score",
            F.round(0.6 * F.col("cobuy_score") + 0.4 * F.col("coview_score"), 6)
        )
    )

    # Rank top-5 recommendations per product_a
    win = Window.partitionBy("product_a").orderBy(F.desc("affinity_score"))
    top5 = (
        affinity
        .withColumn("recommendation_rank", F.rank().over(win))
        .filter(F.col("recommendation_rank") <= 5)
    )

    # Enrich with product metadata
    prod_a = products.select(
        F.col("product_id").alias("product_a"),
        F.col("name").alias("product_a_name"),
        F.col("category_id").alias("product_a_category"),
        F.col("current_price").alias("product_a_price")
    )
    prod_b = products.select(
        F.col("product_id").alias("product_b"),
        F.col("name").alias("recommended_product_name"),
        F.col("category_id").alias("recommended_category"),
        F.col("current_price").alias("recommended_price"),
        F.col("is_active").alias("recommended_is_active")
    )

    recommendations = (
        top5
        .join(prod_a, on="product_a", how="left")
        .join(prod_b, on="product_b", how="left")
        # Only recommend active products
        .filter(F.col("recommended_is_active") == True)
        .select(
            "product_a", "product_a_name", "product_a_category", "product_a_price",
            F.col("product_b").alias("recommended_product_id"),
            "recommended_product_name", "recommended_category",
            "recommended_price", "recommendation_rank",
            "affinity_score", "cobuy_score", "coview_score"
        )
        .orderBy("product_a", "recommendation_rank")
    )

    save(recommendations, "product_recommendations", show_n=20)
    return recommendations


# ═══════════════════════════════════════════════════════════════════════════════
# Q3 — FUNNEL CONVERSION ANALYSIS
# ═══════════════════════════════════════════════════════════════════════════════
#
# Business Question:
#   Where do users drop off in the purchase funnel, and which channels
#   and devices convert best?
#
# Data sources:
#   • sessions (HBase proxy) → page view events, cart, device, referrer
#   • transactions (MongoDB) → completed purchases
#
# Funnel stages:
#   Stage 1 — Session started (all sessions)
#   Stage 2 — Product viewed  (session has viewed_product_count > 0)
#   Stage 3 — Cart initiated  (conversion_status IN abandoned, converted)
#   Stage 4 — Purchase made   (conversion_status = converted)
# ═══════════════════════════════════════════════════════════════════════════════

def funnel_analysis():
    print("\n" + "═" * 60)
    print("  Q3: Funnel Conversion Analysis")
    print("═" * 60)

    sessions = load("sessions")

    # ── 3a. Overall funnel ────────────────────────────────────────────────────
    sessions.createOrReplaceTempView("sessions")
    overall_funnel = spark.sql("""
        SELECT
            COUNT(*)                                                  AS s1_sessions_started,
            SUM(CASE WHEN viewed_product_count > 0 THEN 1 ELSE 0 END) AS s2_product_viewed,
            SUM(CASE WHEN conversion_status IN ('abandoned','converted')
                     THEN 1 ELSE 0 END)                               AS s3_cart_initiated,
            SUM(CASE WHEN conversion_status = 'converted'
                     THEN 1 ELSE 0 END)                               AS s4_purchased
        FROM sessions
    """)

    row = overall_funnel.collect()[0]
    s1, s2, s3, s4 = row
    print(f"""
  ┌─────────────────────────────────────────────────┐
  │  Overall Purchase Funnel                        │
  ├─────────────────────────────────────────────────┤
  │  Stage 1 — Session started    : {s1:>8,}         │
  │  Stage 2 — Product viewed     : {s2:>8,}  ({s2/s1*100:.1f}%)  │
  │  Stage 3 — Cart initiated     : {s3:>8,}  ({s3/s1*100:.1f}%)  │
  │  Stage 4 — Purchase made      : {s4:>8,}  ({s4/s1*100:.1f}%)  │
  └─────────────────────────────────────────────────┘
""")

    # ── 3b. Funnel by device type ─────────────────────────────────────────────
    device_funnel = spark.sql("""
        SELECT
            device_type,
            COUNT(*)                                                   AS sessions,
            SUM(CASE WHEN viewed_product_count > 0 THEN 1 ELSE 0 END) AS product_viewed,
            SUM(CASE WHEN conversion_status IN ('abandoned','converted')
                     THEN 1 ELSE 0 END)                                AS cart_initiated,
            SUM(CASE WHEN conversion_status = 'converted'
                     THEN 1 ELSE 0 END)                                AS purchased,
            ROUND(SUM(CASE WHEN conversion_status = 'converted' THEN 1 ELSE 0 END)
                  / COUNT(*), 4)                                       AS overall_cvr,
            ROUND(AVG(duration_seconds)/60, 2)                        AS avg_session_min,
            ROUND(AVG(page_view_count), 2)                            AS avg_pages
        FROM sessions
        GROUP BY device_type
        ORDER BY overall_cvr DESC
    """)
    print("  Funnel by device type:")
    device_funnel.show(truncate=False)

    # ── 3c. Funnel by referrer / acquisition channel ──────────────────────────
    channel_funnel = spark.sql("""
        SELECT
            referrer                                                   AS channel,
            COUNT(*)                                                   AS sessions,
            SUM(CASE WHEN viewed_product_count > 0 THEN 1 ELSE 0 END) AS product_viewed,
            SUM(CASE WHEN conversion_status IN ('abandoned','converted')
                     THEN 1 ELSE 0 END)                                AS cart_initiated,
            SUM(CASE WHEN conversion_status = 'converted'
                     THEN 1 ELSE 0 END)                                AS purchased,
            ROUND(SUM(CASE WHEN conversion_status = 'converted' THEN 1 ELSE 0 END)
                  / COUNT(*), 4)                                       AS overall_cvr,
            ROUND(AVG(duration_seconds)/60, 2)                        AS avg_session_min
        FROM sessions
        GROUP BY referrer
        ORDER BY overall_cvr DESC
    """)
    print("  Funnel by acquisition channel:")
    channel_funnel.show(truncate=False)

    # ── 3d. Funnel by device × channel (cross-tab) ───────────────────────────
    cross_funnel = spark.sql("""
        SELECT
            device_type,
            referrer AS channel,
            COUNT(*) AS sessions,
            SUM(CASE WHEN conversion_status = 'converted' THEN 1 ELSE 0 END) AS purchased,
            ROUND(SUM(CASE WHEN conversion_status = 'converted' THEN 1 ELSE 0 END)
                  / COUNT(*), 4) AS cvr
        FROM sessions
        GROUP BY device_type, referrer
        ORDER BY cvr DESC
    """)
    print("  Funnel cross-tab (device × channel):")
    cross_funnel.show(30, truncate=False)

    # ── 3e. Cart abandonment deep-dive ────────────────────────────────────────
    abandonment = spark.sql("""
        SELECT
            device_type,
            referrer AS channel,
            SUM(CASE WHEN conversion_status = 'abandoned' THEN 1 ELSE 0 END) AS abandoned,
            SUM(CASE WHEN conversion_status IN ('abandoned','converted')
                     THEN 1 ELSE 0 END)                                        AS had_cart,
            ROUND(
                SUM(CASE WHEN conversion_status = 'abandoned' THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN conversion_status IN ('abandoned','converted')
                                  THEN 1 ELSE 0 END), 0),
            4) AS abandonment_rate
        FROM sessions
        GROUP BY device_type, referrer
        HAVING had_cart > 5
        ORDER BY abandonment_rate DESC
    """)
    print("  Cart abandonment by device × channel:")
    abandonment.show(20, truncate=False)

    # Save all funnel outputs
    device_funnel.coalesce(1).write.mode("overwrite").option("header", True) \
        .csv(os.path.join(INTEGRATION_DIR, "funnel_by_device_csv"))
    channel_funnel.coalesce(1).write.mode("overwrite").option("header", True) \
        .csv(os.path.join(INTEGRATION_DIR, "funnel_by_channel_csv"))
    cross_funnel.coalesce(1).write.mode("overwrite").option("header", True) \
        .csv(os.path.join(INTEGRATION_DIR, "funnel_cross_tab_csv"))
    abandonment.coalesce(1).write.mode("overwrite").option("header", True) \
        .csv(os.path.join(INTEGRATION_DIR, "cart_abandonment_csv"))

    print(f"\n✔  [funnel_analysis]  results saved to {INTEGRATION_DIR}/")
    return device_funnel, channel_funnel


# ═══════════════════════════════════════════════════════════════════════════════
# Q4 — SEASONAL TREND IDENTIFICATION
# ═══════════════════════════════════════════════════════════════════════════════
#
# Business Question:
#   How do sales and browsing behaviour change over time?
#   Are there weekly or monthly patterns worth acting on?
#
# Data sources:
#   • transactions (MongoDB)  → revenue, volume over time
#   • sessions (HBase proxy)  → browsing intensity over time
#   • products (MongoDB)      → category-level trends
# ═══════════════════════════════════════════════════════════════════════════════

def seasonal_trends():
    print("\n" + "═" * 60)
    print("  Q4: Seasonal Trend Identification")
    print("═" * 60)

    txn      = load("transactions")
    sessions = load("sessions")
    products = load("products").select("product_id", "category_id", "current_price")

    # ── 4a. Weekly revenue + browsing intensity ───────────────────────────────
    weekly_revenue = (
        txn
        .filter(F.col("status").isin("completed", "shipped"))
        .groupBy("txn_year", "txn_week")
        .agg(
            F.round(F.sum("total"), 2).alias("weekly_revenue"),
            F.count("transaction_id").alias("weekly_txns"),
            F.countDistinct("user_id").alias("weekly_unique_buyers"),
            F.round(F.avg("total"), 2).alias("weekly_avg_order")
        )
    )

    weekly_sessions = (
        sessions
        .withColumn("week",  F.weekofyear("start_time"))
        .withColumn("year",  F.year("start_time"))
        .groupBy(F.col("year").alias("txn_year"), F.col("week").alias("txn_week"))
        .agg(
            F.count("session_id").alias("weekly_sessions"),
            F.round(F.avg("duration_seconds"), 1).alias("avg_session_duration_s"),
            F.round(F.avg("viewed_product_count"), 2).alias("avg_products_viewed"),
            F.sum(F.when(F.col("conversion_status") == "converted", 1).otherwise(0))
             .alias("weekly_conversions")
        )
    )

    weekly = (
        weekly_revenue
        .join(weekly_sessions, on=["txn_year", "txn_week"], how="outer")
        .fillna(0)
        .withColumn(
            "browse_to_buy_ratio",
            F.round(
                F.when(F.col("weekly_txns") > 0,
                       F.col("weekly_sessions") / F.col("weekly_txns"))
                 .otherwise(F.lit(None).cast(DoubleType())),
                2
            )
        )
        .orderBy("txn_year", "txn_week")
    )

    save(weekly, "seasonal_weekly", show_n=20)

    # ── 4b. Day-of-week revenue pattern ──────────────────────────────────────
    txn.createOrReplaceTempView("transactions")
    dow_pattern = spark.sql("""
        SELECT
            txn_dow,
            CASE txn_dow
                WHEN 1 THEN 'Sunday'   WHEN 2 THEN 'Monday'
                WHEN 3 THEN 'Tuesday'  WHEN 4 THEN 'Wednesday'
                WHEN 5 THEN 'Thursday' WHEN 6 THEN 'Friday'
                WHEN 7 THEN 'Saturday'
            END AS day_name,
            COUNT(transaction_id)       AS txn_count,
            ROUND(SUM(total), 2)        AS total_revenue,
            ROUND(AVG(total), 2)        AS avg_order_value,
            COUNT(DISTINCT user_id)     AS unique_buyers
        FROM transactions
        WHERE status IN ('completed','shipped')
        GROUP BY txn_dow
        ORDER BY txn_dow
    """)
    print("  Day-of-week revenue pattern:")
    dow_pattern.show(truncate=False)
    dow_pattern.coalesce(1).write.mode("overwrite").option("header", True) \
        .csv(os.path.join(INTEGRATION_DIR, "dow_pattern_csv"))

    # ── 4c. Category revenue trend by month ──────────────────────────────────
    txn_items = (
        txn
        .filter(F.col("status").isin("completed", "shipped"))
        .select("transaction_id", "txn_month", "txn_year",
                 F.explode("items").alias("item"))
        .select("transaction_id", "txn_month", "txn_year",
                 F.col("item.product_id").alias("product_id"),
                 F.col("item.quantity").alias("quantity"),
                 F.col("item.unit_price").alias("unit_price"))
    )

    cat_monthly = (
        txn_items
        .join(products, on="product_id", how="left")
        .groupBy("txn_year", "txn_month", "category_id")
        .agg(
            F.round(F.sum(F.col("quantity") * F.col("unit_price")), 2)
             .alias("category_monthly_revenue"),
            F.sum("quantity").alias("units_sold"),
            F.countDistinct("transaction_id").alias("orders")
        )
        .orderBy("txn_year", "txn_month", F.desc("category_monthly_revenue"))
    )

    save(cat_monthly, "seasonal_category_monthly", show_n=20)

    # ── 4d. Browsing trends — most-viewed categories per week (HBase layer) ──
    sessions_products = (
        sessions
        .withColumn("week", F.weekofyear("start_time"))
        .withColumn("year", F.year("start_time"))
        .select("session_id", "year", "week",
                 F.explode("page_views").alias("pv"))
        .filter(F.col("pv.category_id").isNotNull())
        .groupBy("year", "week", F.col("pv.category_id").alias("category_id"))
        .agg(F.count("*").alias("page_view_count"))
        .orderBy("year", "week", F.desc("page_view_count"))
    )

    save(sessions_products, "seasonal_category_views_weekly", show_n=20)

    return weekly, cat_monthly


# ── MAIN ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("  Part 3 — Analytics Integration")
    print("=" * 60)

    os.makedirs(INTEGRATION_DIR, exist_ok=True)

    clv_estimation()
    product_recommendations()
    funnel_analysis()
    seasonal_trends()

    print("\n" + "=" * 60)
    print("  All integration outputs saved to:", INTEGRATION_DIR)
    print("=" * 60)
    spark.stop()
