from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType
import os

# ── Spark session ─────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("ULK_ECommerce_BatchAnalytics")
    .config("spark.sql.shuffle.partitions", "50")   # right-size for local run
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

CLEAN_DIR     = "cleaned"
ANALYTICS_DIR = "analytics"


# ── Helpers ───────────────────────────────────────────────────────────────────

def load(name):
    return spark.read.parquet(os.path.join(CLEAN_DIR, name))


def save(df, name: str, also_csv: bool = True, show_n: int = 10):
    out_parquet = os.path.join(ANALYTICS_DIR, name)
    df.write.mode("overwrite").parquet(out_parquet)
    if also_csv:
        out_csv = os.path.join(ANALYTICS_DIR, name + "_csv")
        df.coalesce(1).write.mode("overwrite").option("header", True).csv(out_csv)
    print(f"\n✔  [{name}]  {df.count():,} rows saved")
    df.show(show_n, truncate=False)


# ═══════════════════════════════════════════════════════════════════════════════
# ANALYSIS 1A — "Users who bought X also bought Y" (co-purchase pairs)
# ═══════════════════════════════════════════════════════════════════════════════

def product_cobuys():
    print("\n" + "─" * 55)
    print("  Analysis 1a: Co-purchase product pairs")
    print("─" * 55)

    txn = load("transactions")

    # Explode items array to get one row per (transaction, product)
    items = (
        txn
        .select("transaction_id", "user_id",
                 F.explode("items").alias("item"))
        .select("transaction_id", "user_id",
                 F.col("item.product_id").alias("product_id"))
        .filter(F.col("product_id").isNotNull())
    )

    # Self-join on transaction_id to find pairs bought together
    pairs = (
        items.alias("a")
        .join(items.alias("b"), on="transaction_id")
        .filter(F.col("a.product_id") < F.col("b.product_id"))   # avoid dupes
        .select(
            F.col("a.product_id").alias("product_a"),
            F.col("b.product_id").alias("product_b")
        )
    )

    # Count co-occurrences
    cobuys = (
        pairs
        .groupBy("product_a", "product_b")
        .agg(F.count("*").alias("co_purchase_count"))
        .orderBy(F.desc("co_purchase_count"))
    )

    # Add support metric: co_purchase_count / total_transactions
    total_txns = txn.count()
    cobuys = cobuys.withColumn(
        "support",
        F.round(F.col("co_purchase_count") / F.lit(total_txns), 6)
    )

    # Keep only pairs with at least 2 co-purchases (filter noise)
    cobuys = cobuys.filter(F.col("co_purchase_count") >= 2)

    save(cobuys, "product_cobuys")
    return cobuys


# ═══════════════════════════════════════════════════════════════════════════════
# ANALYSIS 1B — "Products frequently co-viewed" (from session browsing data)
# ═══════════════════════════════════════════════════════════════════════════════

def product_coviews():
    print("\n" + "─" * 55)
    print("  Analysis 1b: Co-viewed product pairs (session browsing)")
    print("─" * 55)

    sessions = load("sessions")

    # Each session has a viewed_products array — explode to rows
    viewed = (
        sessions
        .select("session_id", F.explode("viewed_products").alias("product_id"))
        .filter(F.col("product_id").isNotNull())
    )

    # Self-join within same session
    pairs = (
        viewed.alias("a")
        .join(viewed.alias("b"), on="session_id")
        .filter(F.col("a.product_id") < F.col("b.product_id"))
        .select(
            F.col("a.product_id").alias("product_a"),
            F.col("b.product_id").alias("product_b")
        )
    )

    coviews = (
        pairs
        .groupBy("product_a", "product_b")
        .agg(F.count("*").alias("co_view_count"))
        .orderBy(F.desc("co_view_count"))
        .filter(F.col("co_view_count") >= 3)   # filter noise
    )

    save(coviews, "product_coviews")
    return coviews


# ═══════════════════════════════════════════════════════════════════════════════
# ANALYSIS 2 — Cohort Analysis (users grouped by registration month)
# ═══════════════════════════════════════════════════════════════════════════════

def cohort_spending():
    """
    For each (cohort_month, activity_month) bucket, compute:
      - unique active users
      - total transactions
      - total revenue
      - avg order value
      - cohort retention rate = active_users / cohort_size
    """
    print("\n" + "─" * 55)
    print("  Analysis 2a: Cohort spending patterns")
    print("─" * 55)

    users = load("users").select(
        "user_id",
        F.date_format("registration_date", "yyyy-MM").alias("cohort_month")
    )

    txn = load("transactions").select(
        "transaction_id", "user_id", "total",
        F.date_format("timestamp", "yyyy-MM").alias("activity_month")
    )

    # Cohort sizes
    cohort_sizes = (
        users
        .groupBy("cohort_month")
        .agg(F.count("user_id").alias("cohort_size"))
    )

    # Join transactions with cohort assignment
    txn_cohort = txn.join(users, on="user_id", how="inner")

    # Aggregate per cohort × activity month
    spending = (
        txn_cohort
        .groupBy("cohort_month", "activity_month")
        .agg(
            F.countDistinct("user_id").alias("active_users"),
            F.count("transaction_id").alias("transaction_count"),
            F.round(F.sum("total"), 2).alias("total_revenue"),
            F.round(F.avg("total"), 2).alias("avg_order_value")
        )
    )

    # Attach cohort size and compute retention rate
    spending = (
        spending
        .join(cohort_sizes, on="cohort_month", how="left")
        .withColumn(
            "retention_rate",
            F.round(F.col("active_users") / F.col("cohort_size"), 4)
        )
        .orderBy("cohort_month", "activity_month")
    )

    save(spending, "cohort_spending")
    return spending


def cohort_retention():
    """
    Build a pivot table: cohort_month (rows) × months_since_registration (cols)
    showing retention rate — the classic cohort retention grid.
    """
    print("\n" + "─" * 55)
    print("  Analysis 2b: Cohort retention grid")
    print("─" * 55)

    users = load("users").select(
        "user_id",
        F.trunc("registration_date", "month").alias("cohort_start")
    )

    txn = load("transactions").select(
        "user_id",
        F.trunc("timestamp", "month").alias("activity_start")
    )

    # Join and compute months since cohort start
    joined = (
        txn.join(users, on="user_id", how="inner")
        .withColumn(
            "months_since_start",
            F.months_between("activity_start", "cohort_start").cast(IntegerType())
        )
        .filter(F.col("months_since_start") >= 0)
        .withColumn("cohort_month", F.date_format("cohort_start", "yyyy-MM"))
    )

    # Count distinct active users per (cohort, offset)
    active = (
        joined
        .groupBy("cohort_month", "months_since_start")
        .agg(F.countDistinct("user_id").alias("active_users"))
    )

    # Cohort sizes at month 0
    cohort_size = (
        active
        .filter(F.col("months_since_start") == 0)
        .select(
            "cohort_month",
            F.col("active_users").alias("cohort_size")
        )
    )

    retention = (
        active
        .join(cohort_size, on="cohort_month")
        .withColumn(
            "retention_rate",
            F.round(F.col("active_users") / F.col("cohort_size"), 4)
        )
        .select("cohort_month", "months_since_start", "active_users",
                "cohort_size", "retention_rate")
        .orderBy("cohort_month", "months_since_start")
    )

    save(retention, "cohort_retention")
    return retention


# ═══════════════════════════════════════════════════════════════════════════════
# PART 2 — Spark SQL Analytics
# ═══════════════════════════════════════════════════════════════════════════════

def spark_sql_analytics():
    """
    Demonstrates Spark SQL queries over the cleaned DataFrames.
    Creates temp views so plain SQL can be used.
    """
    print("\n" + "═" * 55)
    print("  Part 2 — Spark SQL Analytics")
    print("═" * 55)

    # Register views
    load("users").createOrReplaceTempView("users")
    load("products").createOrReplaceTempView("products")
    load("sessions").createOrReplaceTempView("sessions")
    load("transactions").createOrReplaceTempView("transactions")
    load("categories").createOrReplaceTempView("categories")

    # ── SQL 1: Top 10 products by revenue ────────────────────────────────────
    print("\n[SQL 1] Top 10 products by total revenue")
    sql1 = spark.sql("""
        SELECT
            i.product_id,
            ROUND(SUM(i.unit_price * i.quantity), 2)  AS total_revenue,
            SUM(i.quantity)                            AS units_sold,
            COUNT(DISTINCT t.transaction_id)           AS num_orders
        FROM transactions t
        LATERAL VIEW explode(t.items) tmp AS i
        WHERE t.status IN ('completed', 'shipped')
        GROUP BY i.product_id
        ORDER BY total_revenue DESC
        LIMIT 10
    """)
    sql1.show(truncate=False)

    out1 = os.path.join(ANALYTICS_DIR, "sql1_top_products_by_revenue")
    sql1.coalesce(1).write.mode("overwrite").option("header", True).csv(out1)

    # ── SQL 2: Monthly revenue trend ─────────────────────────────────────────
    print("\n[SQL 2] Monthly revenue trend")
    sql2 = spark.sql("""
        SELECT
            txn_year,
            txn_month,
            COUNT(transaction_id)      AS total_transactions,
            ROUND(SUM(total), 2)       AS monthly_revenue,
            ROUND(AVG(total), 2)       AS avg_order_value,
            COUNT(DISTINCT user_id)    AS unique_customers
        FROM transactions
        WHERE status NOT IN ('cancelled', 'refunded')
        GROUP BY txn_year, txn_month
        ORDER BY txn_year, txn_month
    """)
    sql2.show(20, truncate=False)

    out2 = os.path.join(ANALYTICS_DIR, "sql2_monthly_revenue")
    sql2.coalesce(1).write.mode("overwrite").option("header", True).csv(out2)

    # ── SQL 3: Device & referrer conversion analysis ──────────────────────────
    print("\n[SQL 3] Session conversion rate by device type and referrer")
    sql3 = spark.sql("""
        SELECT
            device_type,
            referrer,
            COUNT(*)                                           AS total_sessions,
            SUM(CASE WHEN conversion_status = 'converted'
                     THEN 1 ELSE 0 END)                        AS conversions,
            ROUND(
                SUM(CASE WHEN conversion_status = 'converted'
                         THEN 1 ELSE 0 END)
                / COUNT(*), 4)                                 AS conversion_rate,
            ROUND(AVG(duration_seconds) / 60, 2)              AS avg_session_min,
            ROUND(AVG(page_view_count), 1)                    AS avg_page_views
        FROM sessions
        GROUP BY device_type, referrer
        ORDER BY conversion_rate DESC
    """)
    sql3.show(30, truncate=False)

    out3 = os.path.join(ANALYTICS_DIR, "sql3_conversion_by_device_referrer")
    sql3.coalesce(1).write.mode("overwrite").option("header", True).csv(out3)

    # ── SQL 4: User segmentation by purchase frequency & spending ────────────
    print("\n[SQL 4] User segmentation (RFM-style: frequency & monetary)")
    sql4 = spark.sql("""
        WITH user_stats AS (
            SELECT
                user_id,
                COUNT(transaction_id)      AS frequency,
                ROUND(SUM(total), 2)       AS monetary,
                MAX(timestamp)             AS last_purchase
            FROM transactions
            WHERE status NOT IN ('cancelled')
            GROUP BY user_id
        )
        SELECT
            CASE
                WHEN frequency >= 5 AND monetary >= 500 THEN 'Champions'
                WHEN frequency >= 3 AND monetary >= 200 THEN 'Loyal'
                WHEN frequency = 1 AND monetary >= 100  THEN 'High-Value New'
                WHEN frequency >= 2                     THEN 'Potential Loyal'
                ELSE 'One-Time'
            END                                AS segment,
            COUNT(*)                           AS num_users,
            ROUND(AVG(frequency), 2)           AS avg_frequency,
            ROUND(AVG(monetary), 2)            AS avg_monetary,
            ROUND(SUM(monetary), 2)            AS total_revenue_from_segment
        FROM user_stats
        GROUP BY segment
        ORDER BY total_revenue_from_segment DESC
    """)
    sql4.show(truncate=False)

    out4 = os.path.join(ANALYTICS_DIR, "sql4_user_segments")
    sql4.coalesce(1).write.mode("overwrite").option("header", True).csv(out4)

    # ── SQL 5: Category performance ──────────────────────────────────────────
    print("\n[SQL 5] Revenue and volume by product category")
    sql5 = spark.sql("""
        SELECT
            p.category_id,
            COUNT(DISTINCT p.product_id)               AS products_in_cat,
            SUM(i.quantity)                            AS total_units_sold,
            ROUND(SUM(i.unit_price * i.quantity), 2)   AS category_revenue,
            ROUND(AVG(i.unit_price), 2)                AS avg_unit_price
        FROM transactions t
        LATERAL VIEW explode(t.items) tmp AS i
        JOIN products p ON p.product_id = i.product_id
        WHERE t.status IN ('completed', 'shipped')
        GROUP BY p.category_id
        ORDER BY category_revenue DESC
    """)
    sql5.show(25, truncate=False)

    out5 = os.path.join(ANALYTICS_DIR, "sql5_category_performance")
    sql5.coalesce(1).write.mode("overwrite").option("header", True).csv(out5)

    print("\n✔  All Spark SQL results saved under:", ANALYTICS_DIR)


# ── MAIN ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 55)
    print("  Part 2b — Batch Analytics & Spark SQL")
    print("=" * 55)

    # Batch analytics
    product_cobuys()
    product_coviews()
    cohort_spending()
    cohort_retention()

    # Spark SQL
    spark_sql_analytics()

    print("\n" + "=" * 55)
    print("  All analytics outputs saved to:", ANALYTICS_DIR)
    print("=" * 55)
    spark.stop()
