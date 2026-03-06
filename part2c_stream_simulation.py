
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, LongType
import os

# ── Spark session ─────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("ULK_ECommerce_StreamSimulation")
    .config("spark.sql.shuffle.partitions", "50")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

CLEAN_DIR     = "cleaned"
STREAMING_DIR = "streaming"


# ── Helpers ───────────────────────────────────────────────────────────────────

def load(name):
    return spark.read.parquet(os.path.join(CLEAN_DIR, name))


def save(df, name: str, show_n: int = 10):
    out = os.path.join(STREAMING_DIR, name)
    df.write.mode("overwrite").parquet(out)
    csv_out = out + "_csv"
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(csv_out)
    print(f"\n✔  [{name}]  {df.count():,} rows  →  {out}")
    df.show(show_n, truncate=False)


# ═══════════════════════════════════════════════════════════════════════════════
# STREAM SIM 1 — Windowed revenue (1-hour tumbling windows)
# ═══════════════════════════════════════════════════════════════════════════════

def windowed_revenue():
    """
    Assign each transaction to a 1-hour window bucket, then aggregate.
    Also computes:
      - running cumulative revenue (ordered by window)
      - rolling 6-window (6-hour) average revenue
      - spike flag: window revenue > 2 × rolling avg
    """
    print("\n" + "─" * 55)
    print("  Stream Sim 1: Hourly windowed revenue")
    print("─" * 55)

    txn = (
        load("transactions")
        .filter(F.col("status").isin("completed", "shipped", "processing"))
        .select("transaction_id", "user_id", "timestamp", "total", "item_count")
    )

    # Truncate timestamp to the hour → window key
    windowed = (
        txn
        .withColumn("window_start", F.date_trunc("hour", "timestamp"))
        .withColumn("window_end",
                    F.date_trunc("hour", "timestamp") + F.expr("INTERVAL 1 HOUR"))
        .groupBy("window_start", "window_end")
        .agg(
            F.count("transaction_id").alias("txn_count"),
            F.countDistinct("user_id").alias("unique_users"),
            F.round(F.sum("total"), 2).alias("window_revenue"),
            F.round(F.avg("total"), 2).alias("avg_order_value"),
            F.sum("item_count").alias("total_items_sold")
        )
        .orderBy("window_start")
    )

    # Window spec ordered by window_start
    win_ordered  = Window.orderBy("window_start")
    win_rolling6 = Window.orderBy("window_start").rowsBetween(-5, 0)  # last 6 rows

    windowed = (
        windowed
        # Cumulative revenue
        .withColumn("cumulative_revenue",
                    F.round(F.sum("window_revenue").over(win_ordered), 2))
        # Rolling 6-hour average
        .withColumn("rolling_6h_avg",
                    F.round(F.avg("window_revenue").over(win_rolling6), 2))
        # Spike flag
        .withColumn("is_revenue_spike",
                    F.col("window_revenue") > 2 * F.col("rolling_6h_avg"))
    )

    save(windowed, "windowed_revenue", show_n=24)
    return windowed


# ═══════════════════════════════════════════════════════════════════════════════
# STREAM SIM 2 — Per-window top-3 products (simulating micro-batch ranking)
# ═══════════════════════════════════════════════════════════════════════════════

def product_spikes():
  
    print("\n" + "─" * 55)
    print("  Stream Sim 2: Top products per time window")
    print("─" * 55)

    txn = (
        load("transactions")
        .filter(F.col("status").isin("completed", "shipped", "processing"))
        .select("transaction_id", "timestamp",
                 F.explode("items").alias("item"))
        .select(
            "transaction_id",
            "timestamp",
            F.col("item.product_id").alias("product_id"),
            F.col("item.quantity").alias("quantity"),
            F.col("item.unit_price").alias("unit_price")
        )
    )

    # Assign window
    txn = txn.withColumn(
        "window_start", F.date_trunc("hour", "timestamp")
    )

    # Aggregate per (window, product)
    product_window = (
        txn
        .groupBy("window_start", "product_id")
        .agg(
            F.sum("quantity").alias("units_sold"),
            F.round(F.sum(F.col("quantity") * F.col("unit_price")), 2)
             .alias("window_revenue")
        )
    )

    # Rank within each window
    win_spec = Window.partitionBy("window_start").orderBy(F.desc("units_sold"))
    ranked = (
        product_window
        .withColumn("rank_in_window", F.rank().over(win_spec))
        .filter(F.col("rank_in_window") <= 3)
        .orderBy("window_start", "rank_in_window")
    )

    save(ranked, "product_spikes", show_n=30)
    return ranked


# ═══════════════════════════════════════════════════════════════════════════════
# STREAM SIM 3 — Abandoned-cart rate per hour (session-based)
# ═══════════════════════════════════════════════════════════════════════════════

def abandoned_cart_rate():
    
    print("\n" + "─" * 55)
    print("  Stream Sim 3: Hourly abandoned-cart rate")
    print("─" * 55)

    sessions = (
        load("sessions")
        .select("session_id", "start_time", "conversion_status",
                "viewed_product_count", "page_view_count")
        .withColumn("window_start", F.date_trunc("hour", "start_time"))
        .withColumn("has_cart",
                    F.col("conversion_status").isin("converted", "abandoned"))
    )

    windowed = (
        sessions
        .groupBy("window_start")
        .agg(
            F.count("session_id").alias("total_sessions"),
            F.sum(F.col("has_cart").cast(IntegerType())).alias("sessions_with_cart"),
            F.sum(
                F.when(F.col("conversion_status") == "converted", 1).otherwise(0)
            ).alias("converted_sessions"),
            F.sum(
                F.when(F.col("conversion_status") == "abandoned", 1).otherwise(0)
            ).alias("abandoned_sessions"),
            F.round(F.avg("duration_seconds" if "duration_seconds" in
                           sessions.columns else "page_view_count"), 1)
             .alias("avg_page_views")
        )
        .withColumn(
            "conversion_rate",
            F.round(
                F.when(F.col("sessions_with_cart") > 0,
                       F.col("converted_sessions") / F.col("sessions_with_cart"))
                 .otherwise(F.lit(0.0)),
                4
            )
        )
        .withColumn(
            "abandonment_rate",
            F.round(
                F.when(F.col("sessions_with_cart") > 0,
                       F.col("abandoned_sessions") / F.col("sessions_with_cart"))
                 .otherwise(F.lit(0.0)),
                4
            )
        )
        .orderBy("window_start")
    )

    save(windowed, "abandoned_cart_rate", show_n=24)
    return windowed


# ═══════════════════════════════════════════════════════════════════════════════
# STREAM SIM 4 — Running totals dashboard (global KPIs updated per event batch)
# ═══════════════════════════════════════════════════════════════════════════════

def running_totals():

    print("\n" + "─" * 55)
    print("  Stream Sim 4: Daily running KPI totals")
    print("─" * 55)

    txn = (
        load("transactions")
        .filter(F.col("status").isin("completed", "shipped", "processing"))
        .select("transaction_id", "user_id", "txn_date", "total", "item_count")
    )

    daily = (
        txn
        .groupBy("txn_date")
        .agg(
            F.count("transaction_id").alias("daily_txns"),
            F.countDistinct("user_id").alias("daily_unique_customers"),
            F.round(F.sum("total"), 2).alias("daily_revenue"),
            F.round(F.avg("total"), 2).alias("daily_avg_order"),
            F.sum("item_count").alias("daily_items_sold")
        )
        .orderBy("txn_date")
    )

    win = Window.orderBy("txn_date")

    daily = (
        daily
        .withColumn("running_revenue",
                    F.round(F.sum("daily_revenue").over(win), 2))
        .withColumn("running_txns",
                    F.sum("daily_txns").over(win))
        .withColumn("running_customers",
                    F.sum("daily_unique_customers").over(win))
        # Day-over-day revenue change %
        .withColumn("prev_day_revenue",
                    F.lag("daily_revenue", 1).over(win))
        .withColumn("dod_revenue_change_pct",
                    F.round(
                        F.when(F.col("prev_day_revenue").isNotNull() &
                               (F.col("prev_day_revenue") > 0),
                               (F.col("daily_revenue") - F.col("prev_day_revenue"))
                               / F.col("prev_day_revenue") * 100)
                         .otherwise(F.lit(None).cast(DoubleType())),
                        2
                    ))
        .drop("prev_day_revenue")
    )

    save(daily, "running_totals", show_n=30)
    return daily


# ── MAIN ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 55)
    print("  Part 2c — Simulated Stream Processing")
    print("=" * 55)

    windowed_revenue()
    product_spikes()
    abandoned_cart_rate()
    running_totals()

    print("\n" + "=" * 55)
    print("  All streaming outputs saved to:", STREAMING_DIR)
    print("=" * 55)
    spark.stop()
