"""
spark_processing.py
ULK Software Engineering Final Exam-Tuyizere Andre-202540326
 – Part 2 & 3

PySpark batch processing pipeline:
  - Data cleaning & normalization
  - Product recommendation indicators (co-purchase matrix)
  - Cohort analysis (users grouped by registration month)
  - Spark SQL analytics
  - Integrated CLV estimation (cross-system query simulation)

Usage:
    spark-submit spark_processing.py
    OR: python spark_processing.py  (runs in local simulation mode)
"""

import json, os, sys, math, random
from datetime import datetime
from collections import defaultdict
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = os.environ["PATH"] + ";C:\\hadoop\\bin"
DATA_DIR = "ecommerce_data"

# ── Try importing PySpark ─────────────────────────────────────────────────────
SPARK_AVAILABLE = False
try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import (StructType, StructField, StringType,
                                   DoubleType, IntegerType, BooleanType,
                                   ArrayType, TimestampType)
    from pyspark.sql.window import Window
    SPARK_AVAILABLE = True
except ImportError:
    print("[INFO] PySpark not installed. Running in local Python simulation mode.")

# ════════════════════════════════════════════════════════════════════════════
# LOCAL SIMULATION (pure Python) – mimics Spark transformations
# ════════════════════════════════════════════════════════════════════════════

def load_json(filename):
    path = os.path.join(DATA_DIR, filename)
    with open(path) as f:
        return json.load(f)

def load_all_sessions():
    sessions = []
    for i in range(4):
        sessions.extend(load_json(f"sessions_{i}.json"))
    return sessions

# ── Step 1: Data Cleaning & Normalization ─────────────────────────────────────
def clean_transactions(txns):
    """
    Cleaning steps:
    1. Drop records with missing total (null safety)
    2. Normalize status to lowercase
    3. Parse timestamp to datetime object
    4. Add derived field: discount_pct
    5. Filter out negative totals (data quality)
    """
    cleaned = []
    dropped = 0
    for t in txns:
        # null safety
        if t.get("total") is None or t.get("user_id") is None:
            dropped += 1
            continue
        # normalize
        t["status"] = t.get("status", "unknown").lower().strip()
        # parse timestamp
        try:
            t["_parsed_ts"] = datetime.fromisoformat(t["timestamp"])
        except Exception:
            t["_parsed_ts"] = datetime(2026, 3, 7)
        # derived field
        if t.get("subtotal", 0) > 0:
            t["discount_pct"] = round(t.get("discount",0) / t["subtotal"] * 100, 2)
        else:
            t["discount_pct"] = 0.0
        # quality filter
        if t["total"] < 0:
            dropped += 1
            continue
        cleaned.append(t)
    return cleaned, dropped

def clean_sessions(sessions):
    """
    Cleaning steps:
    1. Fill missing referrer with 'unknown'
    2. Normalize conversion_status
    3. Clip duration_seconds to reasonable bounds (1s - 4h)
    4. Remove sessions with no page_views
    """
    cleaned, dropped = [], 0
    for s in sessions:
        if not s.get("page_views"):
            dropped += 1
            continue
        s["referrer"] = s.get("referrer") or "unknown"
        s["conversion_status"] = s.get("conversion_status","browsing").lower()
        s["duration_seconds"] = max(1, min(s.get("duration_seconds",0), 14400))
        cleaned.append(s)
    return cleaned, dropped

# ── Step 2: Co-Purchase Recommendation Indicators ────────────────────────────
def compute_copurchase_matrix(txns, top_n=10):
    """
    "Users who bought X also bought Y"
    Returns: list of (product_a, product_b, co_purchase_count) tuples
    """
    # Build user → product sets
    user_products = defaultdict(set)
    for t in txns:
        if t["status"] == "completed":
            for item in t.get("items", []):
                user_products[t["user_id"]].add(item["product_id"])

    # Count co-occurrences
    copurchase = defaultdict(int)
    for uid, prods in user_products.items():
        prods = sorted(prods)
        for i in range(len(prods)):
            for j in range(i+1, len(prods)):
                pair = (prods[i], prods[j])
                copurchase[pair] += 1

    # Top pairs
    top = sorted(copurchase.items(), key=lambda x: -x[1])[:top_n]
    return [(a, b, count) for (a,b), count in top]

def compute_frequently_viewed_together(sessions, top_n=10):
    """
    "Products frequently viewed together" from session data.
    """
    coview = defaultdict(int)
    for s in sessions:
        prods = sorted(set(s.get("viewed_products",[])))
        for i in range(len(prods)):
            for j in range(i+1, len(prods)):
                coview[(prods[i], prods[j])] += 1

    top = sorted(coview.items(), key=lambda x: -x[1])[:top_n]
    return [(a, b, count) for (a,b), count in top]

# ── Step 3: Cohort Analysis ────────────────────────────────────────────────────
def cohort_analysis(users, txns):
    """
    Group users by registration month → analyze spending in subsequent months.
    Returns: dict {cohort_month: {spend_month_offset: {users, total_spend, orders}}}
    """
    # Map user_id → registration month
    user_cohort = {}
    for u in users:
        try:
            reg = datetime.fromisoformat(u["registration_date"])
            user_cohort[u["user_id"]] = (reg.year, reg.month)
        except Exception:
            pass

    cohorts = defaultdict(lambda: defaultdict(lambda: {"users":set(),"spend":0.0,"orders":0}))
    for t in txns:
        if t.get("status") not in ("completed","shipped"):
            continue
        uid = t["user_id"]
        if uid not in user_cohort:
            continue
        cohort_ym = user_cohort[uid]
        txn_ym = (t["_parsed_ts"].year, t["_parsed_ts"].month)
        # offset in months
        offset = (txn_ym[0]-cohort_ym[0])*12 + (txn_ym[1]-cohort_ym[1])
        cohorts[cohort_ym][offset]["users"].add(uid)
        cohorts[cohort_ym][offset]["spend"] += t["total"]
        cohorts[cohort_ym][offset]["orders"] += 1

    # Flatten sets to counts
    result = {}
    for cohort, months in sorted(cohorts.items()):
        label = f"{cohort[0]}-{cohort[1]:02d}"
        result[label] = {}
        for offset, data in sorted(months.items()):
            result[label][offset] = {
                "active_users": len(data["users"]),
                "total_spend":  round(data["spend"], 2),
                "orders":       data["orders"],
                "avg_spend":    round(data["spend"]/max(1,len(data["users"])), 2)
            }
    return result

# ── Step 4: Integrated CLV Estimation ─────────────────────────────────────────
def estimate_clv(users, txns, sessions, months_projection=6):
    # User spend & order history
    user_stats = defaultdict(lambda: {"spend":0.0,"orders":0,"sessions":0,
                                       "converted_sessions":0,"total_duration":0})
    for t in txns:
        if t["status"] in ("completed","shipped"):
            user_stats[t["user_id"]]["spend"]  += t["total"]
            user_stats[t["user_id"]]["orders"] += 1

    for s in sessions:
        uid = s["user_id"]
        user_stats[uid]["sessions"] += 1
        user_stats[uid]["total_duration"] += s["duration_seconds"]
        if s["conversion_status"] == "converted":
            user_stats[uid]["converted_sessions"] += 1

    # User tenure map
    user_tenure = {}
    for u in users:
        try:
            reg = datetime.fromisoformat(u["registration_date"])
            tenure_months = max(1, (datetime(2026,3,7)-reg).days // 30)
            user_tenure[u["user_id"]] = tenure_months
        except Exception:
            user_tenure[u["user_id"]] = 1

    clv_results = []
    for uid, stats in user_stats.items():
        if stats["orders"] == 0:
            continue
        tenure = user_tenure.get(uid, 3)
        avg_monthly_rev = stats["spend"] / tenure
        # retention factor: 0.6 baseline + engagement bonus
        conv_rate = stats["converted_sessions"] / max(1, stats["sessions"])
        avg_duration = stats["total_duration"] / max(1, stats["sessions"])
        engagement_score = min(1.0, conv_rate * 2 + avg_duration / 3600)
        retention = 0.60 + 0.30 * engagement_score
        clv = round(avg_monthly_rev * months_projection * retention, 2)
        segment = "High" if clv > 500 else ("Medium" if clv > 150 else "Low")
        clv_results.append({
            "user_id": uid,
            "clv_estimate": clv,
            "avg_monthly_revenue": round(avg_monthly_rev, 2),
            "total_orders": stats["orders"],
            "total_spend": round(stats["spend"], 2),
            "engagement_score": round(engagement_score, 3),
            "clv_segment": segment
        })

    clv_results.sort(key=lambda x: -x["clv_estimate"])
    return clv_results

# ════════════════════════════════════════════════════════════════════════════
# SPARK SQL QUERIES (string definitions for documentation)
# ════════════════════════════════════════════════════════════════════════════

SPARK_SQL_QUERIES = {

"q1_top_revenue_products": """
-- Top 10 products by revenue from completed/shipped transactions
SELECT
    i.product_id,
    SUM(i.subtotal)   AS total_revenue,
    SUM(i.quantity)   AS total_units,
    COUNT(DISTINCT t.transaction_id) AS order_count,
    ROUND(AVG(i.unit_price), 2)      AS avg_price
FROM transactions t
LATERAL VIEW explode(t.items) itemTable AS i
WHERE t.status IN ('completed', 'shipped')
GROUP BY i.product_id
ORDER BY total_revenue DESC
LIMIT 10
""",

"q2_monthly_revenue": """
-- Monthly revenue trend with MoM growth rate
SELECT
    DATE_FORMAT(CAST(timestamp AS TIMESTAMP), 'yyyy-MM') AS month,
    COUNT(*)                            AS order_count,
    ROUND(SUM(total), 2)                AS total_revenue,
    ROUND(AVG(total), 2)                AS avg_order_value,
    ROUND(SUM(discount), 2)             AS total_discounts,
    ROUND(SUM(discount)/SUM(subtotal)*100, 1) AS discount_rate_pct
FROM transactions
WHERE status IN ('completed', 'shipped')
GROUP BY DATE_FORMAT(CAST(timestamp AS TIMESTAMP), 'yyyy-MM')
ORDER BY month
""",

"q3_device_conversion": """
-- Conversion rate by device type (joining sessions + transactions)
SELECT
    s.device_type,
    COUNT(DISTINCT s.session_id)  AS total_sessions,
    COUNT(DISTINCT t.transaction_id) AS conversions,
    ROUND(COUNT(DISTINCT t.transaction_id) /
          COUNT(DISTINCT s.session_id) * 100, 2) AS conversion_rate_pct,
    ROUND(AVG(s.duration_seconds) / 60, 1) AS avg_session_min
FROM sessions s
LEFT JOIN transactions t ON s.session_id = t.session_id
GROUP BY s.device_type
ORDER BY conversion_rate_pct DESC
""",

"q4_referrer_revenue": """
-- Revenue and conversions by traffic referrer source
SELECT
    s.referrer,
    COUNT(DISTINCT s.session_id)     AS sessions,
    COUNT(DISTINCT t.transaction_id) AS conversions,
    ROUND(SUM(t.total), 2)           AS total_revenue,
    ROUND(AVG(t.total), 2)           AS avg_order_value
FROM sessions s
LEFT JOIN transactions t ON s.session_id = t.session_id AND t.status IN ('completed','shipped')
GROUP BY s.referrer
ORDER BY total_revenue DESC NULLS LAST
""",

"q5_product_view_to_purchase_funnel": """
-- Funnel analysis: views → cart adds → purchases per product
WITH product_views AS (
    SELECT pv.product_id, COUNT(*) AS view_count
    FROM sessions s
    LATERAL VIEW explode(s.page_views) pvTable AS pv
    WHERE pv.page_type = 'product_detail' AND pv.product_id IS NOT NULL
    GROUP BY pv.product_id
),
cart_adds AS (
    SELECT cc.product_id, SUM(cc.quantity) AS cart_qty
    FROM sessions s
    LATERAL VIEW explode(map_values(s.cart_contents)) ccTable AS cc
    GROUP BY cc.product_id
),
purchases AS (
    SELECT i.product_id, SUM(i.quantity) AS purchased_qty
    FROM transactions t
    LATERAL VIEW explode(t.items) iTable AS i
    WHERE t.status IN ('completed','shipped')
    GROUP BY i.product_id
)
SELECT
    v.product_id,
    v.view_count,
    COALESCE(c.cart_qty, 0)   AS cart_adds,
    COALESCE(p.purchased_qty, 0) AS purchases,
    ROUND(COALESCE(c.cart_qty,0) / v.view_count * 100, 2) AS view_to_cart_pct,
    ROUND(COALESCE(p.purchased_qty,0) / v.view_count * 100, 2) AS view_to_purchase_pct
FROM product_views v
LEFT JOIN cart_adds c ON v.product_id = c.product_id
LEFT JOIN purchases  p ON v.product_id = p.product_id
ORDER BY view_count DESC
LIMIT 20
""",
}

# ════════════════════════════════════════════════════════════════════════════
# PYSPARK IMPLEMENTATION (runs if Spark is available)
# ════════════════════════════════════════════════════════════════════════════

def run_with_spark():
    spark = SparkSession.builder \
        .appName("ULK_ECommerce_Analytics") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.driver.memory", "2g") \
        .master("local[*]") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Load JSON files into DataFrames
    txn_df = spark.read.json(f"{DATA_DIR}/transactions.json")
    usr_df = spark.read.json(f"{DATA_DIR}/users.json")
    prd_df = spark.read.json(f"{DATA_DIR}/products.json")
    ses_df = spark.read.json(f"{DATA_DIR}/sessions_*.json")

    # Register as temp views for Spark SQL
    txn_df.createOrReplaceTempView("transactions")
    usr_df.createOrReplaceTempView("users")
    prd_df.createOrReplaceTempView("products")
    ses_df.createOrReplaceTempView("sessions")

    print("\n── Spark Schema ──────────────────────────────────────────")
    txn_df.printSchema()

    print("\n── Q1: Top Revenue Products ──────────────────────────────")
    txn_df.filter(F.col("status").isin("completed","shipped")) \
          .withColumn("item", F.explode("items")) \
          .groupBy("item.product_id") \
          .agg(
              F.round(F.sum("item.subtotal"),2).alias("total_revenue"),
              F.sum("item.quantity").alias("units_sold"),
              F.countDistinct("transaction_id").alias("orders")
          ).orderBy(F.desc("total_revenue")).show(10)

    print("\n── Q2: Monthly Revenue ───────────────────────────────────")
    txn_df.filter(F.col("status").isin("completed","shipped")) \
          .withColumn("month", F.date_format(F.col("timestamp").cast("timestamp"), "yyyy-MM")) \
          .groupBy("month") \
          .agg(
              F.count("*").alias("orders"),
              F.round(F.sum("total"),2).alias("revenue"),
              F.round(F.avg("total"),2).alias("avg_order")
          ).orderBy("month").show()

    print("\n── Q3: Device Conversion Rate ────────────────────────────")
    device_conv = ses_df.withColumn("device_type",
                    F.col("device_profile.type")) \
        .join(txn_df.select("session_id","transaction_id"),
              on="session_id", how="left") \
        .groupBy("device_type") \
        .agg(
            F.countDistinct("session_id").alias("sessions"),
            F.countDistinct("transaction_id").alias("conversions"),
            F.round(F.avg("duration_seconds")/60,1).alias("avg_min")
        ).withColumn("conv_pct",
                     F.round(F.col("conversions")/F.col("sessions")*100,2)) \
        .orderBy(F.desc("conv_pct"))
    device_conv.show()

    spark.stop()
    return spark

# ════════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════════

def main():
    print("=" * 60)
    print("ULK Final Exam-Tuyizere Andre-202540326 – Spark Processing")
    print("=" * 60)

    if SPARK_AVAILABLE:
        print("\nRunning with PySpark...")
        run_with_spark()
        return

    # Local Python simulation
    print("\n[Local Mode] Loading data...")
    users    = load_json("users.json")
    txns_raw = load_json("transactions.json")
    sessions = load_all_sessions()

    print("\n── Step 1: Data Cleaning ─────────────────────────────────")
    txns, dropped_t = clean_transactions(txns_raw)
    sess, dropped_s = clean_sessions(sessions)
    print(f"  Transactions: {len(txns_raw)} → {len(txns)} cleaned ({dropped_t} dropped)")
    print(f"  Sessions:     {len(sessions)} → {len(sess)} cleaned ({dropped_s} dropped)")
    # Check a cleaned record
    sample = txns[0]
    print(f"  Sample cleaned txn: status='{sample['status']}' "
          f"discount_pct={sample['discount_pct']}% total=${sample['total']:.2f}")

    print("\n── Step 2a: Co-Purchase Recommendations ─────────────────")
    copurchase = compute_copurchase_matrix(txns, top_n=8)
    print(f"  {'Product A':<14} {'Product B':<14} {'Co-Purchases':>13}")
    for a, b, cnt in copurchase:
        print(f"  {a:<14} {b:<14} {cnt:>13}")

    print("\n── Step 2b: Frequently Viewed Together ──────────────────")
    coview = compute_frequently_viewed_together(sess, top_n=5)
    for a, b, cnt in coview:
        print(f"  {a} + {b}: {cnt} sessions")

    print("\n── Step 3: Cohort Analysis ───────────────────────────────")
    cohorts = cohort_analysis(users, txns)
    print(f"  {'Cohort':<10} {'Month+0':>9} {'Month+1':>9} {'Month+2':>9}")
    for cohort, months in list(cohorts.items())[:4]:
        row = f"  {cohort:<10}"
        for offset in [0, 1, 2]:
            d = months.get(offset, {})
            row += f"  ${d.get('total_spend',0):>7,.0f}"
        print(row)

    print("\n── Step 4: CLV Estimation ────────────────────────────────")
    clv = estimate_clv(users, txns, sess)
    print(f"  Total users with CLV estimate: {len(clv)}")
    segs = defaultdict(list)
    for c in clv:
        segs[c["clv_segment"]].append(c["clv_estimate"])
    for seg in ["High","Medium","Low"]:
        vals = segs[seg]
        if vals:
            print(f"  {seg:<8}: {len(vals):>4} users | avg CLV ${sum(vals)/len(vals):>8,.2f} "
                  f"| max ${max(vals):>8,.2f}")
    print("\n  Top 5 users by CLV:")
    for c in clv[:5]:
        print(f"  {c['user_id']} | CLV=${c['clv_estimate']:>8,.2f} | "
              f"orders={c['total_orders']} | engagement={c['engagement_score']:.3f} | "
              f"segment={c['clv_segment']}")

    print("\n── Spark SQL Queries (defined) ───────────────────────────")
    for name, sql in SPARK_SQL_QUERIES.items():
        print(f"  Query '{name}': {sql.strip().splitlines()[1].strip()}")

    print("\n── Pipeline Summary ──────────────────────────────────────")
    print("  1. Ingest: spark.read.json('ecommerce_data/*.json')")
    print("  2. Clean:  dropna, normalize status/dates, clip durations")
    print("  3. Transform: explode items array, parse timestamps, join datasets")
    print("  4. Analyze: groupBy, agg, Spark SQL views")
    print("  5. Output: write.parquet / write.csv for visualization")
    print("  Optimization: partition by date, broadcast join products (<100MB)")

    return txns, sess, users, clv

if __name__ == "__main__":
    main()
