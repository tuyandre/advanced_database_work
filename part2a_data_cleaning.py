
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, BooleanType, ArrayType, MapType, TimestampType
)
import os, glob

# ── Spark session ─────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("ULK_ECommerce_DataCleaning")
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

DATA_DIR    = "."          # directory that holds the generated JSON files
OUTPUT_DIR  = "cleaned"    # Parquet output root


# ── Helpers ───────────────────────────────────────────────────────────────────

def save(df, name: str, show_n: int = 5):
    out = os.path.join(OUTPUT_DIR, name)
    df.write.mode("overwrite").parquet(out)
    print(f"\n✔  [{name}]  {df.count():,} rows  →  {out}")
    df.printSchema()
    df.show(show_n, truncate=False)


# ── 1. USERS ──────────────────────────────────────────────────────────────────

def clean_users():
    df = spark.read.option("multiline", True).json(
        os.path.join(DATA_DIR, "users.json")
    )

    df = (
        df
        # Drop rows with null user_id (primary key)
        .filter(F.col("user_id").isNotNull())
        # Standardise email to lowercase
        .withColumn("email", F.lower(F.trim(F.col("email"))))
        # Parse timestamps
        .withColumn("registration_date",
                    F.to_timestamp("registration_date", "yyyy-MM-dd'T'HH:mm:ss"))
        .withColumn("last_active",
                    F.to_timestamp("last_active", "yyyy-MM-dd'T'HH:mm:ss"))
        # Clamp age to a sane range; null-out out-of-range values
        .withColumn("age",
                    F.when(F.col("age").between(18, 100), F.col("age"))
                     .otherwise(F.lit(None).cast(IntegerType())))
        # Uppercase state abbreviation
        .withColumn("state", F.upper(F.col("geo_data.state")))
        .withColumn("city",  F.trim(F.col("geo_data.city")))
        .withColumn("country", F.upper(F.col("geo_data.country")))
        # Derive days-since-registration metric
        .withColumn("days_since_registration",
                    F.datediff(F.current_date(),
                               F.to_date("registration_date")))
        # Clean gender — keep only known values
        .withColumn("gender",
                    F.when(F.col("gender").isin("M", "F", "Other"), F.col("gender"))
                     .otherwise(F.lit("Unknown")))
        # Remove raw struct column (replaced by flat columns above)
        .drop("geo_data")
        # De-duplicate on user_id (keep first occurrence)
        .dropDuplicates(["user_id"])
    )

    save(df, "users")
    return df


# ── 2. CATEGORIES ─────────────────────────────────────────────────────────────

def clean_categories():
    df = spark.read.option("multiline", True).json(
        os.path.join(DATA_DIR, "categories.json")
    )

    df = (
        df
        .filter(F.col("category_id").isNotNull())
        .withColumn("name", F.trim(F.col("name")))
        # Explode subcategories into individual rows for easier joining
        .withColumn("subcategory", F.explode("subcategories"))
        .withColumn("subcategory_id",   F.col("subcategory.subcategory_id"))
        .withColumn("subcategory_name", F.trim(F.col("subcategory.name")))
        .withColumn("profit_margin",    F.col("subcategory.profit_margin").cast(DoubleType()))
        # Clamp profit margin to [0, 1]
        .withColumn("profit_margin",
                    F.when(F.col("profit_margin").between(0.0, 1.0),
                           F.col("profit_margin"))
                     .otherwise(F.lit(None).cast(DoubleType())))
        .drop("subcategories", "subcategory")
        .dropDuplicates(["category_id", "subcategory_id"])
    )

    save(df, "categories")
    return df


# ── 3. PRODUCTS ───────────────────────────────────────────────────────────────

def clean_products():
    df = spark.read.option("multiline", True).json(
        os.path.join(DATA_DIR, "products.json")
    )

    df = (
        df
        .filter(F.col("product_id").isNotNull())
        .withColumn("name", F.trim(F.col("name")))
        # Prices must be positive
        .withColumn("base_price",
                    F.when(F.col("base_price") > 0, F.col("base_price"))
                     .otherwise(F.lit(None).cast(DoubleType())))
        .withColumn("current_price",
                    F.when(F.col("current_price") > 0, F.col("current_price"))
                     .otherwise(F.col("base_price")))   # fallback to base
        # Stock must be non-negative
        .withColumn("current_stock",
                    F.when(F.col("current_stock") >= 0, F.col("current_stock"))
                     .otherwise(F.lit(0)))
        # Recompute is_active in case data is inconsistent
        .withColumn("is_active",
                    F.col("current_stock") > 0)
        # Parse creation date
        .withColumn("creation_date",
                    F.to_timestamp("creation_date", "yyyy-MM-dd'T'HH:mm:ss"))
        # Number of price changes recorded
        .withColumn("price_change_count", F.size("price_history"))
        # Price volatility: (max - min) / min  across price history
        .withColumn("max_price",
                    F.aggregate("price_history",
                                F.lit(0.0),
                                lambda acc, x: F.greatest(acc, x["price"])))
        .withColumn("min_price",
                    F.aggregate("price_history",
                                F.lit(9999999.0),
                                lambda acc, x: F.least(acc, x["price"])))
        .withColumn("price_volatility",
                    F.when(F.col("min_price") > 0,
                           (F.col("max_price") - F.col("min_price")) / F.col("min_price"))
                     .otherwise(F.lit(0.0)))
        .drop("max_price", "min_price")
        .dropDuplicates(["product_id"])
    )

    save(df, "products")
    return df


# ── 4. SESSIONS ───────────────────────────────────────────────────────────────

def clean_sessions():
    # Sessions are split across multiple files — read all at once
    session_files = glob.glob(os.path.join(DATA_DIR, "sessions_*.json"))
    if not session_files:
        raise FileNotFoundError("No sessions_*.json files found in " + DATA_DIR)

    df = spark.read.option("multiline", True).json(session_files)

    df = (
        df
        .filter(F.col("session_id").isNotNull())
        .filter(F.col("user_id").isNotNull())
        # Parse timestamps
        .withColumn("start_time",
                    F.to_timestamp("start_time", "yyyy-MM-dd'T'HH:mm:ss"))
        .withColumn("end_time",
                    F.to_timestamp("end_time", "yyyy-MM-dd'T'HH:mm:ss"))
        # Duration must be positive; recompute from timestamps when bad
        .withColumn("duration_seconds",
                    F.when(F.col("duration_seconds") > 0,
                           F.col("duration_seconds"))
                     .otherwise(
                         F.unix_timestamp("end_time") - F.unix_timestamp("start_time")))
        # Flatten device_profile
        .withColumn("device_type",    F.col("device_profile.type"))
        .withColumn("device_os",      F.col("device_profile.os"))
        .withColumn("device_browser", F.col("device_profile.browser"))
        # Flatten geo_data
        .withColumn("session_city",    F.trim(F.col("geo_data.city")))
        .withColumn("session_state",   F.upper(F.col("geo_data.state")))
        .withColumn("session_country", F.upper(F.col("geo_data.country")))
        .withColumn("ip_address",      F.col("geo_data.ip_address"))
        # Derived metrics
        .withColumn("page_view_count", F.size("page_views"))
        .withColumn("viewed_product_count", F.size("viewed_products"))
        # Standardise conversion_status
        .withColumn("conversion_status",
                    F.when(F.col("conversion_status").isin(
                               "converted", "abandoned", "browsing"),
                           F.col("conversion_status"))
                     .otherwise(F.lit("unknown")))
        # Standardise referrer
        .withColumn("referrer",
                    F.lower(F.trim(F.col("referrer"))))
        .drop("device_profile", "geo_data")
        .dropDuplicates(["session_id"])
    )

    save(df, "sessions")
    return df


# ── 5. TRANSACTIONS ───────────────────────────────────────────────────────────

def clean_transactions():
    df = spark.read.option("multiline", True).json(
        os.path.join(DATA_DIR, "transactions.json")
    )

    df = (
        df
        .filter(F.col("transaction_id").isNotNull())
        .filter(F.col("user_id").isNotNull())
        # Parse timestamp
        .withColumn("timestamp",
                    F.to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ss"))
        # Derive date parts for time-based analytics
        .withColumn("txn_date",  F.to_date("timestamp"))
        .withColumn("txn_year",  F.year("timestamp"))
        .withColumn("txn_month", F.month("timestamp"))
        .withColumn("txn_week",  F.weekofyear("timestamp"))
        .withColumn("txn_dow",   F.dayofweek("timestamp"))   # 1=Sun … 7=Sat
        # Validate financials — totals must be non-negative
        .withColumn("subtotal",
                    F.when(F.col("subtotal") >= 0, F.col("subtotal"))
                     .otherwise(F.lit(0.0)))
        .withColumn("discount",
                    F.when(F.col("discount") >= 0, F.col("discount"))
                     .otherwise(F.lit(0.0)))
        .withColumn("total",
                    F.when(F.col("total") >= 0, F.col("total"))
                     .otherwise(F.col("subtotal") - F.col("discount")))
        # Number of line items
        .withColumn("item_count", F.size("items"))
        # Standardise status
        .withColumn("status",
                    F.lower(F.trim(F.col("status"))))
        # Standardise payment method
        .withColumn("payment_method",
                    F.lower(F.trim(F.col("payment_method"))))
        .dropDuplicates(["transaction_id"])
    )

    save(df, "transactions")
    return df


# ── MAIN ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("  Part 2a — Data Cleaning & Normalization")
    print("=" * 60)

    clean_users()
    clean_categories()
    clean_products()
    clean_sessions()
    clean_transactions()

    print("\n" + "=" * 60)
    print("  All datasets cleaned and saved to:", OUTPUT_DIR)
    print("=" * 60)
    spark.stop()
