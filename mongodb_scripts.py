"""
mongodb_scripts.py
ULK Software Engineering Final Exam-Tuyizere Andre-202540326 
– Part 1
MongoDB Schema Design, Data Loading, and Aggregation Pipelines

Usage (with live MongoDB):
    pip install pymongo
    python mongodb_scripts.py

This script can also run in DEMO mode (no MongoDB required) to print
all queries and show what results would look like from sample data.
"""

import json
import os
import random
from datetime import datetime

# ── Try connecting to MongoDB ─────────────────────────────────────────────────
MONGO_URI = "mongodb://localhost:27017/"
DEMO_MODE = False   # Set to False if MongoDB is running

try:
    from pymongo import MongoClient, ASCENDING, DESCENDING
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
    client.server_info()
    DEMO_MODE = False
    print("Connected to MongoDB at", MONGO_URI)
except Exception as e:
    print(f"[DEMO MODE] MongoDB not available ({e}). Printing queries only.")

DATA_DIR = "ecommerce_data"

# ════════════════════════════════════════════════════════════════════════════
# PART 1A – SCHEMA DESIGN (documented as code)
# ════════════════════════════════════════════════════════════════════════════
"""
SCHEMA DESIGN RATIONALE
========================

Collection: products
---------------------
Embeds category name and subcategory directly for read performance.
Avoids cross-collection joins on every product query.

{
  "product_id":      "prod_00123",
  "name":            "Innovative Executive Paradigm",
  "category_id":     "cat_007",
  "category_name":   "Johnson-Williams LLC",      # <-- denormalized
  "subcategory_id":  "sub_007_01",
  "subcategory_name":"Mesh Virtual Deliverables",  # <-- denormalized
  "base_price":      129.99,
  "current_stock":   47,
  "is_active":       true,
  "price_history":   [{"price":149.99,"date":"2024-12-20T00:00:00"}, ...],
  "creation_date":   "2024-12-20T00:00:00"
}

Justification: Product queries almost always need category context.
Embedding avoids expensive $lookup on hot read paths.

Collection: users
-----------------
Embeds summarized purchase statistics so user dashboards load fast.

{
  "user_id":            "user_000042",
  "geo_data":           {"city":"...", "state":"WY", "country":"US"},
  "registration_date":  "2024-12-15T08:42:13",
  "last_active":        "2025-03-12T16:23:47",
  "purchase_summary":   {                          # <-- computed & cached
    "total_orders":  12,
    "total_spent":   1843.20,
    "avg_order":     153.60,
    "last_purchase": "2025-03-10T09:15:00"
  }
}

Justification: Avoids aggregating transactions on every user profile view.
Updated via Spark batch job nightly.

Collection: transactions
------------------------
Embeds line items (one-to-few relationship). References user_id and session_id.

{
  "transaction_id":  "txn_c8d9e7f3a2b1",
  "session_id":      "sess_a7b3c9d8e2",
  "user_id":         "user_000042",
  "timestamp":       "2025-03-12T14:52:41",
  "items":           [{"product_id":"prod_00123","quantity":2,
                       "unit_price":129.99,"subtotal":259.98}],
  "subtotal":        259.98,
  "discount":        25.99,
  "total":           233.99,
  "payment_method":  "credit_card",
  "status":          "completed"
}

Justification: Line items are always read with their parent transaction.
Embedding is safe because a transaction rarely exceeds 10-15 items (<16MB limit).

MongoDB vs HBase: MongoDB chosen for these collections because:
  - Rich querying with nested fields ($elemMatch, $unwind)
  - Aggregation pipelines for business analytics
  - Flexible schema handles varying price_history lengths
  HBase chosen for session time-series because:
  - Billions of page-view events need millisecond point lookups
  - Row key (user_id + timestamp) enables fast user activity scans
  - Wide-column model handles sparse device/geo attributes efficiently
"""

# ════════════════════════════════════════════════════════════════════════════
# PART 1B – DATA LOADING
# ════════════════════════════════════════════════════════════════════════════

def load_json(filename):
    path = os.path.join(DATA_DIR, filename)
    with open(path) as f:
        return json.load(f)

def enrich_products(products, categories):
    """Denormalize category/subcategory names into each product document."""
    cat_map = {c["category_id"]: c for c in categories}
    enriched = []
    for p in products:
        cat = cat_map.get(p["category_id"], {})
        sub_map = {s["subcategory_id"]: s for s in cat.get("subcategories",[])}
        sub = sub_map.get(p.get("subcategory_id",""), {})
        p["category_name"]    = cat.get("name","Unknown")
        p["subcategory_name"] = sub.get("name","Unknown")
        enriched.append(p)
    return enriched

def load_data_into_mongo(db):
    """Load all collections into MongoDB with indexes."""
    print("\n── Loading data ──────────────────────────────")

    # Users
    users = load_json("users.json")
    db.users.drop()
    db.users.insert_many(users)
    db.users.create_index([("user_id", ASCENDING)], unique=True)
    print(f"  Inserted {len(users)} users")

    # Categories
    cats = load_json("categories.json")
    db.categories.drop()
    db.categories.insert_many(cats)
    print(f"  Inserted {len(cats)} categories")

    # Products (enriched)
    products = enrich_products(load_json("products.json"), cats)
    db.products.drop()
    db.products.insert_many(products)
    db.products.create_index([("product_id", ASCENDING)], unique=True)
    db.products.create_index([("category_id", ASCENDING)])
    db.products.create_index([("is_active", ASCENDING)])
    print(f"  Inserted {len(products)} products")

    # Transactions
    txns = load_json("transactions.json")
    db.transactions.drop()
    db.transactions.insert_many(txns)
    db.transactions.create_index([("user_id", ASCENDING)])
    db.transactions.create_index([("timestamp", DESCENDING)])
    db.transactions.create_index([("status", ASCENDING)])
    print(f"  Inserted {len(txns)} transactions")

    print("  Indexes created on users, products, transactions.")
    return products, txns

# ════════════════════════════════════════════════════════════════════════════
# PART 1C – AGGREGATION PIPELINES
# ════════════════════════════════════════════════════════════════════════════

# ── Pipeline 1: Top 10 Products by Revenue ───────────────────────────────────
TOP_REVENUE_PIPELINE = [
    {"$match": {"status": "completed"}},
    {"$unwind": "$items"},
    {"$group": {
        "_id": "$items.product_id",
        "total_revenue": {"$sum": "$items.subtotal"},
        "total_units":   {"$sum": "$items.quantity"},
        "order_count":   {"$sum": 1}
    }},
    {"$sort": {"total_revenue": -1}},
    {"$limit": 10},
    {"$lookup": {
        "from": "products",
        "localField": "_id",
        "foreignField": "product_id",
        "as": "product_info"
    }},
    {"$unwind": {"path": "$product_info", "preserveNullAndEmptyArrays": True}},
    {"$project": {
        "product_id":    "$_id",
        "product_name":  "$product_info.name",
        "category":      "$product_info.category_name",
        "total_revenue": {"$round": ["$total_revenue", 2]},
        "total_units":   1,
        "order_count":   1,
        "_id": 0
    }}
]

# ── Pipeline 2: Monthly Revenue Trend ────────────────────────────────────────
MONTHLY_REVENUE_PIPELINE = [
    {"$match": {"status": {"$in": ["completed", "shipped"]}}},
    {"$addFields": {
        "month": {"$month":  {"$dateFromString": {"dateString": "$timestamp"}}},
        "year":  {"$year":   {"$dateFromString": {"dateString": "$timestamp"}}}
    }},
    {"$group": {
        "_id":           {"year": "$year", "month": "$month"},
        "total_revenue": {"$sum": "$total"},
        "order_count":   {"$sum": 1},
        "avg_order":     {"$avg": "$total"},
        "total_discount":{"$sum": "$discount"}
    }},
    {"$sort": {"_id.year": 1, "_id.month": 1}},
    {"$project": {
        "period":         {"$concat": [
            {"$toString": "$_id.year"}, "-",
            {"$cond": [{"$lt":["$_id.month",10]}, "0", ""]},
            {"$toString": "$_id.month"}
        ]},
        "total_revenue":  {"$round": ["$total_revenue", 2]},
        "order_count":    1,
        "avg_order_value":{"$round": ["$avg_order", 2]},
        "total_discount": {"$round": ["$total_discount", 2]},
        "_id": 0
    }}
]

# ── Pipeline 3: Revenue by Category ──────────────────────────────────────────
CATEGORY_REVENUE_PIPELINE = [
    {"$match": {"status": {"$in": ["completed","shipped"]}}},
    {"$unwind": "$items"},
    {"$lookup": {
        "from": "products",
        "localField": "items.product_id",
        "foreignField": "product_id",
        "as": "prod"
    }},
    {"$unwind": "$prod"},
    {"$group": {
        "_id":           "$prod.category_name",
        "revenue":       {"$sum": "$items.subtotal"},
        "units_sold":    {"$sum": "$items.quantity"},
        "num_products":  {"$addToSet": "$items.product_id"}
    }},
    {"$project": {
        "category":      "$_id",
        "revenue":       {"$round": ["$revenue", 2]},
        "units_sold":    1,
        "unique_products":{"$size": "$num_products"},
        "_id": 0
    }},
    {"$sort": {"revenue": -1}}
]

# ── Pipeline 4: User Purchase Frequency Segmentation ─────────────────────────
USER_SEGMENTATION_PIPELINE = [
    {"$match": {"status": {"$in": ["completed","shipped"]}}},
    {"$group": {
        "_id":        "$user_id",
        "order_count":{"$sum": 1},
        "total_spent":{"$sum": "$total"},
        "avg_order":  {"$avg": "$total"}
    }},
    {"$addFields": {
        "segment": {"$switch": {"branches": [
            {"case": {"$gte": ["$order_count", 5]}, "then": "High-Frequency"},
            {"case": {"$gte": ["$order_count", 2]}, "then": "Regular"},
            {"case": {"$eq": ["$order_count", 1]}, "then": "One-Time"}
        ], "default": "Inactive"}}
    }},
    {"$group": {
        "_id":             "$segment",
        "user_count":      {"$sum": 1},
        "avg_spend":       {"$avg": "$total_spent"},
        "avg_order_count": {"$avg": "$order_count"}
    }},
    {"$project": {
        "segment":         "$_id",
        "user_count":      1,
        "avg_lifetime_spend":{"$round":["$avg_spend",2]},
        "avg_orders":      {"$round":["$avg_order_count",1]},
        "_id": 0
    }},
    {"$sort": {"user_count": -1}}
]

# ── Payment Method Distribution ───────────────────────────────────────────────
PAYMENT_METHOD_PIPELINE = [
    {"$group": {
        "_id":       "$payment_method",
        "count":     {"$sum": 1},
        "revenue":   {"$sum": "$total"}
    }},
    {"$sort": {"count": -1}},
    {"$project": {
        "payment_method": "$_id",
        "transaction_count": "$count",
        "total_revenue": {"$round":["$revenue",2]},
        "_id": 0
    }}
]

def run_pipeline(db, collection, pipeline, label):
    print(f"\n── {label} ──────────────────────────")
    results = list(db[collection].aggregate(pipeline))
    for r in results[:5]:
        print(" ", r)
    if len(results) > 5:
        print(f"  ... ({len(results)} total results)")
    return results

def demo_simulate_results():
    """Print realistic simulated results when MongoDB is not available."""
    print("\n══ AGGREGATION PIPELINE RESULTS (SIMULATED) ══\n")

    print("Pipeline 1 – Top Products by Revenue:")
    for i, (pid, name, cat, rev, units) in enumerate([
        ("prod_00042","Innovative Strategic Platform","Johnson-Williams LLC",  4823.50, 37),
        ("prod_00187","Advanced Modular Framework",  "TechGroup Inc",         3912.00, 28),
        ("prod_00301","Dynamic Enterprise Solution",  "DataCorp Partners",     3541.75, 19),
        ("prod_00099","Scalable Virtual Interface",   "SynergyAssociates LLC", 3120.80, 41),
        ("prod_00412","Agile Paradigm Platform",      "Johnson-Williams LLC",  2988.60, 22),
    ], 1):
        print(f"  #{i} {pid} | {name[:30]:<30} | {cat[:25]:<25} | ${rev:>8,.2f} | {units} units")

    print("\nPipeline 2 – Monthly Revenue Trend:")
    months = ["2025-01","2025-02","2025-03"]
    revenues = [28_412.50, 34_987.30, 41_203.80]
    orders   = [198, 243, 287]
    for m, r, o in zip(months, revenues, orders):
        print(f"  {m}: ${r:>10,.2f}  |  {o} orders  |  avg ${r/o:,.2f}/order")

    print("\nPipeline 3 – Revenue by Category:")
    cats = [("Johnson-Williams LLC",41_230, 312),("TechGroup Inc",32_189,241),
            ("DataCorp Partners",28_940,198),("SynergyAssociates LLC",22_100,187)]
    for cat, rev, units in cats:
        print(f"  {cat:<30}: ${rev:>10,}  |  {units} units")

    print("\nPipeline 4 – User Segmentation:")
    segs = [("High-Frequency",42,1243.60,7.2),("Regular",189,421.30,2.8),
            ("One-Time",267,187.40,1.0)]
    for seg, count, avg_spend, avg_orders in segs:
        print(f"  {seg:<16}: {count:>4} users | avg spend ${avg_spend:>8,.2f} | avg {avg_orders} orders")

    print("\nPipeline 5 – Payment Method Distribution:")
    pmts = [("credit_card",312,54_123),("paypal",189,28_912),
            ("debit_card",112,16_400),("apple_pay",65,10_210)]
    for pm, cnt, rev in pmts:
        print(f"  {pm:<15}: {cnt:>4} txns | ${rev:>10,} revenue")

# ════════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════════
def main():
    print("=" * 60)
    print("ULK Final Project – MongoDB Scripts")
    print("=" * 60)

    if DEMO_MODE:
        print("\n[DEMO] Printing schema documentation and pipeline definitions.")
        print("\n── Schema Documentation ──────────────────────────────────")
        print("  Collections: users, categories, products, transactions")
        print("  See schema design comments at top of this file for full")
        print("  justification of embedding vs referencing decisions.")

        print("\n── Index Strategy ────────────────────────────────────────")
        print("  products:     {product_id:1} unique, {category_id:1}, {is_active:1}")
        print("  users:        {user_id:1} unique")
        print("  transactions: {user_id:1}, {timestamp:-1}, {status:1}")

        print("\n── Aggregation Pipelines Defined ─────────────────────────")
        pipelines = {
            "TOP_REVENUE_PIPELINE":       "Top 10 products by revenue (with $lookup)",
            "MONTHLY_REVENUE_PIPELINE":   "Monthly revenue trend & avg order value",
            "CATEGORY_REVENUE_PIPELINE":  "Revenue breakdown by product category",
            "USER_SEGMENTATION_PIPELINE": "Users segmented by purchase frequency",
            "PAYMENT_METHOD_PIPELINE":    "Transaction & revenue by payment method",
        }
        for name, desc in pipelines.items():
            print(f"  {name}: {desc}")

        demo_simulate_results()
        return

    # Live MongoDB path
    db = client["ecommerce_analytics"]
    products, txns = load_data_into_mongo(db)

    run_pipeline(db, "transactions", TOP_REVENUE_PIPELINE,       "Top Products by Revenue")
    run_pipeline(db, "transactions", MONTHLY_REVENUE_PIPELINE,   "Monthly Revenue Trend")
    run_pipeline(db, "transactions", CATEGORY_REVENUE_PIPELINE,  "Revenue by Category")
    run_pipeline(db, "transactions", USER_SEGMENTATION_PIPELINE, "User Segmentation")
    run_pipeline(db, "transactions", PAYMENT_METHOD_PIPELINE,    "Payment Method Distribution")

if __name__ == "__main__":
    main()
