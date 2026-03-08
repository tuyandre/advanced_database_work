
import os
import csv
import json
import sys
from datetime import datetime

# ── Configuration ─────────────────────────────────────────────────────────────
MONGO_URI   = "mongodb://localhost:27017/"
DB_NAME     = "ecommerce_analytics"
OUTPUT_DIR  = "powerbi_exports"
DATA_DIR    = "ecommerce_data"          # fallback if MongoDB is offline

MONTH_NAMES = {
    1: "January",  2: "February", 3: "March",    4: "April",
    5: "May",      6: "June",     7: "July",      8: "August",
    9: "September",10: "October", 11: "November", 12: "December"
}

# ── Try connecting to MongoDB ──────────────────────────────────────────────────
USE_MONGO = False
try:
    from pymongo import MongoClient
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
    client.server_info()
    db = client[DB_NAME]
    USE_MONGO = True
    print(f"✅ Connected to MongoDB at {MONGO_URI}  (database: {DB_NAME})")
except Exception as e:
    print(f"⚠️  MongoDB not available ({e})")
    print(f"   Falling back to JSON files in '{DATA_DIR}/'")

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── Data loaders ───────────────────────────────────────────────────────────────
def load(collection_name, json_filenames=None):
    """Load data from MongoDB or JSON fallback."""
    if USE_MONGO:
        return list(db[collection_name].find({}, {"_id": 0}))
    if json_filenames is None:
        json_filenames = [f"{collection_name}.json"]
    records = []
    for fname in json_filenames:
        path = os.path.join(DATA_DIR, fname)
        if os.path.exists(path):
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
                records.extend(data if isinstance(data, list) else [data])
        else:
            print(f"   [WARN] File not found: {path}")
    return records

def parse_date(ts):
    """Parse ISO timestamp, return (datetime_obj, date_str, year, month, month_name)."""
    try:
        dt = datetime.fromisoformat(str(ts))
        return dt, dt.strftime("%Y-%m-%d"), dt.year, dt.month, MONTH_NAMES[dt.month]
    except Exception:
        return None, str(ts)[:10], "", "", ""

def write_csv(filename, headers, rows):
    """Write rows to CSV and print a summary line."""
    path = os.path.join(OUTPUT_DIR, filename)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)
    size_kb = os.path.getsize(path) / 1024
    print(f"   ✓  {filename:<32}  {len(rows):>6} rows   {size_kb:>7.1f} KB")
    return len(rows)

# ══════════════════════════════════════════════════════════════════════════════
print("\n── Loading data ──────────────────────────────────────────────────────────")

users        = load("users")
products     = load("products")
transactions = load("transactions")
categories   = load("categories")
sessions     = load("sessions",
                    ["sessions_0.json", "sessions_1.json",
                     "sessions_2.json", "sessions_3.json"])

print(f"   users={len(users)}  products={len(products)}  "
      f"transactions={len(transactions)}  sessions={len(sessions)}  "
      f"categories={len(categories)}")

# ── Build lookup dictionaries ──────────────────────────────────────────────────
cat_map    = {c["category_id"]: c["name"] for c in categories}
subcat_map = {}
for c in categories:
    for s in c.get("subcategories", []):
        subcat_map[s["subcategory_id"]] = s["name"]
prod_map   = {p["product_id"]: p["name"] for p in products}

# ══════════════════════════════════════════════════════════════════════════════
print("\n── Exporting CSV files ───────────────────────────────────────────────────")

# ── 1. users.csv ──────────────────────────────────────────────────────────────
rows = []
for u in users:
    geo = u.get("geo_data", {})
    ps  = u.get("purchase_summary", {})
    rows.append([
        u.get("user_id", ""),
        geo.get("city", ""),
        geo.get("state", ""),
        geo.get("country", "RW"),
        u.get("registration_date", ""),
        u.get("last_active", ""),
        ps.get("total_orders", 0),
        ps.get("total_spent", 0),
        ps.get("avg_order", 0),
    ])
write_csv("users.csv",
    ["user_id", "city", "province", "country",
     "registration_date", "last_active",
     "total_orders", "total_spent_rwf", "avg_order_rwf"],
    rows)

# ── 2. products.csv ───────────────────────────────────────────────────────────
rows = []
for p in products:
    rows.append([
        p.get("product_id", ""),
        p.get("name", ""),
        p.get("category_id", ""),
        cat_map.get(p.get("category_id", ""), ""),
        p.get("subcategory_id", ""),
        subcat_map.get(p.get("subcategory_id", ""), ""),
        p.get("base_price", 0),
        p.get("current_stock", 0),
        p.get("is_active", True),
        p.get("creation_date", ""),
    ])
write_csv("products.csv",
    ["product_id", "name", "category_id", "category_name",
     "subcategory_id", "subcategory_name",
     "base_price_rwf", "current_stock", "is_active", "creation_date"],
    rows)

# ── 3. transactions.csv ───────────────────────────────────────────────────────
rows = []
for t in transactions:
    _, date, year, month, month_name = parse_date(t.get("timestamp", ""))
    rows.append([
        t.get("transaction_id", ""),
        t.get("user_id", ""),
        t.get("session_id", ""),
        t.get("timestamp", ""),
        date,
        year,
        month,
        month_name,
        t.get("subtotal", 0),
        t.get("discount", 0),
        t.get("total", 0),
        t.get("payment_method", ""),
        t.get("status", ""),
        len(t.get("items", [])),
    ])
write_csv("transactions.csv",
    ["transaction_id", "user_id", "session_id", "timestamp",
     "date", "year", "month", "month_name",
     "subtotal_rwf", "discount_rwf", "total_rwf",
     "payment_method", "status", "item_count"],
    rows)

# ── 4. transaction_items.csv ──────────────────────────────────────────────────
# One row per line item — needed for product-level revenue analysis
rows = []
for t in transactions:
    _, date, year, month, month_name = parse_date(t.get("timestamp", ""))
    for item in t.get("items", []):
        pid = item.get("product_id", "")
        rows.append([
            t.get("transaction_id", ""),
            t.get("user_id", ""),
            t.get("timestamp", ""),
            date,
            month,
            month_name,
            t.get("status", ""),
            t.get("payment_method", ""),
            pid,
            prod_map.get(pid, pid),
            item.get("quantity", 0),
            item.get("unit_price", 0),
            item.get("subtotal", 0),
        ])
write_csv("transaction_items.csv",
    ["transaction_id", "user_id", "timestamp", "date",
     "month", "month_name", "status", "payment_method",
     "product_id", "product_name",
     "quantity", "unit_price_rwf", "item_subtotal_rwf"],
    rows)

# ── 5. sessions.csv ───────────────────────────────────────────────────────────
rows = []
for s in sessions:
    geo = s.get("geo_data", {})
    dev = s.get("device_profile", {})
    dur = s.get("duration_seconds", 0)
    cart = s.get("cart_contents", [])
    cart_count = len(cart) if isinstance(cart, list) else 0
    rows.append([
        s.get("session_id", ""),
        s.get("user_id", ""),
        s.get("start_time", ""),
        s.get("end_time", ""),
        dur,
        round(dur / 60, 2),
        geo.get("city", ""),
        geo.get("state", ""),
        dev.get("type", ""),
        dev.get("os", ""),
        dev.get("browser", ""),
        s.get("conversion_status", ""),
        s.get("referrer", ""),
        len(s.get("page_views", [])),
        cart_count,
    ])
write_csv("sessions.csv",
    ["session_id", "user_id", "start_time", "end_time",
     "duration_seconds", "duration_minutes",
     "city", "province", "device_type", "os", "browser",
     "conversion_status", "referrer",
     "page_view_count", "cart_item_count"],
    rows)

# ── 6. categories.csv ─────────────────────────────────────────────────────────
rows = []
for c in categories:
    for s in c.get("subcategories", []):
        rows.append([
            c.get("category_id", ""),
            c.get("name", ""),
            s.get("subcategory_id", ""),
            s.get("name", ""),
            s.get("profit_margin", 0),
        ])
write_csv("categories.csv",
    ["category_id", "category_name",
     "subcategory_id", "subcategory_name", "profit_margin"],
    rows)

# ══════════════════════════════════════════════════════════════════════════════
print(f"""
── Done! ─────────────────────────────────────────────────────────────────────
  Output folder : {os.path.abspath(OUTPUT_DIR)}/

── Power BI — next steps ─────────────────────────────────────────────────────
  1. Open Power BI Desktop
  2. Get Data → Text/CSV → load all 6 files from '{OUTPUT_DIR}/'
  3. Go to Model view and connect:

       users.user_id            ──▶  transactions.user_id        (1:Many)
       users.user_id            ──▶  sessions.user_id             (1:Many)
       transactions.transaction_id ──▶ transaction_items.transaction_id (1:Many)
       products.product_id      ──▶  transaction_items.product_id (1:Many)
       categories.category_id   ──▶  products.category_id         (1:Many)

  4. Set data types in Transform Data:
       transactions  → total_rwf, subtotal_rwf, discount_rwf  : Decimal
       transactions  → date                                    : Date
       sessions      → duration_minutes                        : Decimal
       products      → base_price_rwf                          : Decimal

  5. Add DAX measures:
       Total Revenue RWF  = "RWF " & FORMAT(SUM(transactions[total_rwf]), "#,##0")
       Conversion Rate %  = DIVIDE(COUNTROWS(FILTER(sessions,
                              sessions[conversion_status]="converted")),
                              COUNTROWS(sessions), 0) * 100
       Avg Order RWF      = AVERAGE(transactions[total_rwf])
──────────────────────────────────────────────────────────────────────────────
""")
