
import json
import random
import os
import math
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()
random.seed(42)
Faker.seed(42)

# ─── Configuration ────────────────────────────────────────────────────────────
NUM_USERS         = 500
NUM_CATEGORIES    = 20
SUBCATS_PER_CAT   = 4
NUM_PRODUCTS      = 1000
NUM_SESSIONS      = 5000
SESSIONS_PER_FILE = 1000          # sessions split across multiple files
CONVERSION_RATE   = 0.30          # 30% of sessions convert to a transaction
DAYS_RANGE        = 90            # 90-day activity window
START_DATE        = datetime(2025, 1, 1)
END_DATE          = START_DATE + timedelta(days=DAYS_RANGE)

PAYMENT_METHODS   = ["credit_card", "debit_card", "paypal", "bank_transfer", "mobile_money"]
DEVICE_TYPES      = ["mobile", "desktop", "tablet"]
OS_MAP            = {"mobile": ["iOS", "Android"], "desktop": ["Windows", "macOS", "Linux"], "tablet": ["iPadOS", "Android"]}
BROWSER_MAP       = {"iOS": ["Safari", "Chrome"], "Android": ["Chrome", "Firefox"], "Windows": ["Chrome", "Edge", "Firefox"],
                     "macOS": ["Safari", "Chrome"], "Linux": ["Firefox", "Chrome"], "iPadOS": ["Safari", "Chrome"]}
REFERRERS         = ["search_engine", "social_media", "email", "direct", "affiliate", "display_ad"]
PAGE_TYPES        = ["home", "category_listing", "product_detail", "cart", "checkout", "search_results"]
STATUSES_TXN      = ["completed", "shipped", "processing", "cancelled", "refunded"]
STATUSES_CONV     = ["converted", "abandoned", "browsing"]

OUTPUT_DIR = "."   # change if you want files in a subdirectory


# ─── Helpers ──────────────────────────────────────────────────────────────────

def rand_date(start: datetime, end: datetime) -> datetime:
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))


def fmt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


def write_json(filename: str, data) -> None:
    path = os.path.join(OUTPUT_DIR, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"  ✔  Written: {filename}  ({len(data) if isinstance(data, list) else 1} records)")


# ─── 1. USERS ─────────────────────────────────────────────────────────────────

def generate_users(n: int) -> list:
    print("\n[1/5] Generating users …")
    users = []
    for i in range(n):
        reg_date = rand_date(START_DATE - timedelta(days=180), END_DATE - timedelta(days=1))
        last_active = rand_date(reg_date, END_DATE)
        users.append({
            "user_id": f"user_{i:06d}",
            "name": fake.name(),
            "email": fake.unique.email(),
            "age": random.randint(18, 70),
            "gender": random.choice(["M", "F", "Other"]),
            "geo_data": {
                "city": fake.city(),
                "state": fake.state_abbr(),
                "country": "US"
            },
            "registration_date": fmt(reg_date),
            "last_active": fmt(last_active),
            "preferred_payment": random.choice(PAYMENT_METHODS),
            "is_premium": random.random() < 0.15   # 15% premium users
        })
    return users


# ─── 2. CATEGORIES ────────────────────────────────────────────────────────────

CATEGORY_NAMES = [
    "Electronics", "Clothing & Apparel", "Home & Kitchen", "Sports & Outdoors",
    "Books & Media", "Toys & Games", "Health & Beauty", "Automotive",
    "Garden & Tools", "Food & Grocery", "Jewelry & Watches", "Office Supplies",
    "Pet Supplies", "Baby & Kids", "Musical Instruments", "Travel & Luggage",
    "Arts & Crafts", "Software & Gaming", "Industrial & Scientific", "Handmade"
]

def generate_categories(n: int) -> list:
    print("[2/5] Generating categories …")
    categories = []
    for i in range(n):
        name = CATEGORY_NAMES[i] if i < len(CATEGORY_NAMES) else fake.bs().title()
        subcategories = []
        for j in range(SUBCATS_PER_CAT):
            subcategories.append({
                "subcategory_id": f"sub_{i:03d}_{j:02d}",
                "name": fake.bs().title(),
                "profit_margin": round(random.uniform(0.08, 0.45), 2)
            })
        categories.append({
            "category_id": f"cat_{i:03d}",
            "name": name,
            "subcategories": subcategories
        })
    return categories


# ─── 3. PRODUCTS ──────────────────────────────────────────────────────────────

def generate_products(n: int, categories: list) -> list:
    print("[3/5] Generating products …")
    products = []
    for i in range(n):
        cat = random.choice(categories)
        subcat = random.choice(cat["subcategories"])
        creation = rand_date(START_DATE - timedelta(days=120), END_DATE - timedelta(days=5))

        # Build a small price history (1-3 price changes)
        price_history = []
        price = round(random.uniform(5.0, 500.0), 2)
        ph_date = creation
        for _ in range(random.randint(1, 3)):
            price_history.append({"price": price, "date": fmt(ph_date)})
            next_start = ph_date + timedelta(days=1)
            if next_start >= END_DATE:          # no room left — stop early
                break
            ph_date = rand_date(next_start, END_DATE)
            price = round(price * random.uniform(0.85, 1.20), 2)   # ±15% change

        current_stock = random.randint(0, 200)
        products.append({
            "product_id": f"prod_{i:05d}",
            "name": fake.catch_phrase().title(),
            "category_id": cat["category_id"],
            "subcategory_id": subcat["subcategory_id"],
            "base_price": price_history[0]["price"],
            "current_price": price_history[-1]["price"],
            "current_stock": current_stock,
            "is_active": current_stock > 0,
            "price_history": price_history,
            "creation_date": fmt(creation),
            "tags": [fake.word() for _ in range(random.randint(2, 5))]
        })
    return products


# ─── 4. SESSIONS ──────────────────────────────────────────────────────────────

def _build_page_views(start: datetime, products: list, categories: list) -> tuple:
    """Return (page_views list, viewed_product_ids list, cart_contents dict)."""
    page_views = []
    viewed_products = []
    cart_contents = {}
    ts = start

    # Always start with a home or search page
    page_views.append({
        "timestamp": fmt(ts),
        "page_type": random.choice(["home", "search_results"]),
        "product_id": None,
        "category_id": None,
        "view_duration": random.randint(10, 90)
    })
    ts += timedelta(seconds=page_views[-1]["view_duration"] + random.randint(2, 15))

    # 1-3 category pages
    for _ in range(random.randint(1, 3)):
        cat = random.choice(categories)
        page_views.append({
            "timestamp": fmt(ts),
            "page_type": "category_listing",
            "product_id": None,
            "category_id": cat["category_id"],
            "view_duration": random.randint(20, 120)
        })
        ts += timedelta(seconds=page_views[-1]["view_duration"] + random.randint(2, 15))

    # 1-5 product detail pages
    active_products = [p for p in products if p["is_active"]]
    for _ in range(random.randint(1, 5)):
        prod = random.choice(active_products)
        viewed_products.append(prod["product_id"])
        page_views.append({
            "timestamp": fmt(ts),
            "page_type": "product_detail",
            "product_id": prod["product_id"],
            "category_id": prod["category_id"],
            "view_duration": random.randint(30, 300)
        })
        ts += timedelta(seconds=page_views[-1]["view_duration"] + random.randint(2, 20))

        # ~40% chance the viewed product goes to cart
        if random.random() < 0.40:
            qty = random.randint(1, 4)
            if prod["product_id"] in cart_contents:
                cart_contents[prod["product_id"]]["quantity"] += qty
            else:
                cart_contents[prod["product_id"]] = {
                    "quantity": qty,
                    "price": prod["current_price"]
                }

    # Cart page (if anything in cart)
    if cart_contents:
        page_views.append({
            "timestamp": fmt(ts),
            "page_type": "cart",
            "product_id": None,
            "category_id": None,
            "view_duration": random.randint(20, 120)
        })
        ts += timedelta(seconds=page_views[-1]["view_duration"] + random.randint(2, 15))

    return page_views, list(set(viewed_products)), cart_contents


def generate_sessions(n: int, users: list, products: list, categories: list) -> list:
    print("[4/5] Generating sessions …")
    sessions = []
    for i in range(n):
        user = random.choice(users)
        start = rand_date(START_DATE, END_DATE - timedelta(hours=1))
        page_views, viewed_products, cart_contents = _build_page_views(start, products, categories)
        total_duration = sum(pv["view_duration"] for pv in page_views)
        end = start + timedelta(seconds=total_duration)

        # Determine conversion
        if cart_contents and random.random() < CONVERSION_RATE:
            conv_status = "converted"
        elif cart_contents:
            conv_status = "abandoned"
        else:
            conv_status = "browsing"

        device_type = random.choice(DEVICE_TYPES)
        os_name = random.choice(OS_MAP[device_type])
        browser = random.choice(BROWSER_MAP[os_name])

        sessions.append({
            "session_id": f"sess_{fake.hexify(text='^^^^^^^^^^')}",
            "user_id": user["user_id"],
            "start_time": fmt(start),
            "end_time": fmt(end),
            "duration_seconds": total_duration,
            "geo_data": {
                "city": user["geo_data"]["city"],
                "state": user["geo_data"]["state"],
                "country": "US",
                "ip_address": fake.ipv4()
            },
            "device_profile": {
                "type": device_type,
                "os": os_name,
                "browser": browser
            },
            "viewed_products": viewed_products,
            "page_views": page_views,
            "cart_contents": cart_contents,
            "conversion_status": conv_status,
            "referrer": random.choice(REFERRERS)
        })
    return sessions


# ─── 5. TRANSACTIONS ──────────────────────────────────────────────────────────

def generate_transactions(sessions: list) -> list:
    print("[5/5] Generating transactions …")
    transactions = []
    converted = [s for s in sessions if s["conversion_status"] == "converted"]
    for sess in converted:
        items = []
        subtotal = 0.0
        for prod_id, cart_item in sess["cart_contents"].items():
            line_total = round(cart_item["price"] * cart_item["quantity"], 2)
            items.append({
                "product_id": prod_id,
                "quantity": cart_item["quantity"],
                "unit_price": cart_item["price"],
                "subtotal": line_total
            })
            subtotal += line_total

        subtotal = round(subtotal, 2)
        discount = round(subtotal * random.uniform(0, 0.15), 2) if random.random() < 0.3 else 0.0
        total = round(subtotal - discount, 2)

        transactions.append({
            "transaction_id": f"txn_{fake.hexify(text='^^^^^^^^^^^^')}",
            "session_id": sess["session_id"],
            "user_id": sess["user_id"],
            "timestamp": sess["end_time"],
            "items": items,
            "subtotal": subtotal,
            "discount": discount,
            "total": total,
            "payment_method": random.choice(PAYMENT_METHODS),
            "status": random.choice(STATUSES_TXN)
        })
    return transactions


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 55)
    print("  ULK E-Commerce Dataset Generator")
    print("=" * 55)

    users      = generate_users(NUM_USERS)
    categories = generate_categories(NUM_CATEGORIES)
    products   = generate_products(NUM_PRODUCTS, categories)
    sessions   = generate_sessions(NUM_SESSIONS, users, products, categories)
    transactions = generate_transactions(sessions)

    print("\nWriting JSON files …")
    write_json("users.json", users)
    write_json("categories.json", categories)
    write_json("products.json", products)

    # Split sessions across multiple files
    num_files = math.ceil(len(sessions) / SESSIONS_PER_FILE)
    for file_idx in range(num_files):
        chunk = sessions[file_idx * SESSIONS_PER_FILE : (file_idx + 1) * SESSIONS_PER_FILE]
        write_json(f"sessions_{file_idx}.json", chunk)

    write_json("transactions.json", transactions)

    print("\n" + "=" * 55)
    print("  Summary")
    print("=" * 55)
    print(f"  Users        : {len(users):>6,}")
    print(f"  Categories   : {len(categories):>6,}  (each with {SUBCATS_PER_CAT} subcategories)")
    print(f"  Products     : {len(products):>6,}")
    print(f"  Sessions     : {len(sessions):>6,}  → {num_files} file(s)")
    print(f"  Transactions : {len(transactions):>6,}  (~{len(transactions)/len(sessions)*100:.1f}% conversion)")
    print("=" * 55)
    print("\nDone! All files written to:", os.path.abspath(OUTPUT_DIR))


if __name__ == "__main__":
    main()