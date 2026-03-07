"""
dataset_generator.py
ULK Software Engineering Final Exam-Tuyizere Andre-202540326
Generates synthetic e-commerce dataset: users, categories, products, sessions, transactions
Usage: python dataset_generator.py
Requires: pip install faker pandas
"""

import json
import random
import uuid
import os
from datetime import datetime, timedelta

try:
    from faker import Faker
    fake = Faker()
    USE_FAKER = True
except ImportError:
    USE_FAKER = False
    print("[WARN] Faker not installed – using built-in random data generation.")

# ── Configuration ────────────────────────────────────────────────────────────
SEED = 42
random.seed(SEED)

NUM_USERS        = 500
NUM_CATEGORIES   = 20
NUM_PRODUCTS     = 500
NUM_SESSIONS     = 2000
SESSION_FILES    = 4          # sessions split across N files
NUM_TRANSACTIONS = 800
START_DATE       = datetime(2025, 1, 1)
END_DATE         = datetime(2025, 3, 31)
OUTPUT_DIR       = "ecommerce_data"

STATES = ["Kigali", "Northern", "Southern", "Eastern", "Western"]
CITIES = [
    # Kigali
    "Nyarugenge", "Gasabo", "Kicukiro",
    # Northern
    "Musanze", "Burera", "Gakenke", "Gicumbi", "Rulindo",
    # Southern
    "Huye", "Gisagara", "Muhanga", "Kamonyi", "Nyanza",
    "Nyaruguru", "Ruhango", "Nyamagabe",
    # Eastern
    "Rwamagana", "Bugesera", "Gatsibo", "Kayonza", "Kirehe",
    "Ngoma", "Nyagatare",
    # Western
    "Rubavu", "Karongi", "Ngororero", "Nyabihu", "Nyamasheke",
    "Rusizi", "Rutsiro"
]
PAYMENT_METHODS = [ "mtn_momo", "airtel_money", "bank_transfer", "cash", "visa_card",  ]
DEVICE_TYPES     = ["mobile","desktop","tablet"]
OS_MAP           = {"mobile":["iOS","Android"], "desktop":["Windows","macOS","Linux"],
                    "tablet":["iPadOS","Android"]}
BROWSERS         = ["Chrome","Safari","Firefox","Edge"]
REFERRERS = [ "whatsapp", "facebook", "google_search", "direct", "instagram", "sms_campaign",   "word_of_mouth", ]
PAGE_TYPES       = ["home","category_listing","product_detail","cart","checkout","search"]
CONV_STATUSES    = ["converted","abandoned","browsing"]
TXN_STATUSES     = ["completed","shipped","processing","refunded"]

# ── Helpers ───────────────────────────────────────────────────────────────────
def rand_date(start=START_DATE, end=END_DATE):
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))

def fmt(dt): return dt.strftime("%Y-%m-%dT%H:%M:%S")

CITY_PROVINCE = {
    "Nyarugenge": "Kigali",  "Gasabo": "Kigali",     "Kicukiro": "Kigali",
    "Musanze": "Northern",   "Burera": "Northern",    "Gakenke": "Northern",
    "Gicumbi": "Northern",   "Rulindo": "Northern",
    "Huye": "Southern",      "Gisagara": "Southern",  "Muhanga": "Southern",
    "Kamonyi": "Southern",   "Nyanza": "Southern",    "Nyaruguru": "Southern",
    "Ruhango": "Southern",   "Nyamagabe": "Southern",
    "Rwamagana": "Eastern",  "Bugesera": "Eastern",   "Gatsibo": "Eastern",
    "Kayonza": "Eastern",    "Kirehe": "Eastern",     "Ngoma": "Eastern",
    "Nyagatare": "Eastern",
    "Rubavu": "Western",     "Karongi": "Western",    "Ngororero": "Western",
    "Nyabihu": "Western",    "Nyamasheke": "Western", "Rusizi": "Western",
    "Rutsiro": "Western"
}
def city_state():
    city = random.choice(CITIES)
    return city, CITY_PROVINCE[city]

# def word():
#     words = ["Innovative","Advanced","Strategic","Dynamic","Synergistic","Ergonomic",
#              "Scalable","Virtual","Executive","Paradigm","Interface","Platform",
#              "Framework","Solution","Enterprise","Concrete","Agile","Modular"]
#     return random.choice(words)

# def product_name():
#     return f"{word()} {word()} {word()}"

# def company_name():
#     suffixes = ["LLC","Inc","Corp","Group","Partners","Associates"]
#     return f"{word()}-{word()} {random.choice(suffixes)}"

# ── Generators ────────────────────────────────────────────────────────────────
def generate_users(n=NUM_USERS):
    users = []
    for i in range(n):
        city, state = city_state()
        reg = rand_date(datetime(2024,10,1), datetime(2025,1,1))
        last = rand_date(reg, END_DATE)
        users.append({
            "user_id": f"user_{i:06d}",
            "geo_data": {"city": city, "state": state, "country": "RW"},
            "registration_date": fmt(reg),
            "last_active": fmt(last)
        })
    return users
PRODUCT_CATEGORIES = [
    ("Food & Beverages",        ["Dairy Products","Grains & Flour","Beverages","Cooking Oil","Snacks & Confectionery"]),
    ("Fresh Produce",           ["Vegetables","Fruits","Herbs & Spices","Mushrooms","Flowers"]),
    ("Coffee & Tea",            ["Arabica Coffee","Green Tea","Herbal Tea","Instant Coffee","Coffee Accessories"]),
    ("Electronics & Tech",      ["Mobile Phones","Solar Products","Home Electronics","Accessories","Network Equipment"]),
    ("Clothing & Fashion",      ["Traditional Wear","Casual Wear","School Uniforms","Shoes & Sandals","Accessories"]),
    ("Textiles & Crafts",       ["Kitenge Fabric","Agaseke Baskets","Imigongo Art","Woven Mats","Jewelry"]),
    ("Beauty & Personal Care",  ["Skin Care","Hair Care","Soap & Hygiene","Perfumes","Baby Care"]),
    ("Construction & Hardware", ["Cement & Building","Iron & Steel","Paint & Coating","Plumbing","Electrical"]),
    ("Home & Kitchen",          ["Cookware","Storage","Bedding","Cleaning","Furniture"]),
    ("Agriculture & Farming",   ["Seeds & Seedlings","Fertilizers","Tools & Equipment","Animal Feed","Pesticides"]),
    ("Education & Office",      ["Stationery","Books","School Bags","Office Supplies","Art Materials"]),
    ("Health & Pharmacy",       ["Medicines OTC","Vitamins","First Aid","Medical Devices","Baby Health"]),
    ("Automotive & Transport",  ["Spare Parts","Lubricants","Tyres","Car Accessories","Bicycle Parts"]),
    ("Energy & Solar",          ["Solar Panels","Batteries","Inverters","Gas & LPG","Lighting"]),
    ("Livestock & Poultry",     ["Cattle Products","Poultry","Fish & Seafood","Goat Products","Beekeeping"]),
    ("Sports & Recreation",     ["Football Equipment","Fitness","Outdoor","Games","Cycling"]),
    ("Baby & Children",         ["Baby Food","Toys","Baby Clothing","School Items","Safety Products"]),
    ("Packaging & Logistics",   ["Boxes & Cartons","Bags & Sacks","Wrapping","Labels","Cold Storage"]),
    ("Hospitality & Catering",  ["Restaurant Equipment","Uniforms","Cleaning Products","Disposables","Decor"]),
    ("Digital Services",        ["Airtime & Data","Mobile Money","Printing","Photography","Software"]),
]

def generate_categories(n=NUM_CATEGORIES):
    categories = []
    cats = PRODUCT_CATEGORIES[:n]
    for i, (cat_name, subcats) in enumerate(cats):
        subs = []
        for j, sub_name in enumerate(subcats):
            subs.append({
                "subcategory_id": f"sub_{i:03d}_{j:02d}",
                "name": sub_name,
                "profit_margin": round(random.uniform(0.10, 0.45), 2)
            })
        categories.append({
            "category_id": f"cat_{i:03d}",
            "name": cat_name,
            "subcategories": subs
        })
    return categories
# Rwanda product market - real categories sold in Kigali and across Rwanda
PRODUCT_WORDS = {
    "food": [
        "Inzoga", "Igitoki", "Isombe", "Uburo", "Ibiharage", "Amasaka",
        "Inyama", "Amata", "Akabanga", "Ubunyobwa", "Ibijumba", "Indamutsa",
        "Urwagwa", "Isukari", "Amavuta", "Ibirembo", "Umutsima", "Ikivuguto"
    ],
    "agri": [
        "Arabica", "Bourbon", "Cavendish", "Gahima", "Inyabutatu",
        "Rugali", "Inkoko", "Inka", "Intama", "Ingurube",
        "Imirire", "Imboga", "Imbuto", "Inanasi", "Icyahozo"
    ],
    "tech": [
        "Smart", "Digital", "Mobile", "Solar", "Wireless",
        "FastNet", "PayGo", "EasyPay", "MoMo", "Neza",
        "Keza", "Sheja", "Inzozi", "Iterambere", "Ireme"
    ],
    "textile": [
        "Imishanana", "Kitenge", "Akagera", "Umushanana", "Inzira",
        "Ubwenge", "Inanga", "Agaseke", "Amasunzu", "Urugori"
    ],
    "beauty": [
        "Ubwiza", "Keza", "Nziza", "Sheja", "Umucyo",
        "Amavuta", "Ubusugire", "Inzozi", "Agahire", "Umutima"
    ],
    "construction": [
        "Ibuye", "Urutare", "Icyuma", "Ibiti", "Inzu",
        "Iterambere", "Amazi", "Uruzi", "Isuku", "Imbaho"
    ]
}

PRODUCT_TYPES = [
    # Food & Beverages
    "Akabanga Chilli Oil", "Inyange Juice", "Inyange Milk", "Urwagwa Traditional Beer",
    "Ikivuguto Yoghurt", "Isombe Cassava Leaves", "Ibirayi Fries", "Umutsima Porridge",
    "Inzoga ya Primus", "Mutzig Beer", "Fanta Citron", "Impala Club Soda",
    # Agriculture & Fresh Produce
    "Rwanda Arabica Coffee", "Rukundo Tea Leaves", "Cavendish Banana Bundle",
    "Fresh Avocado Pack", "Passion Fruit Tray", "Pineapple Crate Nyungwe",
    "Irish Potato 5kg", "Sweet Potato Bag", "Maize Flour 2kg", "Sorghum Flour 1kg",
    # Technology & Electronics
    "MTN MoMo Agent Kit", "Solar Home System 50W", "Smartphone Android 4G",
    "Feature Phone Tecno", "Solar Lantern LifeLight", "WiFi Router ZTE",
    "Power Bank 10000mAh", "Phone Charger USB-C", "Earphones Oraimo",
    "Smart TV 32inch", "Decoder StarTimes", "Radio FM Portable",
    # Clothing & Textiles
    "Imishanana Traditional Dress", "Kitenge Fabric 2m", "Agaseke Woven Basket",
    "Umushanana Set", "School Uniform Set", "Gahunda Work Shirt",
    "Leather Shoes Kigali", "Sandals Handmade", "Akagera Print T-Shirt",
    # Beauty & Personal Care
    "Amavuta Shea Butter", "Coconut Hair Oil Rwanda", "Ubwiza Face Cream",
    "Savon Ivoirien Soap", "Nziza Body Lotion", "Toothpaste Colgate",
    "Vaseline Petroleum Jelly", "Dettol Antiseptic", "Nivea Roll-On",
    # Construction & Hardware
    "Iron Sheet 3m", "Cement Bag 50kg Cimerwa", "Paint Bucket 20L",
    "Steel Rod 12mm", "PVC Pipe 2inch", "Electric Wire 2.5mm",
    "Door Lock Yale", "Window Glass Panel", "Roofing Nails 4inch",
    # Home & Kitchen
    "Sufuria Cooking Pot 5L", "Inkorora Storage Basket", "Plastic Jerry Can 20L",
    "Charcoal Stove Jiko", "Gas Cylinder 15kg", "Mosquito Net Treated",
    "Mattress Foam Single", "Pillow Fiber", "Bed Sheet Cotton",
    # Education & Office
    "Exercise Book 96pages", "Biro Pen Box", "Calculator Casio",
    "School Bag Primary", "Printer Paper A4 Ream", "Stapler Heavy Duty",
]

COMPANY_SUFFIXES = [
    "Rwanda Ltd", "Kigali SARL", "Ubumwe Co.", "Iterambere Group",
    "Inzozi Enterprises", "Neza Trading", "Sheja Holdings",
    "Umurava Solutions", "Ejo Heza Ventures", "Ubwiza Industries"
]

COMPANY_NAMES = [
    "Inyange Industries", "Rwanda Trading Company", "Kigali Business Center",
    "Ubumwe Supermarket", "Nakumatt Rwanda", "Simba Superstore",
    "Sonarwa General Store", "BK TecHouse", "RwandAir Shop",
    "Sulfo Rwanda", "BRALIRWA Distributors", "Minimex Rwanda",
    "Akagera Business Group", "Inkomoko Entrepreneurs", "Ikirezi Natural Products",
    "Gashora Enterprises", "Ruliba Clays", "Cimerwa Hardware",
    "Kigali Cement Ltd", "Mahama Trading Post", "Huye Mountain Coffee",
    "Musanze Fresh Market", "Rubavu Fish Market", "Nyagatare Agro Ltd",
    "Eastern Province Traders", "Southern Highlands Co.", "Northern Growers Ltd",
]

def word():
    category = random.choice(list(PRODUCT_WORDS.keys()))
    return random.choice(PRODUCT_WORDS[category])

def product_name():
    return random.choice(PRODUCT_TYPES)

def company_name():
    if random.random() < 0.6:
        return random.choice(COMPANY_NAMES)
    else:
        return random.choice(COMPANY_NAMES) + " - " + random.choice(COMPANY_SUFFIXES)

def generate_products(categories, n=NUM_PRODUCTS):
    products = []
    for i in range(n):
        cat = random.choice(categories)
        sub = random.choice(cat["subcategories"])
        base_price = round(random.uniform(500, 20000), 2)
        creation = rand_date(datetime(2024,10,1), datetime(2025,1,1))
        stock = random.randint(0, 500)
        # price history: 1-3 price changes
        ph = []
        cur_price = round(base_price * random.uniform(0.8, 1.6), 2)
        ph_date = creation
        for _ in range(random.randint(1,3)):
            ph.append({"price": cur_price, "date": fmt(ph_date)})
            ph_date = rand_date(ph_date, END_DATE)
            cur_price = round(cur_price * random.uniform(0.85, 1.8), 2)
        ph.append({"price": base_price, "date": fmt(ph_date)})
        products.append({
            "product_id": f"prod_{i:05d}",
            "name": product_name(),
            "category_id": cat["category_id"],
            "subcategory_id": sub["subcategory_id"],
            "base_price": base_price,
            "current_stock": stock,
            "is_active": stock > 0,
            "price_history": ph,
            "creation_date": fmt(creation)
        })
    return products

def generate_sessions(users, products, n=NUM_SESSIONS):
    sessions = []
    for i in range(n):
        user = random.choice(users)
        start = rand_date()
        duration = random.randint(60, 3600)
        end = start + timedelta(seconds=duration)
        device_type = random.choice(DEVICE_TYPES)
        os_choice = random.choice(OS_MAP[device_type])
        city, state = city_state()
        # page views
        viewed = random.sample(products, min(random.randint(1,6), len(products)))
        viewed_ids = [p["product_id"] for p in viewed]
        page_views = []
        ts = start
        page_views.append({
            "timestamp": fmt(ts), "page_type": "home",
            "product_id": None, "category_id": None,
            "view_duration": random.randint(10,90)
        })
        for prod in viewed:
            ts += timedelta(seconds=random.randint(30,180))
            page_views.append({
                "timestamp": fmt(ts), "page_type": "category_listing",
                "product_id": None, "category_id": prod["category_id"],
                "view_duration": random.randint(20,120)
            })
            ts += timedelta(seconds=random.randint(20,120))
            page_views.append({
                "timestamp": fmt(ts), "page_type": "product_detail",
                "product_id": prod["product_id"], "category_id": prod["category_id"],
                "view_duration": random.randint(30,300)
            })
        # cart
        conv = random.choice(CONV_STATUSES)
        cart = {}
        if conv in ["converted","abandoned"] and viewed:
            cart_products = random.sample(viewed, random.randint(1, min(3, len(viewed))))
            for cp in cart_products:
                cart[cp["product_id"]] = {
                    "quantity": random.randint(1,4),
                    "price": cp["base_price"]
                }
            ts += timedelta(seconds=random.randint(30,120))
            page_views.append({
                "timestamp": fmt(ts), "page_type": "cart",
                "product_id": None, "category_id": None,
                "view_duration": random.randint(20,90)
            })
        session_id = f"sess_{uuid.uuid4().hex[:10]}"
        sessions.append({
            "session_id": session_id,
            "user_id": user["user_id"],
            "start_time": fmt(start),
            "end_time": fmt(end),
            "duration_seconds": duration,
            "geo_data": {
                "city": city, "state": state, "country": "RW",
                "ip_address": f"{random.randint(10,200)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}"
            },
            "device_profile": {"type": device_type, "os": os_choice,
                               "browser": random.choice(BROWSERS)},
            "viewed_products": viewed_ids,
            "page_views": page_views,
            "cart_contents": cart,
            "conversion_status": conv,
            "referrer": random.choice(REFERRERS)
        })
    return sessions

def generate_transactions(sessions, products_dict, n=NUM_TRANSACTIONS):
    conv_sessions = [s for s in sessions if s["conversion_status"] == "converted"]
    random.shuffle(conv_sessions)
    txns = []
    for s in conv_sessions[:n]:
        if not s["cart_contents"]:
            continue
        items = []
        subtotal = 0.0
        for pid, info in s["cart_contents"].items():
            sub = round(info["quantity"] * info["price"], 2)
            items.append({
                "product_id": pid,
                "quantity": info["quantity"],
                "unit_price": info["price"],
                "subtotal": sub
            })
            subtotal += sub
        subtotal = round(subtotal, 2)
        discount = round(subtotal * random.uniform(0, 0.15), 2) if random.random() < 0.3 else 0.0
        total = round(subtotal - discount, 2)
        txns.append({
            "transaction_id": f"txn_{uuid.uuid4().hex[:12]}",
            "session_id": s["session_id"],
            "user_id": s["user_id"],
            "timestamp": s["end_time"],
            "items": items,
            "subtotal": subtotal,
            "discount": discount,
            "total": total,
            "payment_method": random.choice(PAYMENT_METHODS),
            "status": random.choice(TXN_STATUSES)
        })
    return txns

# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print("Generating users...")
    users = generate_users()
    with open(f"{OUTPUT_DIR}/users.json","w") as f:
        json.dump(users, f, indent=2)

    print("Generating categories...")
    categories = generate_categories()
    with open(f"{OUTPUT_DIR}/categories.json","w") as f:
        json.dump(categories, f, indent=2)

    print("Generating products...")
    products = generate_products(categories)
    with open(f"{OUTPUT_DIR}/products.json","w") as f:
        json.dump(products, f, indent=2)

    print("Generating sessions...")
    sessions = generate_sessions(users, products)
    chunk = len(sessions) // SESSION_FILES
    for i in range(SESSION_FILES):
        chunk_data = sessions[i*chunk:(i+1)*chunk] if i < SESSION_FILES-1 else sessions[i*chunk:]
        with open(f"{OUTPUT_DIR}/sessions_{i}.json","w") as f:
            json.dump(chunk_data, f, indent=2)

    print("Generating transactions...")
    products_dict = {p["product_id"]: p for p in products}
    transactions = generate_transactions(sessions, products_dict)
    with open(f"{OUTPUT_DIR}/transactions.json","w") as f:
        json.dump(transactions, f, indent=2)

    print(f"\nDone! Dataset written to '{OUTPUT_DIR}/'")
    print(f"  users:        {len(users)}")
    print(f"  categories:   {len(categories)}")
    print(f"  products:     {len(products)}")
    print(f"  sessions:     {len(sessions)} across {SESSION_FILES} files")
    print(f"  transactions: {len(transactions)}")

if __name__ == "__main__":
    main()
