# 🇷🇼 Rwanda E-Commerce Analytics Platform

> **ULK Software Engineering — Final Project**  
> Advanced Database Design and Implementation in E-Commerce  
> *MongoDB · HBase · Apache Spark · Python*

---

## 📋 Overview

A full-stack analytics system built on three complementary database technologies, modelled on Rwanda's real digital market. Products include Rwanda Arabica Coffee, Agaseke Woven Baskets, and Cimerwa Cement; payment methods are MTN Mobile Money and Airtel Money; users are distributed across all five Rwandan provinces; and session data reflects Rwanda's 80%+ mobile-first internet usage.

| Metric | Value |
|---|---|
| 👥 Users | 500 (Kigali, Northern, Southern, Eastern, Western) |
| 📦 Products | 500 across 20 Rwandan market categories |
| 📱 Sessions | 2,000 (90 days, Android-dominant) |
| 💳 Transactions | 678 (MTN MoMo + Airtel Money) |
| 💰 Revenue | RWF 15M+ (Jan–Mar 2025) |
| 📊 Charts | 7 analytical visualizations |

---

## 🗂️ Repository Structure

```
rwanda-ecommerce-analytics/
│
├── dataset_generator.py      # Synthetic Rwanda dataset generator
├── mongodb_scripts.py        # MongoDB schema, loading & 5 aggregation pipelines
├── hbase_scripts.py          # HBase schema, table creation & session loading
├── spark_processing.py       # PySpark batch analytics, CLV, cohort analysis
├── visualizations.py         # 7 Matplotlib charts (RWF currency)
│
├── ecommerce_data/           # Generated JSON dataset (auto-created)
│   ├── users.json            # 500 users with Rwanda provinces
│   ├── categories.json       # 20 Rwanda market categories
│   ├── products.json         # 500 Rwanda products
│   ├── sessions_0..3.json    # 2,000 sessions (4 files × 500)
│   └── transactions.json     # 678 transactions
│
├── charts/                   # Generated PNG charts (auto-created)
│   ├── 01_monthly_revenue.png
│   ├── 02_category_revenue.png
│   ├── 03_user_segmentation.png
│   ├── 04_conversion_funnel.png
│   ├── 05_device_referrer.png
│   ├── 06_clv_distribution.png
│   └── 07_top_products.png
│
└── README.md
```

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                   dataset_generator.py                       │
│         (Rwanda provinces, RW products, MoMo payments)       │
└──────────┬──────────────┬──────────────────┬────────────────┘
           │              │                  │
           ▼              ▼                  ▼
    ┌──────────┐   ┌──────────────┐   ┌─────────────────┐
    │ MongoDB  │   │    HBase     │   │  Apache Spark   │
    │          │   │  (Docker)    │   │   (PySpark)     │
    │ products │   │user_sessions │   │ co-purchase     │
    │ users    │   │product_perf  │   │ CLV estimation  │
    │ txns     │   │              │   │ cohort analysis │
    └──────────┘   └──────────────┘   └────────┬────────┘
                                               │
                                               ▼
                                      ┌─────────────────┐
                                      │ visualizations  │
                                      │ (7 RWF charts)  │
                                      └─────────────────┘
```

| Layer | Technology | Purpose | Rwanda Data |
|---|---|---|---|
| Storage | MongoDB 7.0 | Document store — rich queries | Products, users by province, MoMo transactions |
| Storage | HBase 2.1 | Wide-column — time-series | Mobile session streams |
| Processing | Apache Spark 3.5 | Distributed batch analytics | CLV, cohort, recommendations |
| Ingestion | Python 3.11 | ETL & dataset generation | Rwanda-specific generator |

---

## ⚡ Quick Start

### 1. Clone and install dependencies

```bash
git clone https://github.com/your-username/rwanda-ecommerce-analytics.git
cd rwanda-ecommerce-analytics

pip install faker pandas pymongo happybase pyspark matplotlib
```

### 2. Generate the dataset

```bash
python dataset_generator.py
```

Expected output:
```
Generating users...
Generating products...
Generating sessions...
Generating transactions...
Done! Dataset written to 'ecommerce_data/'
  users:        500
  categories:   20
  products:     500
  sessions:     2000 across 4 files
  transactions: 678
```

### 3. Start MongoDB

```bash
# Linux
sudo systemctl start mongod

# macOS
brew services start mongodb-community@7.0

# Windows (PowerShell)
Start-Service -Name MongoDB
```

### 4. Start HBase (Docker)

```bash
docker pull harisekhon/hbase:latest

docker run -d --name hbase \
  -p 2181:2181 -p 9090:9090 \
  -p 16000:16000 -p 16010:16010 \
  harisekhon/hbase:latest
```

### 5. Run all scripts

```bash
python mongodb_scripts.py      # Load into MongoDB + run aggregations
python hbase_scripts.py        # Load sessions into HBase
python spark_processing.py     # Spark batch analytics
python visualizations.py       # Generate all 7 charts
```

---

## 🗄️ Database Schemas

### MongoDB — `products` collection

```json
{
  "product_id":       "prod_00042",
  "name":             "Rwanda Arabica Coffee",
  "category_name":    "Coffee & Tea",
  "subcategory_name": "Arabica Coffee",
  "base_price":       12500,
  "current_stock":    143,
  "is_active":        true,
  "price_history":    [{"price": 14000, "date": "2025-01-05"}]
}
```

### MongoDB — `transactions` collection

```json
{
  "transaction_id": "txn_c8d9e7f3",
  "user_id":        "user_000042",
  "items":          [{"product_id": "prod_00042", "quantity": 2, "subtotal": 25000}],
  "total":          22500,
  "payment_method": "mtn_momo",
  "status":         "completed"
}
```

### HBase — `user_sessions` table

| Component | Value |
|---|---|
| Row key | `user_id # reverse_timestamp` |
| Column family `sess` | session_id, start_time, duration_seconds, conversion_status, referrer |
| Column family `device` | type (mobile/desktop), os (Android dominant), browser |
| Column family `stats` | page_view_count, cart_size |

### HBase — `product_performance` table

| Component | Value |
|---|---|
| Row key | `product_id # date` |
| Column family `views` | view_count, unique_users, avg_duration |
| Column family `sales` | units_sold, revenue (RWF), conversion_rate |

---

## 📊 Visualizations

| Chart | Key Finding |
|---|---|
| Monthly Revenue Trend | Feb peak at RWF 5,618,678 — post-New Year spending surge |
| Revenue by Category | Fresh Produce leads (RWF 1.15M) — Rwanda's agri-economy |
| Customer Segmentation | 51.2% inactive users — WhatsApp re-engagement opportunity |
| Conversion Funnel | Two 50% drops — MTN MoMo checkout friction identified |
| Device & Referrer | Instagram best conversion (20.7%) — invest in Instagram Shopping |
| CLV Distribution | Mean CLV RWF 83,293 — right-skewed, 20% drive 80% of value |
| Top 12 Products | Agaseke Woven Basket leads (RWF 600K+) — premium Rwanda craft |

---

## 🔥 Key Analytics

### Customer Lifetime Value (CLV)

```python
CLV = avg_monthly_revenue × 6_months × retention_factor
retention_factor = 0.60 + 0.30 × engagement_score
engagement_score = min(1.0, conversion_rate×2 + avg_duration/3600)
```

244 users estimated — mean CLV **RWF 83,293**.

### Conversion Funnel

```
All Sessions    2,000  (100.0%)
  └─ Added to Cart   1,374   (68.7%)   ▼ 31% drop
       └─ Converted       689   (34.4%)   ▼ 50% drop  ← MTN MoMo friction
            └─ Completed      347   (17.3%)   ▼ 50% drop
```

### Co-Purchase Recommendations

Spark identifies product pairs bought together by the same user — e.g. *"Customers who bought Rwanda Arabica Coffee also bought Agaseke Woven Basket"* — using distributed co-occurrence matrix computation.

---

## 🛠️ Requirements

| Dependency | Version | Install |
|---|---|---|
| Python | 3.9+ | python.org |
| MongoDB Community | 7.0+ | mongodb.com |
| Docker Desktop | 24+ | docker.com |
| pymongo | 4.x | `pip install pymongo` |
| happybase | 1.2.x | `pip install happybase` |
| pyspark | 3.5.x | `pip install pyspark` |
| matplotlib | 3.x | `pip install matplotlib` |
| faker | any | `pip install faker` |

> **Windows users:** See [Windows Setup Guide](Database_Setup_Guide_Windows.docx) for step-by-step PowerShell instructions including winutils.exe setup for Spark.

---

## 🧩 Demo Mode

All scripts run in **demo mode** without any database installed — they print queries, schemas, and simulated results so the design can be evaluated without infrastructure.

```bash
# Runs in demo mode automatically if MongoDB is not running
python mongodb_scripts.py

# Force demo mode by setting at top of script:
DEMO_MODE = True
```

---

## 🌍 Rwanda-Specific Features

| Feature | Implementation |
|---|---|
| **Provinces** | Kigali, Northern, Southern, Eastern, Western as `geo_data.state` |
| **Districts** | 33 real Rwanda districts as `geo_data.city` (Musanze, Huye, Rubavu…) |
| **Payment** | MTN Mobile Money (`mtn_momo`), Airtel Money (`airtel_money`) dominant |
| **Products** | Arabica Coffee, Agaseke baskets, Ikivuguto yoghurt, Cimerwa cement… |
| **Categories** | Coffee & Tea, Fresh Produce, Agriculture & Farming, Textiles & Crafts… |
| **Referrers** | WhatsApp, Facebook, Instagram, SMS campaigns, Google Search |
| **Devices** | Android repeated 3× in generator to reflect Rwanda's mobile dominance |
| **Currency** | All monetary values in Rwandan Francs (RWF) |

---

## 🚀 Scalability Notes

- **MongoDB**: Horizontal sharding on `user_id`. Atlas M10 cluster handles 1M+ users. New payment methods (BK TecHouse, Equity) added without schema migration.
- **HBase**: Region auto-splitting for session growth. At 10M users × 5 sessions/day = 4.5B rows/year. SNAPPY compression saves 60–70% storage in production (disabled in Docker dev).
- **Spark**: Add EMR worker nodes linearly. Partition by province for Kigali vs Northern analytics. Spark Structured Streaming replaces batch jobs for real-time MTN MoMo monitoring.

---

## ⚠️ Known Issues

| Issue | Cause | Fix |
|---|---|---|
| HBase SNAPPY error | Docker image missing native libs | Script uses `NONE` compression — same data, no compression |
| Spark `UnsatisfiedLinkError` on Windows | Missing `winutils.exe` | Download from [cdarlint/winutils](https://github.com/cdarlint/winutils) → `C:\hadoop\bin\` |
| `mongosh` not recognized | MongoDB bin not in PATH | Add `C:\Program Files\MongoDB\Server\7.0\bin` to Windows PATH |

---

## 📄 License

This project was created as an academic final project for ULK Software Engineering. For educational use.

---

## 👤 Author

**Tuyandre**  
ULK School of Engineering  
Masters Program — Advanced Database Design & Implementation  
March 2026
