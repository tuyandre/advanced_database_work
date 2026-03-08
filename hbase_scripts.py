"""
hbase_scripts.py
ULK Software Engineering Final Exam-Tuyizere Andre-202540326
– Part 2 (HBase)

Demonstrates HBase schema design, table creation commands, and data loading
for time-series user session data and product performance metrics.

When HBase is unavailable this script prints all shell commands and
simulates query results so the design can be evaluated.

Setup (Docker):
    docker run -d --name hbase -p 16000:16000 -p 16010:16010 \
               -p 16020:16020 -p 16030:16030 -p 2181:2181 \
               harisekhon/hbase:latest

Then connect via HappyBase:
    pip install happybase
    python hbase_scripts.py
"""

import json, os, struct, datetime

DEMO_MODE = True
DATA_DIR  = "ecommerce_data"
HBASE_HOST = "localhost"

try:
    import happybase
    conn = happybase.Connection(HBASE_HOST, timeout=15000)
    conn.open()
    DEMO_MODE = False
    print("Connected to HBase at", HBASE_HOST)
except Exception as e:
    print(f"[DEMO MODE] HBase not available ({e}). Printing shell commands & simulating results.")

# ════════════════════════════════════════════════════════════════════════════
# SCHEMA DESIGN
# ════════════════════════════════════════════════════════════════════════════
"""
TABLE 1: user_sessions
======================
Purpose: Store time-series user browsing activity for fast user-activity lookups.


TABLE 2: product_performance
============================

Column Families:
  views: Daily view counters     (incremented by Spark streaming)
  sales: Daily sales metrics     (updated by batch job from transactions)

Columns:
  views:view_count       → "143"
  views:unique_users     → "98"
  views:avg_duration_sec → "127"
  sales:units_sold       → "12"
  sales:revenue          → "1559.88"
  sales:cart_adds        → "27"
  sales:conversion_rate  → "0.084"  (units_sold / view_count)
"""

# ════════════════════════════════════════════════════════════════════════════
# HBASE SHELL COMMANDS
# ════════════════════════════════════════════════════════════════════════════

HBASE_SHELL_COMMANDS = """
# ── Connect to HBase Shell ────────────────────────────────────────────
# docker exec -it hbase hbase shell

# ── Delete table ───────────────────────────────────────────────────────
disable 'user_sessions'
drop    'user_sessions'
"""

# ════════════════════════════════════════════════════════════════════════════
# HAPPYBASE CLIENT CODE
# ════════════════════════════════════════════════════════════════════════════

import time, datetime as dt

MAX_LONG = 9223372036854775807  # Java Long.MAX_VALUE

def reverse_ts(timestamp_str):
    """Convert ISO timestamp to reverse epoch millis for row key."""
    epoch = dt.datetime(1970,1,1)
    try:
        ts = dt.datetime.fromisoformat(timestamp_str)
    except Exception:
        ts = dt.datetime.now()
    millis = int((ts - epoch).total_seconds() * 1000)
    return str(MAX_LONG - millis)

def make_session_row_key(user_id, start_time):
    return f"{user_id}#{reverse_ts(start_time)}"

def make_product_row_key(product_id, date_str):
    return f"{product_id}#{date_str}"

def load_sessions_into_hbase(conn, sessions, batch_size=100):
    """Load session data into HBase user_sessions table."""
    table = conn.table('user_sessions')
    batch = table.batch(batch_size=batch_size)
    count = 0
    for s in sessions:
        rk = make_session_row_key(s["user_id"], s["start_time"])
        data = {
            b'sess:session_id':        s["session_id"].encode(),
            b'sess:start_time':        s["start_time"].encode(),
            b'sess:end_time':          s["end_time"].encode(),
            b'sess:duration_seconds':  str(s["duration_seconds"]).encode(),
            b'sess:conversion_status': s["conversion_status"].encode(),
            b'sess:referrer':          s["referrer"].encode(),
            b'sess:viewed_products':   ",".join(s["viewed_products"]).encode(),
            b'device:type':            s["device_profile"]["type"].encode(),
            b'device:os':              s["device_profile"]["os"].encode(),
            b'device:browser':         s["device_profile"]["browser"].encode(),
            b'device:city':            s["geo_data"]["city"].encode(),
            b'device:state':           s["geo_data"]["state"].encode(),
            b'stats:page_view_count':  str(len(s["page_views"])).encode(),
            b'stats:cart_size':        str(len(s["cart_contents"])).encode(),
        }
        batch.put(rk.encode(), data)
        count += 1
    batch.send()
    print(f"  Loaded {count} sessions into user_sessions")

def query_user_sessions(conn, user_id, limit=10):
    """Retrieve most recent sessions for a user (newest-first)."""
    table = conn.table('user_sessions')
    start = f"{user_id}#".encode()
    stop  = f"{user_id}$".encode()
    results = []
    for key, data in table.scan(row_start=start, row_stop=stop, limit=limit):
        results.append({
            "row_key":     key.decode(),
            "session_id":  data.get(b'sess:session_id', b'').decode(),
            "start_time":  data.get(b'sess:start_time', b'').decode(),
            "conversion":  data.get(b'sess:conversion_status', b'').decode(),
            "device":      data.get(b'device:type', b'').decode(),
        })
    return results

def query_converted_sessions(conn, user_id):
    """Return only converted sessions for a user."""
    from happybase.hbase.ttypes import TColumn
    table = conn.table('user_sessions')
    start = f"{user_id}#".encode()
    stop  = f"{user_id}$".encode()
    results = []
    for key, data in table.scan(row_start=start, row_stop=stop):
        if data.get(b'sess:conversion_status', b'').decode() == 'converted':
            results.append({
                "session_id": data.get(b'sess:session_id',b'').decode(),
                "start_time": data.get(b'sess:start_time',b'').decode(),
                "products":   data.get(b'sess:viewed_products',b'').decode(),
            })
    return results

# ════════════════════════════════════════════════════════════════════════════
# DEMO SIMULATION
# ════════════════════════════════════════════════════════════════════════════

def demo_simulate():
    print("\n══ HBASE QUERY RESULTS (SIMULATED) ══\n")

    print("Query 1 – All sessions for user_000042 (newest first):")
    sessions = [
        {"row_key":"user_000042#9223370799715000807","session_id":"sess_a7b3c9d8e2",
         "start_time":"2025-03-12T14:37:22","conversion":"converted","device":"mobile"},
        {"row_key":"user_000042#9223370799723100542","session_id":"sess_b3f4e1d2c7",
         "start_time":"2025-03-08T09:15:11","conversion":"browsing","device":"desktop"},
        {"row_key":"user_000042#9223370799731200300","session_id":"sess_c9a2b7f1e5",
         "start_time":"2025-03-02T20:44:02","conversion":"abandoned","device":"mobile"},
    ]
    for s in sessions:
        print(f"  {s['session_id']} | {s['start_time']} | {s['conversion']:<10} | {s['device']}")

    print("\nQuery 2 – Converted sessions only for user_000042:")
    print("  sess_a7b3c9d8e2 | 2025-03-12T14:37:22 | prod_00123,prod_02456,prod_01872")

    print("\nQuery 3 – Product performance for prod_00123 (March 2025):")
    print(f"  {'Date':<12} {'Views':>6} {'Unique':>7} {'Units':>6} {'Revenue':>10} {'Conv%':>6}")
    data = [
        ("2025-03-10", 48, 35, 4,  519.96, 8.3),
        ("2025-03-11", 62, 47, 6,  779.94, 9.7),
        ("2025-03-12", 71, 53, 9, 1169.91,12.7),
    ]
    for row in data:
        print(f"  {row[0]:<12} {row[1]:>6} {row[2]:>7} {row[3]:>6} ${row[4]:>9,.2f} {row[5]:>5.1f}%")

    print("\nSchema comparison – MongoDB vs HBase:")
    print("  MongoDB:  Rich queries, aggregation pipelines, nested docs, 16MB doc limit")
    print("  HBase:    Time-series at scale, row key scans, sparse columns, petabyte scale")
    print("  Choice:   Sessions → HBase (volume, time-range scans)")
    print("            Products/Transactions → MongoDB (complex queries, joins)")

# ════════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════════
def main():
    print("=" * 60)
    print("ULK Final Exam – HBase Scripts")
    print("=" * 60)

    print("\n── HBase Shell Commands ──────────────────────────────────")
    print(HBASE_SHELL_COMMANDS)

    if DEMO_MODE:
        demo_simulate()
        return

    # Live HBase path
    print("\n── Creating Tables ───────────────────────────────────────")
    existing = [t.decode() for t in conn.tables()]
    for tname in ['user_sessions', 'product_performance']:
        if tname in existing:
            conn.disable_table(tname)
            conn.delete_table(tname)

    conn.create_table('user_sessions', {
        'sess':   {'max_versions': 1, 'compression': 'NONE', 'bloom_filter_type': 'ROW'},
        'device': {'max_versions': 1, 'compression': 'NONE'},
        'stats':  {'max_versions': 3, 'compression': 'NONE'}
    })
    conn.create_table('product_performance', {
        'views': {'max_versions': 1, 'compression': 'NONE'},
        'sales': {'max_versions': 1, 'compression': 'NONE'}
    })
    print("  Tables created: user_sessions, product_performance")

    print("\n── Loading Sessions ──────────────────────────────────────")
    sessions = []
    for i in range(4):
        path = os.path.join(DATA_DIR, f"sessions_{i}.json")
        with open(path) as f:
            sessions.extend(json.load(f))
    load_sessions_into_hbase(conn, sessions[:500])  # load subset

    print("\n── Querying user_000042 sessions ─────────────────────────")
    results = query_user_sessions(conn, "user_000042")
    for r in results:
        print(" ", r)

    print("\n── Querying converted sessions for user_000042 ───────────")
    converted = query_converted_sessions(conn, "user_000042")
    for r in converted:
        print(" ", r)

if __name__ == "__main__":
    main()
