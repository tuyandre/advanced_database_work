"""
visualizations.py
ULK Software Engineering Final Exam-Tuyizere Andre-202540326 
– Part 4
Generates all analytics visualizations from ecommerce dataset.
"""

import json, os, math, random
from collections import defaultdict
from datetime import datetime

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.ticker as mticker

DATA_DIR  = "ecommerce_data"
OUT_DIR   = "charts"
os.makedirs(OUT_DIR, exist_ok=True)

random.seed(42)

# ── Palette ───────────────────────────────────────────────────────────────────
C = ["#2563EB","#16A34A","#DC2626","#D97706","#7C3AED","#0891B2","#BE185D","#65A30D"]
GRAY = "#6B7280"

def save(name):
    path = os.path.join(OUT_DIR, name)
    plt.savefig(path, dpi=150, bbox_inches="tight", facecolor="white")
    plt.close()
    print(f"  Saved {path}")
    return path

def load_json(fn):
    with open(os.path.join(DATA_DIR, fn)) as f:
        return json.load(f)

def load_all_sessions():
    s = []
    for i in range(4):
        s.extend(load_json(f"sessions_{i}.json"))
    return s

# ════════════════════════════════════════════════════════════════════════════
# DATA PREPARATION
# ════════════════════════════════════════════════════════════════════════════
def prepare_data():
    txns     = load_json("transactions.json")
    products = load_json("products.json")
    users    = load_json("users.json")
    sessions = load_all_sessions()
    cats     = load_json("categories.json")

    cat_name = {c["category_id"]: c["name"].split()[0] for c in cats}
    prod_cat  = {p["product_id"]: cat_name.get(p["category_id"],"Other")
                 for p in products}
    prod_price= {p["product_id"]: p["base_price"] for p in products}

    return txns, products, users, sessions, prod_cat, prod_price, cat_name

# ── Chart 1: Monthly Revenue Trend ────────────────────────────────────────────
def chart_monthly_revenue(txns):
    monthly = defaultdict(lambda:{"revenue":0,"orders":0,"discount":0})
    for t in txns:
        if t.get("status") not in ("completed","shipped"):
            continue
        try:
            m = t["timestamp"][:7]
        except Exception:
            continue
        monthly[m]["revenue"]  += t.get("total", 0)
        monthly[m]["orders"]   += 1
        monthly[m]["discount"] += t.get("discount", 0)

    months  = sorted(monthly)
    revenue = [monthly[m]["revenue"] for m in months]
    orders  = [monthly[m]["orders"]  for m in months]
    disc    = [monthly[m]["discount"] for m in months]

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 7), sharex=True,
                                    gridspec_kw={"height_ratios":[2,1]})
    fig.suptitle("Monthly Revenue & Order Trends", fontsize=15, fontweight="bold", y=0.98)

    x = range(len(months))
    bars = ax1.bar(x, revenue, color=C[0], alpha=0.85, label="Revenue")
    ax1.plot(x, revenue, "o-", color=C[2], linewidth=2, zorder=5)
    ax1.set_ylabel("Revenue (RWF)", fontsize=11)
    ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v,_:f"Rwf{v:,.0f}"))
    ax1.grid(axis="y", alpha=0.3)
    for i,(b,v) in enumerate(zip(bars,revenue)):
        ax1.text(b.get_x()+b.get_width()/2, v+500, f"Rwf{v:,.0f}",
                 ha="center", va="bottom", fontsize=8.5, color=C[0], fontweight="bold")

    ax2.bar(x, orders, color=C[1], alpha=0.75)
    ax2.plot(x, orders, "s--", color=C[3], linewidth=1.5, zorder=5)
    ax2.set_ylabel("# Orders", fontsize=10)
    ax2.set_xticks(list(x))
    ax2.set_xticklabels([m[5:] + "\n" + m[:4] for m in months], fontsize=9)
    ax2.grid(axis="y", alpha=0.3)

    fig.tight_layout()
    return save("01_monthly_revenue.png")

# ── Chart 2: Revenue by Category (horizontal bar) ─────────────────────────────
def chart_category_revenue(txns, prod_cat):
    cat_rev  = defaultdict(float)
    cat_unit = defaultdict(int)
    for t in txns:
        if t.get("status") not in ("completed","shipped"):
            continue
        for item in t.get("items",[]):
            cat = prod_cat.get(item["product_id"], "Other")
            cat_rev[cat]  += item.get("subtotal", 0)
            cat_unit[cat] += item.get("quantity", 0)

    top = sorted(cat_rev, key=lambda k: -cat_rev[k])[:10]
    rev = [cat_rev[c] for c in top]

    fig, ax = plt.subplots(figsize=(10, 6))
    y = range(len(top))
    bars = ax.barh(y, rev, color=[C[i%len(C)] for i in range(len(top))], height=0.6)
    ax.set_yticks(list(y))
    ax.set_yticklabels([t[:22] for t in top], fontsize=10)
    ax.set_xlabel("Total Revenue (RWF)", fontsize=11)
    ax.set_title("Revenue by Product Category (Top 10)", fontsize=14, fontweight="bold")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda v,_: f"Rwf{v:,.0f}"))
    ax.grid(axis="x", alpha=0.3)
    ax.invert_yaxis()
    for bar, v in zip(bars, rev):
        ax.text(v + 200, bar.get_y()+bar.get_height()/2,
                f"Rwf{v:,.0f}", va="center", fontsize=8.5)
    fig.tight_layout()
    return save("02_category_revenue.png")

# ── Chart 3: User Segmentation Donut ──────────────────────────────────────────
def chart_user_segmentation(txns, users):
    user_orders = defaultdict(int)
    user_spend  = defaultdict(float)
    for t in txns:
        if t.get("status") in ("completed","shipped"):
            user_orders[t["user_id"]] += 1
            user_spend[t["user_id"]]  += t.get("total", 0)

    segs = {"High-Frequency (5+)":0,"Regular (2-4)":0,"One-Time":0,"Inactive":0}
    seg_spend = {"High-Frequency (5+)":[],"Regular (2-4)":[],"One-Time":[],"Inactive":[]}
    all_uids = set(u["user_id"] for u in users)
    for uid in all_uids:
        n = user_orders.get(uid,0)
        if   n >= 5: k = "High-Frequency (5+)"
        elif n >= 2: k = "Regular (2-4)"
        elif n == 1: k = "One-Time"
        else:        k = "Inactive"
        segs[k] += 1
        if n > 0: seg_spend[k].append(user_spend[uid])

    labels = list(segs.keys())
    sizes  = list(segs.values())
    colors_d = [C[0],C[1],C[3],GRAY]

    fig, (ax1, ax2) = plt.subplots(1,2, figsize=(12, 6))
    fig.suptitle("Customer Segmentation by Purchase Frequency", fontsize=14, fontweight="bold")

    wedges, texts, autotexts = ax1.pie(
        sizes, labels=labels, autopct="%1.1f%%", startangle=140,
        colors=colors_d, pctdistance=0.75,
        wedgeprops={"linewidth":2, "edgecolor":"white"})
    for t in autotexts: t.set_fontsize(9)
    # draw hole
    circle = plt.Circle((0,0), 0.50, fc="white")
    ax1.add_patch(circle)
    ax1.text(0, 0, f"{sum(sizes)}\nUsers", ha="center", va="center",
             fontsize=11, fontweight="bold", color="#111827")

    # avg spend bars
    avg_spends = [sum(seg_spend[l])/max(1,len(seg_spend[l])) for l in labels]
    bars = ax2.bar(labels, avg_spends, color=colors_d, alpha=0.85, width=0.55)
    ax2.set_ylabel("Avg Lifetime Spend (RWF)", fontsize=11)
    ax2.set_title("Avg Spend per Segment", fontsize=12)
    ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v,_:f"Rwf{v:,.0f}"))
    ax2.grid(axis="y", alpha=0.3)
    ax2.tick_params(axis="x", labelrotation=15, labelsize=9)
    for bar, v in zip(bars, avg_spends):
        ax2.text(bar.get_x()+bar.get_width()/2, v+10, f"Rwf{v:,.0f}",
                 ha="center", va="bottom", fontsize=9, fontweight="bold")

    fig.tight_layout()
    return save("03_user_segmentation.png")

# ── Chart 4: Conversion Funnel ─────────────────────────────────────────────────
def chart_conversion_funnel(sessions, txns):
    total_sess   = len(sessions)
    product_views = sum(1 for s in sessions if s.get("viewed_products"))
    cart_adds     = sum(1 for s in sessions if s.get("cart_contents"))
    converted     = sum(1 for s in sessions if s.get("conversion_status")=="converted")
    completed_txn = sum(1 for t in txns if t.get("status") in ("completed","shipped"))

    stages = ["All Sessions","Viewed Product","Added to Cart","Converted","Completed Order"]
    values = [total_sess, product_views, cart_adds, converted, completed_txn]

    fig, ax = plt.subplots(figsize=(10, 7))
    fig.suptitle("E-Commerce Conversion Funnel", fontsize=14, fontweight="bold")

    colors_f = ["#1D4ED8","#2563EB","#3B82F6","#60A5FA","#93C5FD"]
    bar_h = 0.55
    y_positions = list(range(len(stages)))

    for i,(s,v,c) in enumerate(zip(stages,values,colors_f)):
        width = v / total_sess
        ax.barh(i, width, height=bar_h, color=c, alpha=0.9, left=(1-width)/2)
        ax.text(0.5, i, f"{s}\n{v:,} ({width*100:.1f}%)",
                ha="center", va="center", fontsize=10,
                color="white" if width > 0.15 else "#1D4ED8", fontweight="bold")

    # Drop-off annotations
    for i in range(len(values)-1):
        drop = (values[i]-values[i+1])/values[i]*100
        ax.text(0.88, i+0.5, f"▼{drop:.0f}% drop", va="center",
                fontsize=8.5, color=C[2], fontweight="bold")

    ax.set_xlim(0, 1)
    ax.set_ylim(-0.5, len(stages)-0.5)
    ax.axis("off")
    fig.tight_layout()
    return save("04_conversion_funnel.png")

# ── Chart 5: Device & Referrer Analytics ──────────────────────────────────────
def chart_device_referrer(sessions, txns):
    txn_sessions = set(t["session_id"] for t in txns
                        if t.get("status") in ("completed","shipped")
                        and t.get("session_id"))
    device_data  = defaultdict(lambda:{"total":0,"conv":0})
    ref_data     = defaultdict(lambda:{"total":0,"conv":0,"revenue":0})
    txn_rev = {t["session_id"]: t["total"] for t in txns if t.get("session_id")}

    for s in sessions:
        d = s["device_profile"]["type"]
        r = s.get("referrer","unknown")
        device_data[d]["total"] += 1
        ref_data[r]["total"]    += 1
        if s["session_id"] in txn_sessions:
            device_data[d]["conv"] += 1
            ref_data[r]["conv"]    += 1
            ref_data[r]["revenue"] += txn_rev.get(s["session_id"],0)

    fig, axes = plt.subplots(1, 2, figsize=(13, 6))
    fig.suptitle("Session Analytics: Device & Referrer Performance", fontsize=14, fontweight="bold")

    # Device bar chart
    devs   = list(device_data.keys())
    totals = [device_data[d]["total"] for d in devs]
    convs  = [device_data[d]["conv"]  for d in devs]
    conv_r = [c/t*100 for c,t in zip(convs,totals)]
    x = range(len(devs))
    axes[0].bar(x, totals, color=C[0], alpha=0.7, label="Total Sessions", width=0.4)
    axes[0].bar([i+0.4 for i in x], convs, color=C[1], alpha=0.85, label="Conversions", width=0.4)
    ax_r = axes[0].twinx()
    ax_r.plot([i+0.2 for i in x], conv_r, "D-", color=C[2], linewidth=2.5, markersize=8,
              label="Conv Rate %", zorder=5)
    ax_r.set_ylabel("Conversion Rate %", color=C[2], fontsize=10)
    ax_r.tick_params(axis="y", labelcolor=C[2])
    axes[0].set_xticks([i+0.2 for i in x])
    axes[0].set_xticklabels(devs, fontsize=10)
    axes[0].set_ylabel("Session Count", fontsize=10)
    axes[0].set_title("By Device Type", fontsize=12)
    axes[0].legend(loc="upper left", fontsize=8)
    axes[0].grid(axis="y", alpha=0.3)

    # Referrer horizontal bar
    refs = sorted(ref_data, key=lambda k: -ref_data[k]["total"])[:6]
    rev_vals = [ref_data[r]["revenue"] for r in refs]
    col_r = [ref_data[r]["conv"]/ref_data[r]["total"]*100 for r in refs]
    bars2 = axes[1].barh(refs, rev_vals, color=[C[i%len(C)] for i in range(len(refs))], height=0.5)
    axes[1].set_xlabel("Revenue from Conversions (RWF)", fontsize=10)
    axes[1].set_title("Revenue by Referrer Source", fontsize=12)
    axes[1].xaxis.set_major_formatter(mticker.FuncFormatter(lambda v,_:f"Rwf{v:,.0f}"))
    axes[1].grid(axis="x", alpha=0.3)
    for bar, v, cr in zip(bars2, rev_vals, col_r):
        axes[1].text(v+100, bar.get_y()+bar.get_height()/2,
                     f"Rwf{v:,.0f}  ({cr:.1f}% conv)", va="center", fontsize=8)

    fig.tight_layout()
    return save("05_device_referrer.png")

# ── Chart 6: CLV Distribution ─────────────────────────────────────────────────
def chart_clv_distribution(txns, users, sessions):
    user_stats = defaultdict(lambda:{"spend":0,"orders":0,"sessions":0,"conv":0})
    for t in txns:
        if t.get("status") in ("completed","shipped"):
            user_stats[t["user_id"]]["spend"]  += t.get("total",0)
            user_stats[t["user_id"]]["orders"] += 1
    for s in sessions:
        user_stats[s["user_id"]]["sessions"] += 1
        if s.get("conversion_status") == "converted":
            user_stats[s["user_id"]]["conv"] += 1

    user_reg = {}
    for u in users:
        try:
            reg = datetime.fromisoformat(u["registration_date"])
            tenure = max(1,(datetime(2025,3,31)-reg).days//30)
            user_reg[u["user_id"]] = tenure
        except: pass

    clvs, segs = [], []
    for uid, st in user_stats.items():
        if st["orders"] == 0: continue
        tenure = user_reg.get(uid,3)
        amr  = st["spend"]/tenure
        cr   = st["conv"]/max(1,st["sessions"])
        eng  = min(1.0, cr*2)
        ret  = 0.60 + 0.30*eng
        clv  = round(amr * 6 * ret, 2)
        clvs.append(clv)
        segs.append("High" if clv>500 else ("Medium" if clv>150 else "Low"))

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
    fig.suptitle("Customer Lifetime Value (CLV) Analysis", fontsize=14, fontweight="bold")

    ax1.hist(clvs, bins=30, color=C[0], edgecolor="white", alpha=0.85)
    ax1.axvline(sum(clvs)/len(clvs), color=C[2], linestyle="--", linewidth=2,
                label=f"Mean Rwf{sum(clvs)/len(clvs):,.0f}")
    ax1.set_xlabel("CLV Estimate (RWF)", fontsize=11)
    ax1.set_ylabel("# Users", fontsize=11)
    ax1.set_title("CLV Distribution", fontsize=12)
    ax1.xaxis.set_major_formatter(mticker.FuncFormatter(lambda v,_:f"Rwf{v:,.0f}"))
    ax1.legend()
    ax1.grid(axis="y", alpha=0.3)

    seg_counts = {"High": segs.count("High"), "Medium": segs.count("Medium"), "Low": segs.count("Low")}
    seg_colors  = [C[1], C[3], C[2]]
    wedges, _, autotexts = ax2.pie(
        seg_counts.values(), labels=seg_counts.keys(), autopct="%1.1f%%",
        colors=seg_colors, startangle=90,
        wedgeprops={"linewidth":2,"edgecolor":"white"}, pctdistance=0.75)
    for at in autotexts: at.set_fontsize(10)
    ax2.set_title("CLV Segment Distribution", fontsize=12)
    circle = plt.Circle((0,0), 0.50, fc="white")
    ax2.add_patch(circle)
    ax2.text(0,0,f"{len(clvs)}\nCustomers", ha="center", va="center",
             fontsize=11, fontweight="bold")

    fig.tight_layout()
    return save("06_clv_distribution.png")

# ── Chart 7: Top Products Revenue ─────────────────────────────────────────────
def chart_top_products(txns, prod_cat):
    # Load product names lookup
    products = load_json("products.json")
    prod_name = {p["product_id"]: p["name"] for p in products}

    prod_rev   = defaultdict(float)
    prod_units = defaultdict(int)
    for t in txns:
        if t.get("status") not in ("completed", "shipped"):
            continue
        for item in t.get("items", []):
            pid  = item["product_id"]
            name = prod_name.get(pid, pid)   # use name, fallback to id
            prod_rev[name]   += item.get("subtotal", 0)
            prod_units[name] += item.get("quantity", 0)

    top    = sorted(prod_rev, key=lambda k: -prod_rev[k])[:12]
    revs   = [prod_rev[p]   for p in top]
    units  = [prod_units[p] for p in top]
    # Shorten long product names so they fit on the axis
    labels = [p[:22] + "..." if len(p) > 22 else p for p in top]

    fig, ax = plt.subplots(figsize=(13, 6))
    x  = range(len(top))
    ax.bar(x, revs, color=C[0], alpha=0.82, label="Revenue (RWF)", width=0.55)
    ax2 = ax.twinx()
    ax2.plot(x, units, "s-", color=C[3], linewidth=2.5, markersize=8, label="Units Sold")
    ax.set_xticks(list(x))
    ax.set_xticklabels(labels, rotation=35, ha="right", fontsize=8)
    ax.set_ylabel("Revenue (RWF)", fontsize=11)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"RWF {v:,.0f}"))
    ax2.set_ylabel("Units Sold", fontsize=11, color=C[3])
    ax2.tick_params(axis="y", labelcolor=C[3])
    ax.set_title("Top 12 Products by Revenue", fontsize=14, fontweight="bold")
    ax.grid(axis="y", alpha=0.3)
    lines1, labels1 = ax.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax.legend(lines1 + lines2, labels1 + labels2, fontsize=10)
    fig.tight_layout()
    return save("07_top_products.png")
# ════════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════════
def main():
    print("Generating visualizations...")
    txns, products, users, sessions, prod_cat, prod_price, cat_name = prepare_data()

    charts = []
    charts.append(chart_monthly_revenue(txns))
    charts.append(chart_category_revenue(txns, prod_cat))
    charts.append(chart_user_segmentation(txns, users))
    charts.append(chart_conversion_funnel(sessions, txns))
    charts.append(chart_device_referrer(sessions, txns))
    charts.append(chart_clv_distribution(txns, users, sessions))
    charts.append(chart_top_products(txns, prod_cat))

    print(f"\nAll {len(charts)} charts saved to '{OUT_DIR}/'")
    return charts

if __name__ == "__main__":
    main()
