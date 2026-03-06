"""
ULK Final Project — Part 4: Visualizations & Business Insights
==============================================================
Reads the CSV outputs from Parts 2 & 3 and generates publication-quality
charts for the Technical Report.

Run AFTER all previous parts:
    python part4_visualizations.py   (does NOT need spark-submit)

Requirements:
    pip install pandas matplotlib seaborn plotly kaleido

Output:
    charts/01_monthly_revenue.png
    charts/02_top_products_revenue.png
    charts/03_user_segments.png
    charts/04_device_conversion_funnel.png
    charts/05_channel_conversion.png
    charts/06_clv_tier_distribution.png
    charts/07_cohort_retention.png
    charts/08_day_of_week_revenue.png
    charts/09_category_revenue_trend.png
    charts/10_weekly_browse_vs_buy.png
    charts/11_windowed_revenue_spikes.png
    charts/12_cart_abandonment.png
"""

import os, glob
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.ticker as mticker
import seaborn as sns
import numpy as np

# ── style ─────────────────────────────────────────────────────────────────────
BRAND   = "#1A56A4"
ACCENT  = "#E8A020"
SUCCESS = "#27AE60"
DANGER  = "#E74C3C"
LIGHT   = "#EBF3FB"
GREY    = "#7F8C8D"
BG      = "#F8FAFD"

palette = [BRAND, ACCENT, SUCCESS, DANGER, "#8E44AD", "#16A085", "#D35400", "#2980B9"]

plt.rcParams.update({
    "figure.facecolor":  BG,
    "axes.facecolor":    BG,
    "axes.edgecolor":    "#CCCCCC",
    "axes.labelcolor":   "#333333",
    "xtick.color":       "#555555",
    "ytick.color":       "#555555",
    "text.color":        "#333333",
    "grid.color":        "#E0E0E0",
    "grid.linewidth":    0.8,
    "axes.titlesize":    14,
    "axes.titleweight":  "bold",
    "axes.labelsize":    11,
    "legend.fontsize":   10,
    "figure.dpi":        150,
    "savefig.dpi":       180,
    "savefig.bbox":      "tight",
    "font.family":       "DejaVu Sans",
})

CHARTS_DIR   = "charts"
ANALYTICS_DIR = "analytics"
INTEGRATION_DIR = "integration"
STREAMING_DIR = "streaming"
os.makedirs(CHARTS_DIR, exist_ok=True)


# ── helpers ───────────────────────────────────────────────────────────────────

def load_csv(folder: str, base: str = ANALYTICS_DIR) -> pd.DataFrame:
    """Load the first CSV file found inside a _csv output folder."""
    pattern = os.path.join(base, folder + "_csv", "*.csv")
    files = glob.glob(pattern)
    if not files:
        pattern2 = os.path.join(base, folder, "*.csv")
        files = glob.glob(pattern2)
    if not files:
        raise FileNotFoundError(f"No CSV found for: {folder} in {base}")
    return pd.read_csv(files[0])


def save_chart(fig, name: str):
    path = os.path.join(CHARTS_DIR, name)
    fig.savefig(path)
    plt.close(fig)
    print(f"  ✔  {name}")


def add_value_labels(ax, fmt="{:.0f}", fontsize=9, color="white", padding=3):
    for bar in ax.patches:
        h = bar.get_height()
        if h > 0:
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                h + padding,
                fmt.format(h),
                ha="center", va="bottom",
                fontsize=fontsize, color="#333333", fontweight="bold"
            )


def title_block(ax, title, subtitle=""):
    ax.set_title(title, pad=12, color="#1A1A2E", fontsize=14, fontweight="bold")
    if subtitle:
        ax.text(0.5, 1.01, subtitle, transform=ax.transAxes,
                ha="center", va="bottom", fontsize=9, color=GREY, style="italic")


# ══════════════════════════════════════════════════════════════════════════════
# CHART 1 — Monthly Revenue Trend
# ══════════════════════════════════════════════════════════════════════════════

def chart_monthly_revenue():
    df = load_csv("sql2_monthly_revenue")
    df["period"] = df["txn_year"].astype(str) + "-" + df["txn_month"].astype(str).str.zfill(2)
    df = df.sort_values("period")

    fig, ax1 = plt.subplots(figsize=(12, 5))
    ax2 = ax1.twinx()

    ax1.fill_between(df["period"], df["monthly_revenue"], alpha=0.15, color=BRAND)
    ax1.plot(df["period"], df["monthly_revenue"], color=BRAND, lw=2.5, marker="o",
             markersize=5, label="Monthly Revenue")
    ax2.bar(df["period"], df["total_transactions"], color=ACCENT, alpha=0.5,
            label="Transactions")

    ax1.set_ylabel("Revenue ($)", color=BRAND)
    ax2.set_ylabel("Transactions", color=ACCENT)
    ax1.tick_params(axis="y", labelcolor=BRAND)
    ax2.tick_params(axis="y", labelcolor=ACCENT)
    plt.xticks(rotation=45, ha="right")
    ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    ax1.grid(True, axis="y", ls="--")
    title_block(ax1, "Monthly Revenue & Transaction Volume",
                "Completed and shipped orders only")

    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left")

    save_chart(fig, "01_monthly_revenue.png")


# ══════════════════════════════════════════════════════════════════════════════
# CHART 2 — Top 10 Products by Revenue
# ══════════════════════════════════════════════════════════════════════════════

def chart_top_products():
    df = load_csv("sql1_top_products_by_revenue")
    df = df.nlargest(10, "total_revenue").sort_values("total_revenue")

    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.barh(df["product_id"], df["total_revenue"],
                   color=[BRAND if i % 2 == 0 else ACCENT for i in range(len(df))],
                   edgecolor="white", height=0.65)

    for bar, val in zip(bars, df["total_revenue"]):
        ax.text(bar.get_width() + 50, bar.get_y() + bar.get_height()/2,
                f"${val:,.0f}", va="center", fontsize=9, fontweight="bold")

    ax.set_xlabel("Total Revenue ($)")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    ax.set_xlim(0, df["total_revenue"].max() * 1.18)
    ax.invert_yaxis()
    ax.grid(True, axis="x", ls="--", alpha=0.7)
    title_block(ax, "Top 10 Products by Total Revenue",
                "Completed & shipped transactions")
    save_chart(fig, "02_top_products_revenue.png")


# ══════════════════════════════════════════════════════════════════════════════
# CHART 3 — User Segment Distribution (RFM)
# ══════════════════════════════════════════════════════════════════════════════

def chart_user_segments():
    df = load_csv("sql4_user_segments")
    df = df.sort_values("total_revenue_from_segment", ascending=False)

    seg_colors = {
        "Champions": BRAND, "Loyal": SUCCESS, "High-Value New": ACCENT,
        "Potential Loyal": "#8E44AD", "One-Time": GREY
    }
    colors = [seg_colors.get(s, GREY) for s in df["segment"]]

    fig, axes = plt.subplots(1, 2, figsize=(13, 5))

    # Pie: user count per segment
    wedge_props = {"linewidth": 2, "edgecolor": "white"}
    axes[0].pie(df["num_users"], labels=df["segment"], colors=colors,
                autopct="%1.1f%%", startangle=140, wedgeprops=wedge_props,
                textprops={"fontsize": 10})
    axes[0].set_title("User Distribution by Segment", fontweight="bold", pad=12)

    # Bar: revenue contribution
    bars = axes[1].bar(df["segment"], df["total_revenue_from_segment"],
                        color=colors, edgecolor="white")
    for bar, val in zip(bars, df["total_revenue_from_segment"]):
        axes[1].text(bar.get_x() + bar.get_width()/2, bar.get_height() + 20,
                     f"${val:,.0f}", ha="center", fontsize=9, fontweight="bold")

    axes[1].set_ylabel("Total Revenue ($)")
    axes[1].yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    axes[1].set_title("Revenue Contribution by Segment", fontweight="bold", pad=12)
    axes[1].grid(True, axis="y", ls="--", alpha=0.7)
    plt.xticks(rotation=20, ha="right")

    fig.suptitle("RFM Customer Segmentation", fontsize=15, fontweight="bold",
                 color="#1A1A2E", y=1.01)
    save_chart(fig, "03_user_segments.png")


# ══════════════════════════════════════════════════════════════════════════════
# CHART 4 — Conversion Funnel by Device
# ══════════════════════════════════════════════════════════════════════════════

def chart_device_funnel():
    df = load_csv("funnel_by_device", base=INTEGRATION_DIR)
    devices = df["device_type"].tolist()
    stages = ["sessions", "product_viewed", "cart_initiated", "purchased"]
    stage_labels = ["Session\nStarted", "Product\nViewed", "Cart\nInitiated", "Purchased"]

    fig, ax = plt.subplots(figsize=(11, 6))
    x = np.arange(len(stage_labels))
    width = 0.25
    dev_colors = [BRAND, ACCENT, SUCCESS]

    for i, (device, color) in enumerate(zip(devices, dev_colors)):
        row = df[df["device_type"] == device].iloc[0]
        vals = [row[s] for s in stages]
        bars = ax.bar(x + i * width, vals, width, label=device.title(),
                      color=color, alpha=0.85, edgecolor="white")
        for bar, val in zip(bars, vals):
            ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 10,
                    f"{val:,}", ha="center", fontsize=8, fontweight="bold")

    ax.set_xticks(x + width)
    ax.set_xticklabels(stage_labels)
    ax.set_ylabel("Number of Sessions / Events")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
    ax.legend(title="Device")
    ax.grid(True, axis="y", ls="--", alpha=0.7)
    title_block(ax, "Purchase Funnel by Device Type",
                "Stages: Session → Product View → Cart → Purchase")
    save_chart(fig, "04_device_conversion_funnel.png")


# ══════════════════════════════════════════════════════════════════════════════
# CHART 5 — Conversion Rate by Acquisition Channel
# ══════════════════════════════════════════════════════════════════════════════

def chart_channel_conversion():
    df = load_csv("funnel_by_channel", base=INTEGRATION_DIR)
    df = df.sort_values("overall_cvr", ascending=True)

    fig, ax = plt.subplots(figsize=(10, 6))
    cmap = plt.cm.Blues(np.linspace(0.35, 0.85, len(df)))
    bars = ax.barh(df["channel"], df["overall_cvr"] * 100,
                   color=cmap, edgecolor="white", height=0.6)

    for bar, val in zip(bars, df["overall_cvr"]):
        ax.text(bar.get_width() + 0.1, bar.get_y() + bar.get_height()/2,
                f"{val*100:.1f}%", va="center", fontsize=10, fontweight="bold")

    ax.set_xlabel("Conversion Rate (%)")
    ax.set_xlim(0, df["overall_cvr"].max() * 140)
    ax.grid(True, axis="x", ls="--", alpha=0.7)
    title_block(ax, "Session-to-Purchase Conversion Rate by Channel",
                "% of sessions that resulted in a completed transaction")
    save_chart(fig, "05_channel_conversion.png")


# ══════════════════════════════════════════════════════════════════════════════
# CHART 6 — CLV Tier Distribution
# ══════════════════════════════════════════════════════════════════════════════

def chart_clv_tiers():
    try:
        df = load_csv("clv_tier_summary", base=INTEGRATION_DIR)
    except FileNotFoundError:
        print("  ⚠  CLV tier summary not found — skipping chart 6")
        return

    tier_order = ["Platinum", "Gold", "Silver", "Bronze"]
    tier_colors = {"Platinum": "#1A56A4", "Gold": "#E8A020",
                   "Silver": "#95A5A6",   "Bronze": "#A0522D"}
    df["clv_tier"] = pd.Categorical(df["clv_tier"], categories=tier_order, ordered=True)
    df = df.sort_values("clv_tier")

    fig, axes = plt.subplots(1, 3, figsize=(15, 5))
    colors = [tier_colors[t] for t in df["clv_tier"]]

    # Users per tier
    axes[0].bar(df["clv_tier"], df["num_users"], color=colors, edgecolor="white")
    for ax_bar, v in zip(axes[0].patches, df["num_users"]):
        axes[0].text(ax_bar.get_x() + ax_bar.get_width()/2, ax_bar.get_height() + 0.5,
                     str(v), ha="center", fontweight="bold")
    axes[0].set_title("Users per Tier", fontweight="bold")
    axes[0].set_ylabel("Count")
    axes[0].grid(True, axis="y", ls="--", alpha=0.6)

    # Average CLV score
    axes[1].bar(df["clv_tier"], df["avg_clv_score"], color=colors, edgecolor="white")
    axes[1].set_title("Avg CLV Score per Tier", fontweight="bold")
    axes[1].set_ylabel("CLV Score")
    axes[1].grid(True, axis="y", ls="--", alpha=0.6)

    # Average revenue per tier
    axes[2].bar(df["clv_tier"], df["avg_revenue"], color=colors, edgecolor="white")
    axes[2].set_title("Avg Revenue per User ($)", fontweight="bold")
    axes[2].set_ylabel("Revenue ($)")
    axes[2].yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    axes[2].grid(True, axis="y", ls="--", alpha=0.6)

    fig.suptitle("Customer Lifetime Value (CLV) Tier Analysis",
                 fontsize=15, fontweight="bold", color="#1A1A2E", y=1.02)
    save_chart(fig, "06_clv_tier_distribution.png")


# ══════════════════════════════════════════════════════════════════════════════
# CHART 7 — Cohort Retention Heatmap
# ══════════════════════════════════════════════════════════════════════════════

def chart_cohort_retention():
    df = load_csv("cohort_retention")
    pivot = df.pivot(index="cohort_month", columns="months_since_start",
                     values="retention_rate")
    pivot = pivot.iloc[:, :7]   # show first 7 months

    fig, ax = plt.subplots(figsize=(12, max(5, len(pivot) * 0.55)))
    sns.heatmap(pivot, annot=True, fmt=".0%", cmap="Blues",
                linewidths=0.5, linecolor="white",
                cbar_kws={"label": "Retention Rate"},
                ax=ax, vmin=0, vmax=1)

    ax.set_xlabel("Months Since Registration")
    ax.set_ylabel("Cohort (Registration Month)")
    title_block(ax, "User Cohort Retention Matrix",
                "% of cohort still purchasing in each subsequent month")
    save_chart(fig, "07_cohort_retention.png")


# ══════════════════════════════════════════════════════════════════════════════
# CHART 8 — Day-of-Week Revenue Pattern
# ══════════════════════════════════════════════════════════════════════════════

def chart_dow_revenue():
    df = load_csv("dow_pattern", base=INTEGRATION_DIR)
    dow_order = ["Sunday", "Monday", "Tuesday", "Wednesday",
                 "Thursday", "Friday", "Saturday"]
    df["day_name"] = pd.Categorical(df["day_name"], categories=dow_order, ordered=True)
    df = df.sort_values("day_name")

    fig, ax1 = plt.subplots(figsize=(10, 5))
    ax2 = ax1.twinx()

    colors = [ACCENT if v == df["total_revenue"].max() else BRAND
              for v in df["total_revenue"]]
    bars = ax1.bar(df["day_name"], df["total_revenue"], color=colors,
                   alpha=0.85, edgecolor="white", label="Revenue")
    ax2.plot(df["day_name"], df["avg_order_value"], color=DANGER, lw=2.5,
             marker="D", markersize=7, label="Avg Order Value", zorder=5)

    ax1.set_ylabel("Total Revenue ($)", color=BRAND)
    ax2.set_ylabel("Avg Order Value ($)", color=DANGER)
    ax1.tick_params(axis="y", labelcolor=BRAND)
    ax2.tick_params(axis="y", labelcolor=DANGER)
    ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    ax1.grid(True, axis="y", ls="--", alpha=0.5)

    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left")

    title_block(ax1, "Revenue Pattern by Day of Week",
                "Highlighted bar = highest revenue day")
    save_chart(fig, "08_day_of_week_revenue.png")


# ══════════════════════════════════════════════════════════════════════════════
# CHART 9 — Top Categories Monthly Revenue (stacked area)
# ══════════════════════════════════════════════════════════════════════════════

def chart_category_revenue():
    df = load_csv("seasonal_category_monthly", base=INTEGRATION_DIR)
    df["period"] = df["txn_year"].astype(str) + "-" + df["txn_month"].astype(str).str.zfill(2)

    # Keep top 6 categories by total revenue
    top_cats = (df.groupby("category_id")["category_monthly_revenue"]
                  .sum().nlargest(6).index.tolist())
    df = df[df["category_id"].isin(top_cats)]

    pivot = df.pivot_table(index="period", columns="category_id",
                           values="category_monthly_revenue", aggfunc="sum").fillna(0)
    pivot = pivot.sort_index()

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.stackplot(pivot.index, pivot.T.values,
                 labels=pivot.columns.tolist(),
                 colors=palette[:len(pivot.columns)], alpha=0.85)

    ax.set_ylabel("Monthly Revenue ($)")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    ax.legend(loc="upper left", title="Category", fontsize=9, ncol=2)
    plt.xticks(rotation=45, ha="right")
    ax.grid(True, axis="y", ls="--", alpha=0.5)
    title_block(ax, "Monthly Revenue by Top 6 Product Categories",
                "Stacked area — shows category contribution over time")
    save_chart(fig, "09_category_revenue_trend.png")


# ══════════════════════════════════════════════════════════════════════════════
# CHART 10 — Weekly Browse-to-Buy Ratio
# ══════════════════════════════════════════════════════════════════════════════

def chart_browse_vs_buy():
    df = load_csv("seasonal_weekly", base=INTEGRATION_DIR)
    df["period"] = df["txn_year"].astype(str) + "-W" + df["txn_week"].astype(str).str.zfill(2)
    df = df.sort_values(["txn_year", "txn_week"]).head(13)   # first 13 weeks

    fig, axes = plt.subplots(2, 1, figsize=(12, 8), sharex=True)

    axes[0].plot(df["period"], df["weekly_sessions"], color=BRAND, lw=2,
                 marker="o", ms=4, label="Sessions")
    axes[0].plot(df["period"], df["weekly_txns"], color=ACCENT, lw=2,
                 marker="s", ms=4, label="Transactions")
    axes[0].set_ylabel("Count")
    axes[0].legend()
    axes[0].grid(True, ls="--", alpha=0.6)
    axes[0].set_title("Weekly Sessions vs Transactions", fontweight="bold")

    axes[1].bar(df["period"], df["browse_to_buy_ratio"],
                color=[DANGER if v > df["browse_to_buy_ratio"].mean() * 1.3 else SUCCESS
                       for v in df["browse_to_buy_ratio"]],
                edgecolor="white", alpha=0.85)
    axes[1].axhline(df["browse_to_buy_ratio"].mean(), color=GREY, ls="--",
                    lw=1.5, label="Average")
    axes[1].set_ylabel("Sessions per Transaction")
    axes[1].set_xlabel("Week")
    axes[1].legend()
    axes[1].grid(True, axis="y", ls="--", alpha=0.6)
    axes[1].set_title("Browse-to-Buy Ratio (Lower = More Efficient)",
                       fontweight="bold")

    plt.xticks(rotation=45, ha="right")
    fig.suptitle("Weekly Browsing vs Purchasing Behaviour",
                 fontsize=14, fontweight="bold", color="#1A1A2E", y=1.01)
    save_chart(fig, "10_weekly_browse_vs_buy.png")


# ══════════════════════════════════════════════════════════════════════════════
# CHART 11 — Windowed Revenue with Spike Detection
# ══════════════════════════════════════════════════════════════════════════════

def chart_revenue_spikes():
    df = load_csv("windowed_revenue", base=STREAMING_DIR)
    df["window_start"] = pd.to_datetime(df["window_start"])
    df = df.sort_values("window_start").head(72)   # first 3 days (72 hours)

    fig, ax = plt.subplots(figsize=(13, 5))
    ax.fill_between(df["window_start"], df["window_revenue"],
                    alpha=0.12, color=BRAND)
    ax.plot(df["window_start"], df["window_revenue"],
            color=BRAND, lw=1.5, label="Hourly Revenue")
    ax.plot(df["window_start"], df["rolling_6h_avg"],
            color=ACCENT, lw=2, ls="--", label="6-Hour Rolling Avg")

    # Highlight spike windows
    spikes = df[df["is_revenue_spike"] == True]
    ax.scatter(spikes["window_start"], spikes["window_revenue"],
               color=DANGER, s=80, zorder=5, label="Revenue Spike")

    ax.set_ylabel("Revenue ($)")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    ax.xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%d %b %H:%M"))
    plt.xticks(rotation=45, ha="right")
    ax.legend()
    ax.grid(True, ls="--", alpha=0.5)
    title_block(ax, "Hourly Revenue Stream with Spike Detection",
                "Simulated streaming output — spikes flagged when revenue > 2× rolling average")
    save_chart(fig, "11_windowed_revenue_spikes.png")


# ══════════════════════════════════════════════════════════════════════════════
# CHART 12 — Cart Abandonment Heatmap (device × channel)
# ══════════════════════════════════════════════════════════════════════════════

def chart_cart_abandonment():
    df = load_csv("cart_abandonment", base=INTEGRATION_DIR)
    pivot = df.pivot_table(index="device_type", columns="channel",
                           values="abandonment_rate", aggfunc="mean").fillna(0)

    fig, ax = plt.subplots(figsize=(12, 4))
    sns.heatmap(pivot, annot=True, fmt=".1%", cmap="Reds",
                linewidths=0.5, linecolor="white",
                cbar_kws={"label": "Abandonment Rate"},
                ax=ax, vmin=0, vmax=1)
    ax.set_xlabel("Acquisition Channel")
    ax.set_ylabel("Device Type")
    title_block(ax, "Cart Abandonment Rate: Device × Channel",
                "Higher % = more users add to cart but don't complete purchase")
    save_chart(fig, "12_cart_abandonment.png")


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    print("=" * 55)
    print("  Part 4 — Visualizations & Business Insights")
    print("=" * 55)
    print("\nGenerating charts …\n")

    funcs = [
        ("Chart 01 — Monthly Revenue",           chart_monthly_revenue),
        ("Chart 02 — Top Products",               chart_top_products),
        ("Chart 03 — User Segments (RFM)",        chart_user_segments),
        ("Chart 04 — Device Funnel",              chart_device_funnel),
        ("Chart 05 — Channel Conversion",         chart_channel_conversion),
        ("Chart 06 — CLV Tiers",                  chart_clv_tiers),
        ("Chart 07 — Cohort Retention Heatmap",   chart_cohort_retention),
        ("Chart 08 — Day-of-Week Revenue",        chart_dow_revenue),
        ("Chart 09 — Category Revenue Trend",     chart_category_revenue),
        ("Chart 10 — Browse vs Buy Weekly",       chart_browse_vs_buy),
        ("Chart 11 — Revenue Spikes (Streaming)", chart_revenue_spikes),
        ("Chart 12 — Cart Abandonment Heatmap",   chart_cart_abandonment),
    ]

    failed = []
    for label, fn in funcs:
        try:
            print(f"  → {label}")
            fn()
        except Exception as e:
            print(f"  ✗  SKIPPED ({e})")
            failed.append(label)

    print("\n" + "=" * 55)
    print(f"  Done!  {len(funcs)-len(failed)}/{len(funcs)} charts saved to: {CHARTS_DIR}/")
    if failed:
        print("  Skipped (run previous parts first):")
        for f in failed:
            print(f"    • {f}")
    print("=" * 55)


if __name__ == "__main__":
    main()
