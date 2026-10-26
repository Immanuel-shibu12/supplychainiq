#!/usr/bin/env python3
"""
SupplyChainIQ - step 4: turn the mr_* collections into CSVs, charts and a findings draft.

    python 04_export_and_charts.py                      # reads MongoDB (supplychainiq db)
    python 04_export_and_charts.py --json-dir out/mr_json   # offline: reads simulated results

Writes into results/:
    *.csv                     one table per MapReduce job
    charts/*.png              figures for the report
    tables.md                 the main tables in Markdown, ready to paste
    findings_draft.md         factual sentences with the numbers filled in
Everything is derived from the data actually in the collections - nothing is hard-coded.
The findings state what the numbers show; explaining WHY is your job in the report.
"""
import argparse
import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt      # noqa: E402
import numpy as np                   # noqa: E402
import pandas as pd                  # noqa: E402

DOW = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
BAD, GOOD, NEUTRAL = "#c0392b", "#2e7d5b", "#3b6ea5"


# --------------------------------------------------------------------------- #
# Loading
# --------------------------------------------------------------------------- #
class Source:
    def __init__(self, json_dir, mongo_uri, db_name):
        self.json_dir, self.db = (Path(json_dir) if json_dir else None), None
        if not json_dir:
            from pymongo import MongoClient
            client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
            client.admin.command("ping")
            self.db = client[db_name]

    def rows(self, name):
        if self.json_dir:
            f = self.json_dir / f"{name}.json"
            return json.load(open(f, encoding="utf-8")) if f.exists() else []
        return list(self.db[name].find({}))

    def frame(self, name):
        rows = self.rows(name)
        if not rows:
            return None
        return pd.DataFrame([{"key": r["_id"], **r["value"]} for r in rows])

    def timings(self):
        if self.db is None:
            return None
        t = list(self.db["meta_mr_timings"].find({}, {"_id": 0}))
        return pd.DataFrame(t) if t else None


def split_key(df, names, sep=" | "):
    parts = df["key"].astype(str).str.split(sep, n=len(names) - 1, expand=True, regex=False)
    for i, n in enumerate(names):
        df[n] = parts[i]
    return df


def md_table(df, max_rows=12, floatfmt="{:,.2f}"):
    d = df.head(max_rows)
    head = "| " + " | ".join(map(str, d.columns)) + " |\n|" + "---|" * len(d.columns) + "\n"
    body = ""
    for _, r in d.iterrows():
        cells = [floatfmt.format(x) if isinstance(x, (float, np.floating)) else str(x) for x in r]
        body += "| " + " | ".join(cells) + " |\n"
    return head + body


def short(s, n=38):
    s = str(s)
    return s if len(s) <= n else s[: n - 1] + "…"


# --------------------------------------------------------------------------- #
# Charts
# --------------------------------------------------------------------------- #
def save(fig, path):
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print("  chart ->", path)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--json-dir", help="read mr_*.json files instead of MongoDB (offline mode)")
    ap.add_argument("--mongo-uri", default="mongodb://localhost:27017")
    ap.add_argument("--db", default="supplychainiq")
    ap.add_argument("--out", default="results")
    args = ap.parse_args()

    src = Source(args.json_dir, args.mongo_uri, args.db)
    out = Path(args.out)
    (out / "charts").mkdir(parents=True, exist_ok=True)
    findings, tables = [], []

    def add_table(title, df, **kw):
        tables.append(f"### {title}\n\n{md_table(df, **kw)}\n")

    # ---- 1. late delivery by shipping mode ------------------------------------------------
    d = src.frame("mr_late_by_mode")
    if d is not None:
        d = d.sort_values("late_rate_pct", ascending=False)
        d.to_csv(out / "late_by_mode.csv", index=False)
        add_table("Late-delivery rate by shipping mode",
                  d[["key", "n", "late", "late_rate_pct", "avg_delay_days"]].rename(columns={"key": "shipping_mode"}))
        fig, ax = plt.subplots(figsize=(7, 4))
        ax.bar(d["key"], d["late_rate_pct"], color=NEUTRAL)
        for i, v in enumerate(d["late_rate_pct"]):
            ax.text(i, v + 0.8, f"{v:.1f}%", ha="center", fontsize=9)
        ax.set_ylabel("Late deliveries (% of order items)")
        ax.set_title("Late-delivery rate by shipping mode")
        ax.set_ylim(0, max(100, d["late_rate_pct"].max() + 8))
        save(fig, out / "charts" / "late_rate_by_mode.png")
        overall = 100 * d["late"].sum() / d["n"].sum()
        hi, lo = d.iloc[0], d.iloc[-1]
        findings.append(f"Across {int(d['n'].sum()):,} order items, {overall:.1f}% were flagged as late-delivery risk.")
        findings.append(f"The highest late rate is {hi['key']} at {hi['late_rate_pct']:.1f}% "
                        f"(average {hi['avg_delay_days']:.2f} days over schedule); the lowest is {lo['key']} "
                        f"at {lo['late_rate_pct']:.1f}% ({lo['avg_delay_days']:.2f} days).")

    # ---- 2. delivery status by region -------------------------------------------------------
    d = src.frame("mr_delivery_status_by_region")
    if d is not None:
        d = split_key(d, ["region", "status"])
        piv = d.pivot_table(index="region", columns="status", values="n", aggfunc="sum", fill_value=0)
        share = piv.div(piv.sum(axis=1), axis=0) * 100
        share.round(2).to_csv(out / "delivery_status_share_by_region.csv")
        fig, ax = plt.subplots(figsize=(9, 5))
        share.sort_index().plot(kind="barh", stacked=True, ax=ax, width=0.8)
        ax.set_xlabel("% of order items")
        ax.set_ylabel("")
        ax.set_title("Delivery outcome by order region")
        ax.legend(title="", fontsize=8, loc="lower center", bbox_to_anchor=(0.5, -0.28), ncol=4)
        save(fig, out / "charts" / "delivery_status_by_region.png")

    # ---- 3. sales / profit by region ------------------------------------------------------------
    d = src.frame("mr_sales_by_region")
    if d is not None:
        d = split_key(d, ["market", "region"]).sort_values("revenue", ascending=False)
        d.to_csv(out / "sales_by_region.csv", index=False)
        add_table("Revenue and profit by region",
                  d[["market", "region", "n", "revenue", "profit", "profit_margin_pct", "loss_item_pct"]])
        s = d.sort_values("profit")
        fig, ax = plt.subplots(figsize=(8, max(4, 0.32 * len(s))))
        ax.barh(s["region"], s["profit"], color=[BAD if x < 0 else GOOD for x in s["profit"]])
        ax.set_xlabel("Total profit")
        ax.set_title("Profit by order region")
        save(fig, out / "charts" / "profit_by_region.png")
        top, bot = d.iloc[0], d.sort_values("profit_margin_pct").iloc[0]
        best = d.sort_values("profit_margin_pct").iloc[-1]
        findings.append(f"{top['region']} ({top['market']}) has the highest revenue: {top['revenue']:,.0f} "
                        f"({100 * top['revenue'] / d['revenue'].sum():.1f}% of the total).")
        findings.append(f"Profit margin ranges from {bot['profit_margin_pct']:.2f}% ({bot['region']}) "
                        f"to {best['profit_margin_pct']:.2f}% ({best['region']}).")
        neg = d[d["profit"] < 0]
        findings.append("Regions with negative total profit: " +
                        (", ".join(neg["region"]) if len(neg) else "none") + ".")

    # ---- 4. monthly trend ---------------------------------------------------------------------------
    d = src.frame("mr_monthly_trend")
    if d is not None:
        d = d.sort_values("key")
        d.to_csv(out / "monthly_trend.csv", index=False)
        fig, ax = plt.subplots(figsize=(10, 4.2))
        ax.plot(d["key"], d["revenue"], label="Revenue", color=NEUTRAL)
        ax.plot(d["key"], d["profit"], label="Profit", color=GOOD)
        step = max(1, len(d) // 12)
        ax.set_xticks(range(0, len(d), step))
        ax.set_xticklabels(d["key"].iloc[::step], rotation=45, ha="right")
        ax.set_title("Monthly revenue and profit")
        ax.legend()
        save(fig, out / "charts" / "monthly_trend.png")
        pk, tr = d.loc[d["revenue"].idxmax()], d.loc[d["revenue"].idxmin()]
        findings.append(f"Monthly revenue is available for {len(d)} months ({d['key'].iloc[0]} to {d['key'].iloc[-1]}); "
                        f"the peak is {pk['key']} ({pk['revenue']:,.0f}) and the low is {tr['key']} ({tr['revenue']:,.0f}).")
        d["calendar_month"] = d["key"].str[-2:]
        cm = d.groupby("calendar_month")["revenue"].mean().sort_values(ascending=False)
        findings.append(f"Averaged across years, the strongest calendar month is {cm.index[0]} and the weakest is "
                        f"{cm.index[-1]} (check that every month appears in the same number of years before reading this as seasonality).")

    # ---- 5. discount impact ----------------------------------------------------------------------------
    d = src.frame("mr_discount_impact")
    if d is not None:
        d = d.sort_values("key")
        d["bucket"] = d["key"].astype(int).map(lambda lo: f"{lo}-{lo + 4}%")
        d.to_csv(out / "discount_impact.csv", index=False)
        add_table("Profit by discount bucket",
                  d[["bucket", "n", "avg_profit_per_item", "avg_profit_ratio_pct", "loss_item_pct"]])
        fig, ax1 = plt.subplots(figsize=(7.5, 4.2))
        ax1.bar(d["bucket"], d["avg_profit_ratio_pct"], color=NEUTRAL)
        ax1.set_ylabel("Average profit ratio (%)")
        ax1.set_xlabel("Discount applied")
        ax2 = ax1.twinx()
        ax2.plot(d["bucket"], d["loss_item_pct"], color=BAD, marker="o")
        ax2.set_ylabel("Loss-making items (%)", color=BAD)
        ax1.set_title("Profitability by discount bucket")
        save(fig, out / "charts" / "discount_impact.png")
        a, b = d.iloc[0], d.iloc[-1]
        findings.append(f"Average profit ratio is {a['avg_profit_ratio_pct']:.2f}% in the {a['bucket']} bucket "
                        f"and {b['avg_profit_ratio_pct']:.2f}% in the {b['bucket']} bucket; the share of "
                        f"loss-making items goes from {a['loss_item_pct']:.1f}% to {b['loss_item_pct']:.1f}%.")

    # ---- 6. products ------------------------------------------------------------------------------------
    d = src.frame("mr_product_performance")
    if d is not None:
        d = split_key(d, ["category", "product"]).sort_values("profit", ascending=False)
        d.to_csv(out / "product_performance.csv", index=False)
        add_table("Top 10 products by total profit",
                  d[["category", "product", "n", "qty", "revenue", "profit", "profit_margin_pct"]].head(10), max_rows=10)
        add_table("Bottom 10 products by total profit",
                  d[["category", "product", "n", "qty", "revenue", "profit", "profit_margin_pct"]].tail(10), max_rows=10)
        top, bot = d.head(10).iloc[::-1], d.tail(10)
        fig, axes = plt.subplots(1, 2, figsize=(13, 5))
        axes[0].barh([short(x) for x in top["product"]], top["profit"], color=GOOD)
        axes[0].set_title("Top 10 products by profit")
        axes[1].barh([short(x) for x in bot["product"]], bot["profit"],
                     color=[BAD if x < 0 else NEUTRAL for x in bot["profit"]])
        axes[1].set_title("Bottom 10 products by profit")
        save(fig, out / "charts" / "top_bottom_products.png")
        lossy = int((d["profit"] < 0).sum())
        findings.append(f"{len(d):,} distinct products were sold; {lossy:,} have negative total profit. "
                        f"The most profitable is '{d.iloc[0]['product']}' ({d.iloc[0]['profit']:,.0f}); "
                        f"the least profitable is '{d.iloc[-1]['product']}' ({d.iloc[-1]['profit']:,.0f}).")

    # ---- 7. segment x payment ----------------------------------------------------------------------------------
    d = src.frame("mr_segment_payment")
    if d is not None:
        d = split_key(d, ["segment", "payment_type"]).sort_values("revenue", ascending=False)
        d.to_csv(out / "segment_payment.csv", index=False)
        add_table("Revenue by customer segment and payment type",
                  d[["segment", "payment_type", "n", "revenue", "profit", "avg_item_value"]])
        seg = d.groupby("segment")[["revenue", "profit"]].sum().sort_values("revenue", ascending=False)
        findings.append("Revenue by customer segment: " +
                        "; ".join(f"{k} {r.revenue:,.0f} (profit {r.profit:,.0f})" for k, r in seg.iterrows()) + ".")

    # ---- 8. clickstream -------------------------------------------------------------------------------------------
    h = src.frame("mr_clicks_by_hour")
    if h is not None:
        h["key"] = h["key"].astype(int)
        h = h.sort_values("key")
        h.to_csv(out / "clicks_by_hour.csv", index=False)
        fig, ax = plt.subplots(figsize=(9, 4))
        ax.bar(h["key"], h["hits"], color=NEUTRAL)
        ax.set_xticks(range(0, 24))
        ax.set_xlabel("Hour of day")
        ax.set_ylabel("Page views")
        ax.set_title("Browsing activity by hour")
        save(fig, out / "charts" / "clicks_by_hour.png")
        pk, qt = h.loc[h["hits"].idxmax()], h.loc[h["hits"].idxmin()]
        findings.append(f"Of {int(h['hits'].sum()):,} logged page views, activity peaks at hour {int(pk['key']):02d}:00 "
                        f"({int(pk['hits']):,} views) and is lowest at {int(qt['key']):02d}:00 ({int(qt['hits']):,}).")
    w = src.frame("mr_clicks_by_dow")
    if w is not None:
        w["key"] = w["key"].astype(int)
        w = w.sort_values("hits", ascending=False)
        findings.append(f"The busiest weekday for browsing is {DOW[int(w.iloc[0]['key'])]} "
                        f"and the quietest is {DOW[int(w.iloc[-1]['key'])]}.")
    tp = src.frame("mr_top_products_viewed")
    if tp is not None:
        tp = tp.sort_values("hits", ascending=False)
        tp.rename(columns={"key": "product"}).to_csv(out / "top_products_viewed.csv", index=False)
        add_table("Most-viewed products", tp.rename(columns={"key": "product"})[["product", "hits"]], max_rows=10,
                  floatfmt="{:,.0f}")
        findings.append("Most-viewed products: " + "; ".join(
            f"{r['key']} ({int(r['hits']):,})" for _, r in tp.head(3).iterrows()) + ".")

    # ---- 9. browsing vs sales by department (cross-dataset) -----------------------------------------------------------
    cd, sd = src.frame("mr_clicks_by_department"), src.frame("mr_sales_by_department")
    if cd is not None and sd is not None:
        m = cd[["key", "hits"]].merge(sd[["key", "revenue", "profit"]], on="key", how="outer").fillna(0)
        m["hit_share_pct"] = 100 * m["hits"] / m["hits"].sum()
        m["revenue_share_pct"] = 100 * m["revenue"] / m["revenue"].sum()
        m = m.sort_values("revenue_share_pct", ascending=False).rename(columns={"key": "department"})
        m.to_csv(out / "browsing_vs_sales_by_department.csv", index=False)
        add_table("Browsing share vs revenue share by department",
                  m[["department", "hits", "hit_share_pct", "revenue", "revenue_share_pct"]], max_rows=15)
        x = np.arange(len(m))
        fig, ax = plt.subplots(figsize=(10, 4.5))
        ax.bar(x - 0.2, m["hit_share_pct"], 0.4, label="Share of page views", color=NEUTRAL)
        ax.bar(x + 0.2, m["revenue_share_pct"], 0.4, label="Share of revenue", color=GOOD)
        ax.set_xticks(x)
        ax.set_xticklabels(m["department"], rotation=40, ha="right")
        ax.set_ylabel("%")
        ax.set_title("Browsing vs revenue by department")
        ax.legend()
        save(fig, out / "charts" / "browsing_vs_sales.png")
        both = m[(m["hits"] > 0) & (m["revenue"] > 0)]
        if len(both) >= 3:
            rho = both["hits"].rank().corr(both["revenue"].rank())
            findings.append(f"Across {len(both)} departments present in both datasets, the Spearman rank correlation "
                            f"between page views and revenue is {rho:.2f}.")
        both = both.assign(gap=both["hit_share_pct"] - both["revenue_share_pct"]).sort_values("gap")
        if len(both):
            findings.append(f"Largest gap in favour of browsing: {both.iloc[-1]['department']} "
                            f"({both.iloc[-1]['hit_share_pct']:.1f}% of views vs {both.iloc[-1]['revenue_share_pct']:.1f}% of revenue); "
                            f"in favour of revenue: {both.iloc[0]['department']} "
                            f"({both.iloc[0]['hit_share_pct']:.1f}% of views vs {both.iloc[0]['revenue_share_pct']:.1f}% of revenue).")
        missing = sorted(set(cd["key"]) ^ set(sd["key"]))
        if missing:
            findings.append("Department names present in only one dataset (check spelling before trusting the join): "
                            + ", ".join(missing) + ".")

    # ---- 10. timings -------------------------------------------------------------------------------------------------------
    t = src.timings()
    if t is not None:
        piv = t.pivot_table(index="job", columns="engine", values="seconds", aggfunc="first")
        piv.round(3).to_csv(out / "run_times.csv")
        add_table("Run time in seconds: mapReduce vs aggregation", piv.reset_index(), floatfmt="{:,.3f}")

    (out / "tables.md").write_text("# Result tables\n\n" + "\n".join(tables), encoding="utf-8")
    (out / "findings_draft.md").write_text(
        "# Findings draft (generated from the collections)\n\n"
        "Each line states what the numbers show. Add the explanation and business recommendation yourself.\n\n"
        + "\n".join(f"- {f}" for f in findings) + "\n", encoding="utf-8")
    print(f"\n{len(findings)} findings -> {out / 'findings_draft.md'}\ntables   -> {out / 'tables.md'}")


if __name__ == "__main__":
    main()
