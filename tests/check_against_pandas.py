"""
check_against_pandas.py
-----------------------
Recomputes the MapReduce results independently with pandas and compares them with the
files written by tests/simulate_mapreduce.js (out/mr_json/*.json).

    python tests/check_against_pandas.py [out_dir]

Exit code 0 = every job matches. This verifies the map/reduce/finalize LOGIC; to verify the
real server run, use 03_validate_aggregation.js inside mongosh.
"""
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

out = Path(sys.argv[1] if len(sys.argv) > 1 else "out")
EXCLUDED = {"CANCELED", "SUSPECTED_FRAUD"}


def ndjson(name):
    with open(out / name, encoding="utf-8") as fh:
        return pd.json_normalize([json.loads(line) for line in fh])


def mr(name):
    with open(out / "mr_json" / f"{name}.json", encoding="utf-8") as fh:
        return {str(r["_id"]): r["value"] for r in json.load(fh)}


failures = 0


def compare(job, expected, fields, tol=0.011):
    """expected: {key: {field: value}} computed with pandas."""
    global failures
    got = mr(job)
    problems = []
    if set(got) != set(expected):
        problems.append(f"key sets differ: only-MR={sorted(set(got) - set(expected))[:3]} "
                        f"only-pandas={sorted(set(expected) - set(got))[:3]}")
    for k in set(got) & set(expected):
        for f in fields:
            a, b = got[k].get(f), expected[k][f]
            if a is None and (b is None or (isinstance(b, float) and np.isnan(b))):
                continue
            if a is None or abs(a - b) > tol:
                problems.append(f"{k}.{f}: MR={a} pandas={b}")
    status = "PASS" if not problems else "FAIL"
    failures += bool(problems)
    print(f"  {status}  {job:32s} {len(expected):4d} keys, fields={fields}")
    for p in problems[:5]:
        print("        ", p)


orders = ndjson("orders.ndjson")
clicks = ndjson("clicks.ndjson")
rev = orders[~orders["order_status"].isin(EXCLUDED)].copy()
print(f"orders={len(orders)} (revenue base={len(rev)}), clicks={len(clicks)}")

# --- late by mode --------------------------------------------------------------------------
d = orders[orders["shipping.mode"].notna()]
g = d.groupby("shipping.mode").agg(n=("_id", "size"), late=("shipping.late_risk", "sum"),
                                   slip=("shipping.delay_days", "sum"))
exp = {k: {"n": r.n, "late": r.late, "slip": r.slip,
           "late_rate_pct": round(100 * r.late / r.n, 2)} for k, r in g.iterrows()}
compare("mr_late_by_mode", exp, ["n", "late", "slip", "late_rate_pct"])

# --- delivery status by region ---------------------------------------------------------------
g = orders.groupby(["order_location.region", "shipping.delivery_status"]).size()
compare("mr_delivery_status_by_region", {f"{a} | {b}": {"n": n} for (a, b), n in g.items()}, ["n"])

# --- sales by region -------------------------------------------------------------------------
g = rev.groupby(["order_location.market", "order_location.region"]).agg(
    n=("_id", "size"), sales=("financials.sales", "sum"), revenue=("financials.item_total", "sum"),
    profit=("financials.profit_per_order", "sum"), loss=("derived.is_loss", "sum"))
exp = {f"{a} | {b}": {"n": r.n, "sales": r.sales, "revenue": r.revenue, "profit": r.profit,
                      "loss_items": r.loss,
                      "profit_margin_pct": round(100 * r.profit / r.revenue, 2)}
       for (a, b), r in g.iterrows()}
compare("mr_sales_by_region", exp, ["n", "sales", "revenue", "profit", "loss_items", "profit_margin_pct"], tol=0.05)

# --- sales by department -----------------------------------------------------------------------
g = rev.assign(dep=rev["product.department_name"].str.lower()).groupby("dep").agg(
    n=("_id", "size"), revenue=("financials.item_total", "sum"), profit=("financials.profit_per_order", "sum"))
compare("mr_sales_by_department", {k: {"n": r.n, "revenue": r.revenue, "profit": r.profit}
                                   for k, r in g.iterrows()}, ["n", "revenue", "profit"], tol=0.05)

# --- monthly trend (rows with a parsed order date only) --------------------------------------------
m = rev[rev["order_date"].notna()].copy()
m["month"] = pd.to_datetime(m["order_date"]).dt.strftime("%Y-%m")
g = m.groupby("month").agg(n=("_id", "size"), qty=("financials.quantity", "sum"),
                           revenue=("financials.item_total", "sum"), profit=("financials.profit_per_order", "sum"))
compare("mr_monthly_trend", {k: {"n": r.n, "qty": r.qty, "revenue": r.revenue, "profit": r.profit}
                             for k, r in g.iterrows()}, ["n", "qty", "revenue", "profit"], tol=0.05)

# --- discount impact ---------------------------------------------------------------------------------
x = rev[rev["financials.discount_rate"].notna()].copy()
pct = np.floor(x["financials.discount_rate"] * 100 + 0.5)
x["bucket"] = (np.floor(pct / 5) * 5).astype(int).map(lambda lo: f"{lo:02d}")
g = x.groupby("bucket").agg(n=("_id", "size"), revenue=("financials.item_total", "sum"),
                            profit=("financials.profit_per_order", "sum"), loss=("derived.is_loss", "sum"))
compare("mr_discount_impact", {k: {"n": r.n, "revenue": r.revenue, "profit": r.profit, "loss_items": r.loss}
                               for k, r in g.iterrows()}, ["n", "revenue", "profit", "loss_items"], tol=0.05)

# --- product performance ---------------------------------------------------------------------------
g = rev.groupby(["product.category_name", "product.name"]).agg(
    n=("_id", "size"), qty=("financials.quantity", "sum"), profit=("financials.profit_per_order", "sum"))
compare("mr_product_performance", {f"{a} | {b}": {"n": r.n, "qty": r.qty, "profit": r.profit}
                                   for (a, b), r in g.iterrows()}, ["n", "qty", "profit"], tol=0.05)

# --- segment x payment -------------------------------------------------------------------------------
g = rev.groupby(["customer.segment", "payment_type"]).agg(n=("_id", "size"), revenue=("financials.item_total", "sum"))
compare("mr_segment_payment", {f"{a} | {b}": {"n": r.n, "revenue": r.revenue} for (a, b), r in g.iterrows()},
        ["n", "revenue"], tol=0.05)

# --- clickstream ---------------------------------------------------------------------------------------
h = clicks[clicks["hour"].notna()].copy()
g = h.groupby("hour").agg(hits=("event_type", "size"), pv=("event_type", lambda s: (s == "product_view").sum()))
compare("mr_clicks_by_hour", {str(int(k)): {"hits": r.hits, "product_views": r.pv} for k, r in g.iterrows()},
        ["hits", "product_views"])

dw = clicks[clicks["dow"].notna()]
g = dw.groupby("dow").size()
compare("mr_clicks_by_dow", {str(int(k)): {"hits": n} for k, n in g.items()}, ["hits"])

dp = clicks[clicks["department"].notna()].copy()
dp["dep"] = dp["department"].str.lower()
g = dp.groupby("dep").agg(hits=("event_type", "size"), pv=("event_type", lambda s: (s == "product_view").sum()))
compare("mr_clicks_by_department", {k: {"hits": r.hits, "product_views": r.pv} for k, r in g.iterrows()},
        ["hits", "product_views"])

pr = clicks[(clicks["event_type"] == "product_view") & clicks["product"].notna()]
compare("mr_top_products_viewed", {k: {"hits": n} for k, n in pr.groupby("product").size().items()}, ["hits"])

print("\nALL JOBS MATCH" if not failures else f"\n{failures} JOB(S) DIFFER")
sys.exit(1 if failures else 0)
