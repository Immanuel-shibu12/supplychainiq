"""
make_sample_data.py
-------------------
Generates a SMALL, SYNTHETIC stand-in for the two DataCo files so the whole
pipeline can be smoke-tested without downloading the real dataset.

    python tests/make_sample_data.py            # writes into ./data/sample/

The column headers match the real Kaggle files. The VALUES ARE RANDOM: never
quote results computed from this sample in your report. The sample also contains
deliberate "dirt" (duplicates, bad dates, blank ZIPs, odd casing, accented
characters) so the cleaning log has something to report.
"""
import argparse
import random
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

ORDER_COLUMNS = [
    "Type", "Days for shipping (real)", "Days for shipment (scheduled)",
    "Benefit per order", "Sales per customer", "Delivery Status",
    "Late_delivery_risk", "Category Id", "Category Name", "Customer City",
    "Customer Country", "Customer Email", "Customer Fname", "Customer Id",
    "Customer Lname", "Customer Password", "Customer Segment", "Customer State",
    "Customer Street", "Customer Zipcode", "Department Id", "Department Name",
    "Latitude", "Longitude", "Market", "Order City", "Order Country",
    "Order Customer Id", "order date (DateOrders)", "Order Id",
    "Order Item Cardprod Id", "Order Item Discount", "Order Item Discount Rate",
    "Order Item Id", "Order Item Product Price", "Order Item Profit Ratio",
    "Order Item Quantity", "Sales", "Order Item Total", "Order Profit Per Order",
    "Order Region", "Order State", "Order Status", "Order Zipcode",
    "Product Card Id", "Product Category Id", "Product Description",
    "Product Image", "Product Name", "Product Price", "Product Status",
    "shipping date (DateOrders)", "Shipping Mode",
]

# (category_id, category_name, department_id, department_name, product_id, product_name, price)
CATALOG = [
    (17, "Cleats", 4, "Fitness", 1360, "Perfect Fitness Perfect Rip Deck", 59.99),
    (18, "Men's Footwear", 4, "Fitness", 1014, "O'Brien Men's Neoprene Life Vest", 49.98),
    (24, "Women's Apparel", 5, "Apparel", 502, "Nike Men's Dri-FIT Victory Golf Polo", 50.0),
    (29, "Shop By Sport", 7, "Footwear", 365, "Perfect Fitness Perfect Rip Deck", 59.99),
    (43, "Camping & Hiking", 7, "Outdoors", 403, "Nike Men's CJ Elite 2 TD Football Cleat", 129.99),
    (45, "Fishing", 8, "Fan Shop", 957, "Diamondback Women's Serene Classic Comfort Bike", 299.98),
    (46, "Indoor/Outdoor Games", 8, "Fan Shop", 1004, "Field & Stream Sportsman 16 Gun Fire Safe", 399.98),
    (48, "Water Sports", 7, "Outdoors", 1073, "Pelican Sunstream 100 Kayak", 199.99),
    (9, "Electronics", 3, "Technology", 191, "Garmin Forerunner 10 GPS Watch", 129.99),
    (12, "Accessories", 2, "Golf", 817, "Nike Men's Free 5.0+ Running Shoe", 99.99),
]
GEO = [
    ("Europe", "Western Europe", "France", "Paris", "Ile-de-France", 48.85, 2.35),
    ("Europe", "Northern Europe", "United Kingdom", "London", "England", 51.5, -0.12),
    ("LATAM", "Central America", "México", "Querétaro", "Querétaro", 20.59, -100.39),
    ("LATAM", "South America", "Colombia", "Bogotá", "Bogotá", 4.71, -74.07),
    ("USCA", "East of USA", "Estados Unidos", "New York City", "New York", 40.71, -74.0),
    ("USCA", "West of USA", "Estados Unidos", "Los Angeles", "California", 34.05, -118.24),
    ("Pacific Asia", "Southeast Asia", "Indonesia", "Jakarta", "Java", -6.2, 106.85),
    ("Africa", "West Africa", "Nigeria", "Lagos", "Lagos", 6.52, 3.37),
]
SHIP_MODES = {"Standard Class": 4, "Second Class": 2, "First Class": 1, "Same Day": 0}
SEGMENTS = ["Consumer", "Corporate", "Home Office"]
PAY_TYPES = ["DEBIT", "TRANSFER", "PAYMENT", "CASH"]
STATUSES = ["COMPLETE", "PENDING", "CLOSED", "PENDING_PAYMENT", "CANCELED",
            "PROCESSING", "SUSPECTED_FRAUD", "ON_HOLD", "PAYMENT_REVIEW"]


def fmt(dt):
    return f"{dt.month}/{dt.day}/{dt.year} {dt.hour}:{dt.minute:02d}"


def make_orders(n, rng):
    rows = []
    start = datetime(2015, 1, 1)
    item_id = 1
    order_id = 1000
    while len(rows) < n:
        order_id += 1
        for _ in range(rng.randint(1, 3)):
            cat_id, cat, dep_id, dep, prod_id, prod, price = rng.choice(CATALOG)
            market, region, country, city, state, lat, lon = rng.choice(GEO)
            mode = rng.choices(list(SHIP_MODES), weights=[6, 2, 1.5, 0.5])[0]
            sched = SHIP_MODES[mode]
            real = max(0, sched + rng.choice([-1, 0, 0, 1, 2, 3]))
            late = 1 if real > sched else 0
            if late:
                dstatus = "Late delivery"
            else:
                dstatus = rng.choice(["Shipping on time", "Advance shipping"])
            qty = rng.randint(1, 5)
            rate = rng.choice([0.0, 0.02, 0.05, 0.1, 0.15, 0.2, 0.25])
            disc = round(price * qty * rate, 2)
            sales = round(price * qty, 2)
            total = round(sales - disc, 2)
            profit = round(total * rng.uniform(-0.25, 0.4), 2)
            odate = start + timedelta(days=rng.randint(0, 1100), hours=rng.randint(0, 23),
                                      minutes=rng.randint(0, 59))
            sdate = odate + timedelta(days=real)
            cust = rng.randint(1, 400)
            rows.append({
                "Type": rng.choice(PAY_TYPES),
                "Days for shipping (real)": real,
                "Days for shipment (scheduled)": sched,
                "Benefit per order": profit,
                "Sales per customer": round(total * rng.uniform(1, 2), 2),
                "Delivery Status": dstatus,
                "Late_delivery_risk": late,
                "Category Id": cat_id, "Category Name": cat,
                "Customer City": city, "Customer Country": country,
                "Customer Email": "XXXXXXXXX", "Customer Fname": f"Name{cust}",
                "Customer Id": cust, "Customer Lname": "Masked",
                "Customer Password": "XXXXXXXXX",
                "Customer Segment": rng.choice(SEGMENTS),
                "Customer State": state, "Customer Street": f"{rng.randint(1, 9999)} Some St",
                "Customer Zipcode": rng.choice([rng.randint(10000, 99999), None]),
                "Department Id": dep_id, "Department Name": dep,
                "Latitude": lat, "Longitude": lon, "Market": market,
                "Order City": city, "Order Country": country,
                "Order Customer Id": cust,
                "order date (DateOrders)": fmt(odate),
                "Order Id": order_id,
                "Order Item Cardprod Id": prod_id,
                "Order Item Discount": disc, "Order Item Discount Rate": rate,
                "Order Item Id": item_id,
                "Order Item Product Price": price,
                "Order Item Profit Ratio": round(profit / total, 2) if total else 0,
                "Order Item Quantity": qty, "Sales": sales,
                "Order Item Total": total, "Order Profit Per Order": profit,
                "Order Region": region, "Order State": state,
                "Order Status": rng.choices(STATUSES, weights=[30, 15, 12, 15, 5, 10, 3, 5, 5])[0],
                "Order Zipcode": None,
                "Product Card Id": prod_id, "Product Category Id": cat_id,
                "Product Description": None,
                "Product Image": f"http://images.example.com/{prod_id}.jpg",
                "Product Name": prod, "Product Price": price, "Product Status": 0,
                "shipping date (DateOrders)": fmt(sdate),
                "Shipping Mode": mode,
            })
            item_id += 1
    return pd.DataFrame(rows[:n], columns=ORDER_COLUMNS)


def add_dirt(df, rng):
    df = df.copy()
    # duplicates of existing order items
    df = pd.concat([df, df.sample(15, random_state=1)], ignore_index=True)
    # unparseable / missing dates
    idx = rng.sample(range(len(df)), 6)
    df.loc[idx[:3], "order date (DateOrders)"] = "not a date"
    df.loc[idx[3:], "shipping date (DateOrders)"] = None
    # messy status text and padded strings
    idx = rng.sample(range(len(df)), 25)
    df.loc[idx[:10], "Order Status"] = df.loc[idx[:10], "Order Status"].str.lower()
    df.loc[idx[10:], "Shipping Mode"] = "  " + df.loc[idx[10:], "Shipping Mode"] + " "
    # impossible values
    idx = rng.sample(range(len(df)), 4)
    df.loc[idx, "Days for shipping (real)"] = -2
    # delivery status disagreeing with the risk flag
    idx = rng.sample(range(len(df)), 5)
    df.loc[idx, "Late_delivery_risk"] = 1 - df.loc[idx, "Late_delivery_risk"]
    return df


def make_logs(n, rng):
    rows = []
    depts = {}
    for _, cat, _, dep, _, prod, _ in CATALOG:
        depts.setdefault(dep, []).append((cat, prod))
    start = datetime(2017, 1, 1)
    for _ in range(n):
        dep = rng.choice(list(depts))
        cat, prod = rng.choice(depts[dep])
        kind = rng.choices(["product", "category", "department", "other"], weights=[55, 25, 15, 5])[0]
        when = start + timedelta(days=rng.randint(0, 400), hours=rng.choices(
            range(24), weights=[1, 1, 1, 1, 1, 2, 3, 4, 5, 6, 6, 6, 6, 6, 6, 6, 6, 6, 7, 8, 9, 8, 5, 2])[0],
            minutes=rng.randint(0, 59))
        d = dep.lower().replace(" ", "%20")
        c = cat.lower().replace(" ", "%20").replace("/", "%2F")
        if kind == "product":
            url = f"/department/{d}/category/{c}/product/{prod.replace(' ', '%20')}"
        elif kind == "category":
            url = f"/department/{d}/category/{c}"
        elif kind == "department":
            url = f"/department/{d}"
        else:
            url = rng.choice(["/checkout", "/cart", "/login"])
        row = {
            "Product": prod if kind == "product" else None,
            "Category": cat if kind in ("product", "category") else None,
            "Date": fmt(when),
            "Month": when.strftime("%b"),
            "Hour": when.hour,
            "Department": dep if kind != "other" else None,
            "ip": f"10.{rng.randint(0, 20)}.{rng.randint(0, 255)}.{rng.randint(1, 254)}",
            "url": url,
        }
        # simulate fields that are blank in the file but recoverable from the URL
        if rng.random() < 0.15:
            row["Department"] = None
            row["Category"] = None
            row["Product"] = None
        rows.append(row)
    return pd.DataFrame(rows, columns=["Product", "Category", "Date", "Month", "Hour",
                                       "Department", "ip", "url"])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/sample")
    ap.add_argument("--orders", type=int, default=2000)
    ap.add_argument("--logs", type=int, default=6000)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    rng = random.Random(args.seed)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    orders = add_dirt(make_orders(args.orders, rng), rng)
    # real file is Latin-1 encoded; mimic that so the encoding handling is exercised
    orders.to_csv(out / "DataCoSupplyChainDataset.csv", index=False, encoding="latin-1")
    make_logs(args.logs, rng).to_csv(out / "tokenized_access_logs.csv", index=False, encoding="latin-1")
    print(f"Wrote synthetic sample to {out}/ ({len(orders)} order rows, {args.logs} log rows)")


if __name__ == "__main__":
    main()
