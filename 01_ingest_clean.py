#!/usr/bin/env python3
"""
SupplyChainIQ - step 1: ingest, clean and reshape the DataCo files into MongoDB.

Inputs (from the Kaggle dataset "DataCo Smart Supply Chain for Big Data Analysis"):
    DataCoSupplyChainDataset.csv   -> orders   collection (nested documents)
    tokenized_access_logs.csv      -> clicks   collection (parsed clickstream events)

Typical use:
    python 01_ingest_clean.py --orders data/DataCoSupplyChainDataset.csv \
                              --logs   data/tokenized_access_logs.csv --drop

No MongoDB handy yet? Validate the cleaning first:
    python 01_ingest_clean.py --orders ... --logs ... --dry-run

Every cleaning step is counted; the counts land in out/cleaning_log.json and in the
`meta_cleaning_log` collection, ready to paste into the report.
"""
import argparse
import hashlib
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import unquote

import numpy as np
import pandas as pd

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
# Personal / useless columns removed before anything reaches the database.
PII_COLUMNS = ["customer_email", "customer_password", "customer_fname",
               "customer_lname", "customer_street"]
EMPTY_COLUMN_THRESHOLD = 0.95        # drop any column that is >95% null

# Columns (after renaming) without which the analysis cannot run.
REQUIRED_COLUMNS = ["order_item_id", "order_id", "order_date", "shipping_mode",
                    "order_region", "order_item_total", "sales"]
SOFT_TEXT_COLUMNS = ["order_status", "type", "customer_zipcode", "order_zipcode", "delivery_status"]

RENAMES = {
    "order_date_dateorders": "order_date",
    "shipping_date_dateorders": "ship_date",
    "days_for_shipping_real": "days_actual",
    "days_for_shipment_scheduled": "days_scheduled",
}

NUMERIC_COLUMNS = [
    "days_actual", "days_scheduled", "benefit_per_order", "sales_per_customer",
    "late_delivery_risk", "category_id", "customer_id", "department_id", "latitude",
    "longitude", "order_customer_id", "order_id", "order_item_cardprod_id",
    "order_item_discount", "order_item_discount_rate", "order_item_id",
    "order_item_product_price", "order_item_profit_ratio", "order_item_quantity",
    "sales", "order_item_total", "order_profit_per_order", "product_card_id",
    "product_category_id", "product_price", "product_status",
]

DATE_FORMATS = ["%m/%d/%Y %H:%M", "%m/%d/%Y"]

URL_RE = re.compile(r"/department/(?P<dep>[^/?#]+)"
                    r"(?:/category/(?P<cat>[^/?#]+))?"
                    r"(?:/product/(?P<prod>[^/?#]+))?", re.IGNORECASE)


# --------------------------------------------------------------------------- #
# Small helpers
# --------------------------------------------------------------------------- #
def snake(name: str) -> str:
    """'Days for shipping (real)' -> 'days_for_shipping_real'."""
    return re.sub(r"[^0-9a-z]+", "_", str(name).strip().lower()).strip("_")


def v(x):
    """Convert pandas/numpy scalars (incl. NaN/NaT/NA) into plain Python or None."""
    if x is None or isinstance(x, (list, dict)):
        return x
    try:
        if pd.isna(x):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(x, pd.Timestamp):
        return x.to_pydatetime()
    if isinstance(x, np.generic):
        return x.item()
    return x


def as_int(x):
    x = v(x)
    if x is None:
        return None
    try:
        return int(float(x))
    except (TypeError, ValueError):
        return None


def as_float(x, nd=2):
    x = v(x)
    if x is None:
        return None
    try:
        return round(float(x), nd)
    except (TypeError, ValueError):
        return None


def as_str(x):
    x = v(x)
    return None if x is None else str(x)


def tidy_strings(df: pd.DataFrame) -> pd.DataFrame:
    """Trim, collapse internal whitespace, turn empty strings into missing values."""
    for c in df.columns:
        s = df[c].astype("object")
        s = s.map(lambda t: re.sub(r"\s+", " ", t).strip() if isinstance(t, str) else t)
        s = s.map(lambda t: None if (isinstance(t, str) and t == "") else t)
        df[c] = s
    return df


def parse_dates(series: pd.Series) -> pd.Series:
    """Parse M/D/YYYY H:MM (the dataset's format) with a date-only fallback."""
    out = pd.to_datetime(series, format=DATE_FORMATS[0], errors="coerce")
    for fmt in DATE_FORMATS[1:]:
        todo = out.isna() & series.notna()
        if todo.any():
            out.loc[todo] = pd.to_datetime(series[todo], format=fmt, errors="coerce")
    return out


def read_csv(path: Path, encoding: str) -> pd.DataFrame:
    print(f"  reading {path} (encoding={encoding}) ...")
    return pd.read_csv(path, encoding=encoding, dtype=str, low_memory=False)


# --------------------------------------------------------------------------- #
# Orders: clean
# --------------------------------------------------------------------------- #
def clean_orders(df: pd.DataFrame, log: dict):
    log["rows_read"] = int(len(df))
    log["columns_read"] = int(df.shape[1])

    df = df.rename(columns={c: snake(c) for c in df.columns}).rename(columns=RENAMES)
    absent = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if absent:
        sys.exit(f"ERROR: expected columns not found in the orders file: {absent}\n"
                 f"Columns seen: {list(df.columns)}")
    # any other expected column that is missing is created empty (and reported)
    soft_missing = [c for c in NUMERIC_COLUMNS + ["ship_date"] + SOFT_TEXT_COLUMNS
                    if c not in df.columns]
    for c in soft_missing:
        df[c] = None
    log["expected_columns_missing_filled_null"] = soft_missing

    df = tidy_strings(df)

    # --- numeric coercion ---------------------------------------------------
    coerced = {}
    for c in NUMERIC_COLUMNS:
        before = df[c].notna()
        df[c] = pd.to_numeric(df[c], errors="coerce")
        bad = int((before & df[c].isna()).sum())
        if bad:
            coerced[c] = bad
    log["non_numeric_values_set_to_null"] = coerced

    # --- dates --------------------------------------------------------------
    raw_od, raw_sd = df["order_date"], df["ship_date"]
    df["order_date"] = parse_dates(raw_od)
    df["ship_date"] = parse_dates(raw_sd)
    log["unparseable_dates"] = {
        "order_date": int((raw_od.notna() & df["order_date"].isna()).sum()),
        "ship_date": int((raw_sd.notna() & df["ship_date"].isna()).sum()),
    }

    # --- rows we cannot use at all ------------------------------------------
    key_missing = df["order_item_id"].isna() | df["order_id"].isna()
    log["rows_removed_missing_id"] = int(key_missing.sum())
    df = df[~key_missing]

    dup = df.duplicated(subset="order_item_id", keep="first")
    log["rows_removed_duplicate_order_item_id"] = int(dup.sum())
    df = df[~dup].reset_index(drop=True)

    # --- standardise categorical text --------------------------------------
    df["order_status"] = df["order_status"].map(lambda t: t.upper().replace(" ", "_") if isinstance(t, str) else t)
    df["type"] = df["type"].map(lambda t: t.upper() if isinstance(t, str) else t)
    for c in ("customer_zipcode", "order_zipcode"):
        df[c] = df[c].map(lambda t: re.sub(r"\.0$", "", str(t)) if pd.notna(t) else None)

    # --- impossible values -> null (and flagged below) -----------------------
    neg_days = (df["days_actual"] < 0) | (df["days_scheduled"] < 0)
    flags = {}
    flags["negative_shipping_days"] = neg_days
    df.loc[df["days_actual"] < 0, "days_actual"] = np.nan
    df.loc[df["days_scheduled"] < 0, "days_scheduled"] = np.nan

    flags["bad_order_date"] = df["order_date"].isna()
    flags["bad_ship_date"] = df["ship_date"].isna()
    flags["ship_before_order"] = (df["ship_date"] < df["order_date"]).fillna(False)
    flags["non_positive_quantity"] = (df["order_item_quantity"] <= 0).fillna(False)
    flags["discount_rate_out_of_range"] = (
        (df["order_item_discount_rate"] < 0) | (df["order_item_discount_rate"] > 1)).fillna(False)

    ds = df["delivery_status"].astype("object").map(lambda t: t.lower() if isinstance(t, str) else t)
    says_late = ds.eq("late delivery")
    risk = df["late_delivery_risk"]
    flags["status_risk_mismatch"] = (
        ((says_late & risk.eq(0)) | (~says_late & ds.notna() & risk.eq(1))).fillna(False))

    log["rows_flagged_by_check"] = {k: int(m.sum()) for k, m in flags.items()}
    log["invalid_values_set_to_null"] = {"negative_shipping_days": int(neg_days.sum())}

    row_flags = [[] for _ in range(len(df))]
    for name, mask in flags.items():
        for i in np.flatnonzero(mask.to_numpy()):
            row_flags[i].append(name)
    df["_flags"] = row_flags

    # --- drop PII and near-empty columns -------------------------------------
    dropped_pii = [c for c in PII_COLUMNS if c in df.columns]
    df = df.drop(columns=dropped_pii)
    null_share = df.isna().mean()
    mostly_empty = [c for c in null_share.index
                    if null_share[c] > EMPTY_COLUMN_THRESHOLD and c not in ("_flags",)]
    # keep dates/derived inputs even if sparse; drop everything else that is empty
    mostly_empty = [c for c in mostly_empty if c not in ("order_date", "ship_date")]
    df = df.drop(columns=mostly_empty)
    log["columns_dropped_pii"] = dropped_pii
    log["columns_dropped_mostly_null"] = {c: round(float(null_share[c]), 4) for c in mostly_empty}

    # --- derived fields -------------------------------------------------------
    df["delay_days"] = df["days_actual"] - df["days_scheduled"]
    log["rows_loaded"] = int(len(df))
    kept_nulls = df.drop(columns=["_flags"]).isna().sum()
    log["null_counts_after_cleaning"] = {c: int(n) for c, n in kept_nulls.items() if n}
    return df


def order_docs(df: pd.DataFrame):
    """Yield one nested document per order line item."""
    cols = set(df.columns)

    def g(row, key, conv=as_str):
        return conv(row[key]) if key in cols else None

    for row in df.to_dict("records"):
        lat, lon = as_float(row.get("latitude"), 6), as_float(row.get("longitude"), 6)
        geo = None
        if lat is not None and lon is not None and -90 <= lat <= 90 and -180 <= lon <= 180:
            geo = {"type": "Point", "coordinates": [lon, lat]}
        order_date = v(row["order_date"])
        profit = as_float(row.get("order_profit_per_order"))
        yield {
            "_id": as_int(row["order_item_id"]),
            "order_id": as_int(row["order_id"]),
            "order_date": order_date,
            "order_status": g(row, "order_status"),
            "payment_type": g(row, "type"),
            "customer": {
                "id": g(row, "customer_id", as_int),
                "segment": g(row, "customer_segment"),
                "city": g(row, "customer_city"),
                "state": g(row, "customer_state"),
                "country": g(row, "customer_country"),
                "zipcode": g(row, "customer_zipcode"),
            },
            "product": {
                "id": g(row, "product_card_id", as_int),
                "name": g(row, "product_name"),
                "price": g(row, "product_price", as_float),
                "status": g(row, "product_status", as_int),
                "image": g(row, "product_image"),
                "category_id": g(row, "category_id", as_int),
                "category_name": g(row, "category_name"),
                "department_id": g(row, "department_id", as_int),
                "department_name": g(row, "department_name"),
            },
            "shipping": {
                "mode": g(row, "shipping_mode"),
                "delivery_status": g(row, "delivery_status"),
                "late_risk": g(row, "late_delivery_risk", as_int),
                "days_actual": g(row, "days_actual", as_int),
                "days_scheduled": g(row, "days_scheduled", as_int),
                "delay_days": g(row, "delay_days", as_int),
                "ship_date": v(row["ship_date"]),
            },
            "order_location": {
                "market": g(row, "market"),
                "region": g(row, "order_region"),
                "country": g(row, "order_country"),
                "state": g(row, "order_state"),
                "city": g(row, "order_city"),
            },
            "geo": geo,   # coordinates exactly as supplied in the dataset
            "financials": {
                "unit_price": g(row, "order_item_product_price", as_float),
                "quantity": g(row, "order_item_quantity", as_int),
                "sales": g(row, "sales", as_float),
                "discount": g(row, "order_item_discount", as_float),
                "discount_rate": g(row, "order_item_discount_rate", lambda x: as_float(x, 4)),
                "item_total": g(row, "order_item_total", as_float),
                "profit_ratio": g(row, "order_item_profit_ratio", lambda x: as_float(x, 4)),
                "profit_per_order": profit,
                "benefit_per_order": g(row, "benefit_per_order", as_float),
                "sales_per_customer": g(row, "sales_per_customer", as_float),
            },
            "derived": {
                "order_month": order_date.strftime("%Y-%m") if order_date else None,
                "is_loss": (profit < 0) if profit is not None else None,
            },
            "dq": {"flags": row["_flags"]},
        }


# --------------------------------------------------------------------------- #
# Clickstream: clean
# --------------------------------------------------------------------------- #
def clean_logs(df: pd.DataFrame, log: dict, salt: str):
    log["rows_read"] = int(len(df))
    df = df.rename(columns={c: snake(c) for c in df.columns})
    for c in ["product", "category", "date", "month", "hour", "department", "ip", "url"]:
        if c not in df.columns:
            df[c] = None
    df = tidy_strings(df)

    raw_dates = df["date"]
    df["ts"] = parse_dates(raw_dates)
    log["unparseable_dates"] = int((raw_dates.notna() & df["ts"].isna()).sum())

    hour = pd.to_numeric(df["hour"], errors="coerce")
    hour = hour.where((hour >= 0) & (hour <= 23))
    log["hour_filled_from_timestamp"] = int((hour.isna() & df["ts"].notna()).sum())
    df["hour"] = hour.fillna(df["ts"].dt.hour)

    # decode the URL and recover department / category / product when the columns are blank
    df["path"] = df["url"].map(lambda u: unquote(u) if isinstance(u, str) else None)
    parsed = df["url"].map(lambda u: URL_RE.search(u) if isinstance(u, str) else None)
    filled = {"department": 0, "category": 0, "product": 0}
    for col, grp in (("department", "dep"), ("category", "cat"), ("product", "prod")):
        from_url = parsed.map(lambda m, g=grp: unquote(m.group(g)) if m and m.group(g) else None)
        need = df[col].isna() & from_url.notna()
        df.loc[need, col] = from_url[need]
        filled[col] = int(need.sum())
    log["fields_recovered_from_url"] = filled

    df["event_type"] = np.select(
        [df["product"].notna(), df["category"].notna(), df["department"].notna()],
        ["product_view", "category_view", "department_view"], default="other")
    log["event_type_counts"] = {k: int(n) for k, n in df["event_type"].value_counts().items()}

    # pseudonymise the visitor IP (salted hash) - the raw address is never stored
    def pseudo(ip):
        if not isinstance(ip, str):
            return None
        return hashlib.sha256((salt + ip).encode()).hexdigest()[:12]
    df["visitor_id"] = df["ip"].map(pseudo)
    log["rows_without_ip"] = int(df["visitor_id"].isna().sum())
    log["exact_duplicate_rows_kept"] = int(df.duplicated(
        subset=["ts", "visitor_id", "path"]).sum())     # repeat views are legitimate events
    df["dow"] = df["ts"].dt.dayofweek
    df["month"] = df["month"].where(df["month"].notna(), df["ts"].dt.strftime("%b"))
    log["rows_loaded"] = int(len(df))
    return df


def click_docs(df: pd.DataFrame):
    for row in df.to_dict("records"):
        yield {
            "ts": v(row["ts"]),
            "hour": as_int(row["hour"]),
            "dow": as_int(row["dow"]),
            "month": as_str(row["month"]),
            "event_type": row["event_type"],
            "department": as_str(row["department"]),
            "category": as_str(row["category"]),
            "product": as_str(row["product"]),
            "visitor_id": as_str(row["visitor_id"]),
            "path": as_str(row["path"]),
        }


# --------------------------------------------------------------------------- #
# Output
# --------------------------------------------------------------------------- #
def jdefault(o):
    if isinstance(o, datetime):
        return o.isoformat()
    raise TypeError(f"not serialisable: {type(o)}")


def write_ndjson(path: Path, docs, limit):
    n = 0
    with open(path, "w", encoding="utf-8") as fh:
        for d in docs:
            fh.write(json.dumps(d, default=jdefault, ensure_ascii=False) + "\n")
            n += 1
            if limit and n >= limit:
                break
    print(f"  wrote {n:,} documents -> {path}")


def load_mongo(coll, docs, batch_size):
    batch, total = [], 0
    for d in docs:
        batch.append(d)
        if len(batch) >= batch_size:
            coll.insert_many(batch, ordered=False)
            total += len(batch)
            batch = []
            print(f"    {coll.name}: {total:,} inserted", end="\r")
    if batch:
        coll.insert_many(batch, ordered=False)
        total += len(batch)
    print(f"    {coll.name}: {total:,} inserted")
    return total


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--orders", help="path to DataCoSupplyChainDataset.csv")
    ap.add_argument("--logs", help="path to tokenized_access_logs.csv")
    ap.add_argument("--mongo-uri", default="mongodb://localhost:27017")
    ap.add_argument("--db", default="supplychainiq")
    ap.add_argument("--drop", action="store_true", help="drop target collections first")
    ap.add_argument("--dry-run", action="store_true", help="clean only; write NDJSON, skip MongoDB")
    ap.add_argument("--dry-run-limit", type=int, default=0, help="max documents per NDJSON file (0 = all)")
    ap.add_argument("--out-dir", default="out")
    ap.add_argument("--encoding", default="latin-1", help="the Kaggle CSVs are ISO-8859-1, not UTF-8")
    ap.add_argument("--batch-size", type=int, default=10_000)
    ap.add_argument("--ip-salt", default=os.environ.get("SCIQ_IP_SALT", "supplychainiq"))
    args = ap.parse_args()

    if not args.orders and not args.logs:
        ap.error("give at least one of --orders / --logs")

    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    run_log = {"run_at": datetime.now(timezone.utc), "pandas": pd.__version__}

    order_docs_iter = click_docs_iter = None
    if args.orders:
        print("[orders]")
        olog = {}
        odf = clean_orders(read_csv(Path(args.orders), args.encoding), olog)
        run_log["orders"] = olog
        order_docs_iter = lambda: order_docs(odf)     # noqa: E731
    if args.logs:
        print("[clicks]")
        llog = {}
        ldf = clean_logs(read_csv(Path(args.logs), args.encoding), llog, args.ip_salt)
        run_log["clicks"] = llog
        click_docs_iter = lambda: click_docs(ldf)     # noqa: E731

    with open(out / "cleaning_log.json", "w", encoding="utf-8") as fh:
        json.dump(run_log, fh, indent=2, default=jdefault, ensure_ascii=False)
    print(f"\ncleaning log -> {out / 'cleaning_log.json'}")

    if args.dry_run:
        print("[dry run] writing NDJSON instead of loading MongoDB")
        if order_docs_iter:
            write_ndjson(out / "orders.ndjson", order_docs_iter(), args.dry_run_limit)
        if click_docs_iter:
            write_ndjson(out / "clicks.ndjson", click_docs_iter(), args.dry_run_limit)
        return

    try:
        from pymongo import ASCENDING, GEOSPHERE, MongoClient
    except ImportError:
        sys.exit("pymongo is not installed: pip install -r requirements.txt")
    client = MongoClient(args.mongo_uri, serverSelectionTimeoutMS=5000)
    try:
        client.admin.command("ping")
    except Exception as exc:  # noqa: BLE001
        sys.exit(f"Cannot reach MongoDB at {args.mongo_uri}: {exc}\n"
                 f"Start mongod, or use --dry-run to only validate the cleaning.")
    db = client[args.db]
    if order_docs_iter:
        if args.drop:
            db.orders.drop()
        print("[load] orders")
        load_mongo(db.orders, order_docs_iter(), args.batch_size)
        for spec in ([("order_date", ASCENDING)], [("shipping.mode", ASCENDING)],
                     [("order_location.region", ASCENDING)], [("product.category_name", ASCENDING)],
                     [("financials.discount_rate", ASCENDING)], [("order_status", ASCENDING)]):
            db.orders.create_index(spec)
        db.orders.create_index([("geo", GEOSPHERE)])
    if click_docs_iter:
        if args.drop:
            db.clicks.drop()
        print("[load] clicks")
        load_mongo(db.clicks, click_docs_iter(), args.batch_size)
        for spec in ([("hour", ASCENDING)], [("department", ASCENDING)], [("event_type", ASCENDING)]):
            db.clicks.create_index(spec)
    db.meta_cleaning_log.insert_one(json.loads(json.dumps(run_log, default=jdefault)))
    print("\nDone. Collections:", ", ".join(sorted(db.list_collection_names())))


if __name__ == "__main__":
    main()
