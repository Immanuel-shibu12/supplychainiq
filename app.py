#!/usr/bin/env python3
"""
SupplyChainIQ - Interactive Full-Stack Backend Application
Connects live to MongoDB (supplychainiq database) and exposes real-time query,
search, delivery-risk prediction, and clickstream analytics APIs.
"""
import math
import os
import re
import time
from datetime import datetime
from pathlib import Path
from flask import Flask, jsonify, render_template, request, send_from_directory
from pymongo import MongoClient, ASCENDING, DESCENDING

app = Flask(__name__, static_folder="static", template_folder="templates")
app.config["TEMPLATES_AUTO_RELOAD"] = True
app.config["SEND_FILE_MAX_AGE_DEFAULT"] = 0
app.jinja_env.auto_reload = True

# Connect to MongoDB
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.environ.get("MONGO_DB", "supplychainiq")
client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=4000)
db = client[DB_NAME]


def serialize_doc(doc):
    """Recursively convert datetime and non-JSON types for JSON serialization."""
    if not isinstance(doc, dict):
        return doc
    clean = {}
    for k, v in doc.items():
        if isinstance(v, datetime):
            clean[k] = v.isoformat()
        elif isinstance(v, dict):
            clean[k] = serialize_doc(v)
        elif isinstance(v, list):
            clean[k] = [serialize_doc(i) if isinstance(i, dict) else (i.isoformat() if isinstance(i, datetime) else i) for i in v]
        elif isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            clean[k] = None
        else:
            clean[k] = v
    return clean


# -----------------------------------------------------------------------------
# Frontend Routes
# -----------------------------------------------------------------------------
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/results/<path:filename>")
def serve_results(filename):
    return send_from_directory("results", filename)


# -----------------------------------------------------------------------------
# API: Metadata & Distinct Filter Values
# -----------------------------------------------------------------------------
@app.route("/api/metadata", methods=["GET"])
def get_metadata():
    try:
        orders_count = db.orders.count_documents({})
        clicks_count = db.clicks.count_documents({})
        regions = sorted([r for r in db.orders.distinct("order_location.region") if r])
        modes = sorted([m for m in db.orders.distinct("shipping.mode") if m])
        categories = sorted([c for c in db.orders.distinct("product.category_name") if c])
        statuses = sorted([s for s in db.orders.distinct("order_status") if s])
        departments = sorted([d for d in db.clicks.distinct("department") if d])

        return jsonify({
            "status": "connected",
            "database": DB_NAME,
            "orders_count": orders_count,
            "clicks_count": clicks_count,
            "regions": regions,
            "modes": modes,
            "categories": categories,
            "statuses": statuses,
            "departments": departments,
        })
    except Exception as exc:
        return jsonify({"status": "error", "message": str(exc)}), 500


# -----------------------------------------------------------------------------
# API: Live Interactive Search on orders collection
# -----------------------------------------------------------------------------
@app.route("/api/orders/search", methods=["GET"])
def search_orders():
    started = time.time()
    query_text = request.args.get("q", "").strip()
    region = request.args.get("region", "").strip()
    shipping_mode = request.args.get("mode", "").strip()
    status = request.args.get("status", "").strip()
    late_only = request.args.get("late_only", "false").lower() == "true"
    min_price = request.args.get("min_price", type=float)
    max_price = request.args.get("max_price", type=float)
    page = max(1, request.args.get("page", 1, type=int))
    limit = min(50, max(5, request.args.get("limit", 15, type=int)))

    filter_dict = {}

    if region:
        filter_dict["order_location.region"] = region

    if shipping_mode:
        filter_dict["shipping.mode"] = shipping_mode

    if status:
        filter_dict["order_status"] = status

    if late_only:
        filter_dict["shipping.late_risk"] = 1

    if min_price is not None or max_price is not None:
        price_filter = {}
        if min_price is not None:
            price_filter["$gte"] = min_price
        if max_price is not None:
            price_filter["$lte"] = max_price
        filter_dict["financials.item_total"] = price_filter

    if query_text:
        # Check if query is an exact integer ID
        if query_text.isdigit():
            val = int(query_text)
            filter_dict["$or"] = [{"_id": val}, {"order_id": val}, {"customer.id": val}]
        else:
            # Case-insensitive substring regex on product name or customer city
            safe_rgx = re.escape(query_text)
            filter_dict["$or"] = [
                {"product.name": {"$regex": safe_rgx, "$options": "i"}},
                {"customer.city": {"$regex": safe_rgx, "$options": "i"}},
                {"product.category_name": {"$regex": safe_rgx, "$options": "i"}}
            ]

    total_count = db.orders.count_documents(filter_dict)
    cursor = (
        db.orders.find(filter_dict)
        .sort([("_id", DESCENDING)])
        .skip((page - 1) * limit)
        .limit(limit)
    )

    results = [serialize_doc(doc) for doc in cursor]
    elapsed_ms = round((time.time() - started) * 1000, 2)

    return jsonify({
        "success": True,
        "query_time_ms": elapsed_ms,
        "total_results": total_count,
        "page": page,
        "limit": limit,
        "total_pages": math.ceil(total_count / limit) if limit else 1,
        "results": results
    })


# -----------------------------------------------------------------------------
# API: Delivery Risk & Profit Simulator (Dynamic Prediction Engine)
# -----------------------------------------------------------------------------
@app.route("/api/simulator/predict", methods=["POST"])
def simulate_order():
    started = time.time()
    data = request.get_json() or {}

    shipping_mode = data.get("shipping_mode", "Standard Class")
    region = data.get("region", "Western Europe")
    category = data.get("category", "")
    unit_price = float(data.get("unit_price", 100.0))
    quantity = int(data.get("quantity", 1))
    discount_pct = float(data.get("discount_pct", 0.0))

    # Match criteria for historical analysis
    match_query = {
        "shipping.mode": shipping_mode,
        "order_location.region": region,
    }
    if category:
        match_query["product.category_name"] = category

    # Run aggregation on live historical database
    pipeline = [
        {"$match": match_query},
        {"$group": {
            "_id": None,
            "sample_size": {"$sum": 1},
            "late_count": {"$sum": "$shipping.late_risk"},
            "avg_delay_days": {"$avg": "$shipping.delay_days"},
            "avg_profit_ratio": {"$avg": "$financials.profit_ratio"},
            "loss_count": {"$sum": {"$cond": [{"$eq": ["$derived.is_loss", True]}, 1, 0]}}
        }}
    ]

    agg_res = list(db.orders.aggregate(pipeline))
    if not agg_res or agg_res[0].get("sample_size", 0) < 5:
        # Fallback to shipping_mode alone if sample is too narrow
        fallback_pipeline = [
            {"$match": {"shipping.mode": shipping_mode}},
            {"$group": {
                "_id": None,
                "sample_size": {"$sum": 1},
                "late_count": {"$sum": "$shipping.late_risk"},
                "avg_delay_days": {"$avg": "$shipping.delay_days"},
                "avg_profit_ratio": {"$avg": "$financials.profit_ratio"},
                "loss_count": {"$sum": {"$cond": [{"$eq": ["$derived.is_loss", True]}, 1, 0]}}
            }}
        ]
        agg_res = list(db.orders.aggregate(fallback_pipeline))

    stats = agg_res[0] if agg_res else {
        "sample_size": 0, "late_count": 0, "avg_delay_days": 0.0, "avg_profit_ratio": 0.12, "loss_count": 0
    }

    sample_size = stats.get("sample_size", 1)
    late_prob_pct = round((stats.get("late_count", 0) / sample_size) * 100, 1)
    avg_delay = round(stats.get("avg_delay_days") or 0.0, 2)
    base_margin = float(stats.get("avg_profit_ratio") or 0.12)

    # Financial projections
    gross_sales = unit_price * quantity
    discount_amount = gross_sales * (discount_pct / 100.0)
    net_revenue = gross_sales - discount_amount
    # Estimated baseline cost without promotional discount
    estimated_cost = gross_sales * (1.0 - base_margin)
    projected_profit = round(net_revenue - estimated_cost, 2)
    projected_margin_pct = round((projected_profit / net_revenue * 100), 2) if net_revenue > 0 else 0.0
    is_loss = projected_profit < 0

    # Risk level classification
    if late_prob_pct >= 75:
        risk_level = "CRITICAL"
        risk_color = "#f87171"
    elif late_prob_pct >= 45:
        risk_level = "MODERATE"
        risk_color = "#fbbf24"
    else:
        risk_level = "LOW"
        risk_color = "#34d399"

    # Actionable smart recommendations
    recommendations = []
    if shipping_mode == "First Class":
        recommendations.append("First Class has a 95%+ historical late rate due to narrow 1-day promise. Advise customer of a 2-day delivery buffer.")
    elif shipping_mode == "Same Day" and late_prob_pct > 40:
        recommendations.append("Same Day orders to this region experience elevated volatility; ensure local warehouse fulfillment.")

    if discount_pct >= 20:
        recommendations.append(f"Caution: {discount_pct}% discount triggers high probability of margin erosion (projected margin: {projected_margin_pct}%).")
    elif is_loss:
        recommendations.append("Warning: This order is projected to operate at a net financial loss! Reduce promotional discount.")
    else:
        recommendations.append(f"Sustainable pricing: Projected net profit is ₹{projected_profit:,.2f} ({projected_margin_pct}% margin).")

    elapsed_ms = round((time.time() - started) * 1000, 2)

    return jsonify({
        "success": True,
        "query_time_ms": elapsed_ms,
        "inputs": {
            "shipping_mode": shipping_mode,
            "region": region,
            "category": category or "All Categories",
            "unit_price": unit_price,
            "quantity": quantity,
            "discount_pct": discount_pct
        },
        "historical_sample_size": sample_size,
        "late_probability_pct": late_prob_pct,
        "risk_level": risk_level,
        "risk_color": risk_color,
        "avg_delay_days": avg_delay,
        "financials": {
            "gross_sales": round(gross_sales, 2),
            "discount_amount": round(discount_amount, 2),
            "net_revenue": round(net_revenue, 2),
            "estimated_cost": round(estimated_cost, 2),
            "projected_profit": projected_profit,
            "projected_margin_pct": projected_margin_pct,
            "is_loss": is_loss
        },
        "recommendations": recommendations
    })


# -----------------------------------------------------------------------------
# API: Live Clickstream Traffic Analyzer
# -----------------------------------------------------------------------------
@app.route("/api/clickstream/analyze", methods=["GET"])
def analyze_clickstream():
    started = time.time()
    department = request.args.get("department", "").strip().lower()
    product_query = request.args.get("product", "").strip()

    filter_dict = {}
    if department:
        filter_dict["department"] = department
    if product_query:
        filter_dict["product"] = {"$regex": re.escape(product_query), "$options": "i"}

    # 1. Total hits
    total_hits = db.clicks.count_documents(filter_dict)

    # 2. Hourly distribution (0-23)
    hourly_pipeline = [
        {"$match": filter_dict},
        {"$group": {"_id": "$hour", "count": {"$sum": 1}}},
        {"$sort": {"_id": ASCENDING}}
    ]
    hourly_res = list(db.clicks.aggregate(hourly_pipeline))
    hours_map = {h: 0 for h in range(24)}
    for r in hourly_res:
        if r["_id"] is not None and 0 <= r["_id"] <= 23:
            hours_map[r["_id"]] = r["count"]

    # 3. Top viewed products
    product_pipeline = [
        {"$match": filter_dict},
        {"$match": {"product": {"$ne": None}}},
        {"$group": {"_id": "$product", "hits": {"$sum": 1}}},
        {"$sort": {"hits": DESCENDING}},
        {"$limit": 6}
    ]
    top_products = [{"product": r["_id"], "hits": r["hits"]} for r in db.clicks.aggregate(product_pipeline)]

    elapsed_ms = round((time.time() - started) * 1000, 2)

    return jsonify({
        "success": True,
        "query_time_ms": elapsed_ms,
        "department": department or "all",
        "total_hits": total_hits,
        "hourly_distribution": [hours_map[h] for h in range(24)],
        "top_products": top_products
    })


# -----------------------------------------------------------------------------
# API: Live MongoDB Aggregation Query Studio
# -----------------------------------------------------------------------------
@app.route("/api/analytics/preset", methods=["POST"])
def run_preset_query():
    started = time.time()
    data = request.get_json() or {}
    preset = data.get("preset", "top_loss_products")

    pipeline = []
    description = ""

    if preset == "top_loss_products":
        description = "Products with greatest total negative profit (margin destroyers)"
        pipeline = [
            {"$match": {"derived.is_loss": True}},
            {"$group": {
                "_id": "$product.name",
                "category": {"$first": "$product.category_name"},
                "loss_orders": {"$sum": 1},
                "total_loss": {"$sum": "$financials.profit_per_order"}
            }},
            {"$sort": {"total_loss": ASCENDING}},
            {"$limit": 10}
        ]
    elif preset == "carrier_performance":
        description = "Shipping mode logistics performance & delay days"
        pipeline = [
            {"$group": {
                "_id": "$shipping.mode",
                "total_shipments": {"$sum": 1},
                "late_shipments": {"$sum": "$shipping.late_risk"},
                "avg_delay": {"$avg": "$shipping.delay_days"},
                "cancellations": {"$sum": {"$cond": [{"$eq": ["$shipping.delivery_status", "Shipping canceled"]}, 1, 0]}}
            }},
            {"$addFields": {
                "late_pct": {"$round": [{"$multiply": [{"$divide": ["$late_shipments", "$total_shipments"]}, 100]}, 2]},
                "avg_delay": {"$round": ["$avg_delay", 2]}
            }},
            {"$sort": {"late_pct": DESCENDING}}
        ]
    elif preset == "high_value_regions":
        description = "Regional performance for orders exceeding ₹500 total"
        pipeline = [
            {"$match": {"financials.item_total": {"$gte": 500}}},
            {"$group": {
                "_id": "$order_location.region",
                "high_val_count": {"$sum": 1},
                "total_revenue": {"$sum": "$financials.item_total"},
                "total_profit": {"$sum": "$financials.profit_per_order"}
            }},
            {"$sort": {"total_revenue": DESCENDING}},
            {"$limit": 10}
        ]
    elif preset == "payment_risk":
        description = "Disputed or canceled orders by payment method"
        pipeline = [
            {"$group": {
                "_id": "$payment_type",
                "total": {"$sum": 1},
                "suspected_fraud": {"$sum": {"$cond": [{"$eq": ["$order_status", "SUSPECTED_FRAUD"]}, 1, 0]}},
                "canceled": {"$sum": {"$cond": [{"$eq": ["$order_status", "CANCELED"]}, 1, 0]}}
            }},
            {"$sort": {"total": DESCENDING}}
        ]
    else:
        return jsonify({"error": "Unknown preset"}), 400

    results = list(db.orders.aggregate(pipeline))
    elapsed_ms = round((time.time() - started) * 1000, 2)

    return jsonify({
        "success": True,
        "preset": preset,
        "description": description,
        "query_time_ms": elapsed_ms,
        "count": len(results),
        "results": [serialize_doc(r) for r in results]
    })


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5050))
    print(f">> SupplyChainIQ Backend Server starting on http://localhost:{port}")
    app.run(host="0.0.0.0", port=port, debug=False)
