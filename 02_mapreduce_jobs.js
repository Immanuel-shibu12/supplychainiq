/*
 * SupplyChainIQ - step 2: MapReduce analysis jobs.
 *
 * Run (after 01_ingest_clean.py has loaded the data):
 *     mongosh "mongodb://localhost:27017/supplychainiq" --file 02_mapreduce_jobs.js
 *
 * Every job writes its result to a collection named mr_<something> (documents look like
 * {_id: <key>, value: {...}}) and its run time to meta_mr_timings.
 *
 * Notes
 *  - map / reduce / finalize are shipped to the server as source text, so they must be
 *    self-contained: no references to variables defined outside the function body.
 *  - reduce may be called several times on partial results and its output is fed back into
 *    reduce. That is why every value has the same shape and reduce simply sums fields.
 *  - db.collection.mapReduce() is deprecated since MongoDB 5.0 (still works); the equivalent
 *    aggregation pipelines in 03_validate_aggregation.js are used to cross-check the results.
 */

// ---- settings ------------------------------------------------------------------------------
// Revenue/profit jobs ignore orders that never became real sales.
const EXCLUDED_STATUSES = ["CANCELED", "SUSPECTED_FRAUD"];
const REVENUE_QUERY = { order_status: { $nin: EXCLUDED_STATUSES } };
// Leave empty to run every job, or list job names, e.g. ["mr_late_by_mode"].
const ONLY = [];

// ---- one generic reduce: add up every numeric field -----------------------------------------
const sumReduce = function (key, values) {
  var r = {};
  values.forEach(function (v) {
    for (var k in v) { r[k] = (r[k] || 0) + v[k]; }
  });
  return r;
};

// ---- job definitions ---------------------------------------------------------------------------
const JOBS = [
  // ===================== ORDERS =====================
  {
    name: "mr_late_by_mode", coll: "orders", query: {},
    question: "Which shipping modes miss their delivery promise?",
    map: function () {
      var s = this.shipping;
      if (!s || !s.mode) return;
      emit(s.mode, {
        n: 1,
        late: s.late_risk || 0,
        slip: s.delay_days || 0,
        shipping_canceled: s.delivery_status === "Shipping canceled" ? 1 : 0
      });
    },
    finalize: function (key, r) {
      r.late_rate_pct = Math.round(10000 * r.late / r.n) / 100;
      r.avg_delay_days = Math.round(100 * r.slip / r.n) / 100;
      return r;
    }
  },
  {
    name: "mr_delivery_status_by_region", coll: "orders", query: {},
    question: "How are delivery outcomes distributed in each region?",
    map: function () {
      var l = this.order_location, s = this.shipping;
      if (!l || !s) return;
      emit((l.region || "Unknown") + " | " + (s.delivery_status || "Unknown"), { n: 1 });
    }
  },
  {
    name: "mr_sales_by_region", coll: "orders", query: REVENUE_QUERY,
    question: "Where is revenue made, and where does profit leak?",
    map: function () {
      var l = this.order_location, f = this.financials;
      if (!l || !f) return;
      emit((l.market || "Unknown") + " | " + (l.region || "Unknown"), {
        n: 1,
        sales: f.sales || 0,
        revenue: f.item_total || 0,
        profit: f.profit_per_order || 0,
        loss_items: (this.derived && this.derived.is_loss) ? 1 : 0
      });
    },
    finalize: function (key, r) {
      r.profit_margin_pct = r.revenue > 0 ? Math.round(10000 * r.profit / r.revenue) / 100 : null;
      r.loss_item_pct = Math.round(10000 * r.loss_items / r.n) / 100;
      return r;
    }
  },
  {
    name: "mr_sales_by_department", coll: "orders", query: REVENUE_QUERY,
    question: "Which departments earn the most (used later to compare with browsing)?",
    map: function () {
      var p = this.product, f = this.financials;
      if (!p || !f) return;
      emit((p.department_name || "Unknown").toLowerCase(), {
        n: 1,
        revenue: f.item_total || 0,
        profit: f.profit_per_order || 0
      });
    },
    finalize: function (key, r) {
      r.profit_margin_pct = r.revenue > 0 ? Math.round(10000 * r.profit / r.revenue) / 100 : null;
      return r;
    }
  },
  {
    name: "mr_monthly_trend", coll: "orders", query: REVENUE_QUERY,
    question: "How do sales and profit move month by month (seasonality)?",
    map: function () {
      var d = this.order_date, f = this.financials;
      if (!d || !f) return;
      var key = d.getUTCFullYear() + "-" + ("0" + (d.getUTCMonth() + 1)).slice(-2);
      emit(key, { n: 1, qty: f.quantity || 0, revenue: f.item_total || 0, profit: f.profit_per_order || 0 });
    },
    finalize: function (key, r) {
      r.avg_item_value = Math.round(100 * r.revenue / r.n) / 100;
      return r;
    }
  },
  {
    name: "mr_discount_impact", coll: "orders", query: REVENUE_QUERY,
    question: "Does deeper discounting erode profit?",
    map: function () {
      var f = this.financials;
      if (!f || f.discount_rate === null || f.discount_rate === undefined) return;
      var pct = Math.floor(f.discount_rate * 100 + 0.5);        // whole percent
      var lo = Math.floor(pct / 5) * 5;                            // 5-point buckets
      emit(lo < 10 ? "0" + lo : "" + lo, {                         // key = bucket lower bound
        n: 1,
        revenue: f.item_total || 0,
        profit: f.profit_per_order || 0,
        ratio_sum: f.profit_ratio || 0,
        loss_items: (this.derived && this.derived.is_loss) ? 1 : 0
      });
    },
    finalize: function (key, r) {
      r.avg_profit_per_item = Math.round(100 * r.profit / r.n) / 100;
      r.avg_profit_ratio_pct = Math.round(10000 * r.ratio_sum / r.n) / 100;
      r.loss_item_pct = Math.round(10000 * r.loss_items / r.n) / 100;
      return r;
    }
  },
  {
    name: "mr_product_performance", coll: "orders", query: REVENUE_QUERY,
    question: "Which products drive profit, and which destroy it?",
    map: function () {
      var p = this.product, f = this.financials;
      if (!p || !f) return;
      emit((p.category_name || "Unknown") + " | " + (p.name || "Unknown"), {
        n: 1,
        qty: f.quantity || 0,
        revenue: f.item_total || 0,
        profit: f.profit_per_order || 0,
        loss_items: (this.derived && this.derived.is_loss) ? 1 : 0
      });
    },
    finalize: function (key, r) {
      r.profit_margin_pct = r.revenue > 0 ? Math.round(10000 * r.profit / r.revenue) / 100 : null;
      return r;
    }
  },
  {
    name: "mr_segment_payment", coll: "orders", query: REVENUE_QUERY,
    question: "How do customer segments and payment types differ?",
    map: function () {
      var c = this.customer, f = this.financials;
      if (!c || !f) return;
      emit((c.segment || "Unknown") + " | " + (this.payment_type || "Unknown"), {
        n: 1,
        revenue: f.item_total || 0,
        profit: f.profit_per_order || 0
      });
    },
    finalize: function (key, r) {
      r.avg_item_value = Math.round(100 * r.revenue / r.n) / 100;
      return r;
    }
  },

  // ===================== CLICKSTREAM =====================
  {
    name: "mr_clicks_by_hour", coll: "clicks", query: {},
    question: "At which hours of the day do customers browse?",
    map: function () {
      if (this.hour === null || this.hour === undefined) return;
      emit(this.hour, { hits: 1, product_views: this.event_type === "product_view" ? 1 : 0 });
    }
  },
  {
    name: "mr_clicks_by_dow", coll: "clicks", query: {},
    question: "Which weekdays are busiest (0 = Monday)?",
    map: function () {
      if (this.dow === null || this.dow === undefined) return;
      emit(this.dow, { hits: 1 });
    }
  },
  {
    name: "mr_clicks_by_department", coll: "clicks", query: {},
    question: "Which departments attract the most browsing?",
    map: function () {
      if (!this.department) return;
      emit(this.department.toLowerCase(), {
        hits: 1,
        product_views: this.event_type === "product_view" ? 1 : 0
      });
    }
  },
  {
    name: "mr_top_products_viewed", coll: "clicks", query: { event_type: "product_view" },
    question: "Which individual products are viewed most?",
    map: function () {
      if (!this.product) return;
      emit(this.product, { hits: 1 });
    }
  }
];

// ---- runner -----------------------------------------------------------------------------------
function runJob(job) {
  const options = { out: { replace: job.name }, query: job.query || {} };
  if (job.finalize) options.finalize = job.finalize;
  const started = Date.now();
  db.getCollection(job.coll).mapReduce(job.map, job.reduce || sumReduce, options);
  const seconds = (Date.now() - started) / 1000;
  const outDocs = db.getCollection(job.name).countDocuments({});
  db.getCollection("meta_mr_timings").insertOne({
    job: job.name, source: job.coll, engine: "mapReduce",
    seconds: seconds, output_docs: outDocs, run_at: new Date()
  });
  print("  " + job.name.padEnd(32) + String(outDocs).padStart(7) + " keys   " + seconds.toFixed(2) + " s");
}

print("SupplyChainIQ MapReduce - database: " + db.getName());
print("  orders: " + db.orders.countDocuments({}) + "   clicks: " + db.clicks.countDocuments({}));
db.getCollection("meta_mr_timings").deleteMany({ engine: "mapReduce" });
JOBS.filter(function (j) { return ONLY.length === 0 || ONLY.indexOf(j.name) >= 0; })
    .forEach(runJob);
print("Done. Inspect e.g.:  db.mr_late_by_mode.find().sort({'value.late_rate_pct': -1})");
