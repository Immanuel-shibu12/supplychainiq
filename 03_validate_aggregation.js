/*
 * SupplyChainIQ - step 3: validate the MapReduce results with aggregation pipelines.
 *
 *     mongosh "mongodb://localhost:27017/supplychainiq" --file 03_validate_aggregation.js
 *
 * For a representative set of jobs this script
 *   1. runs the equivalent aggregation pipeline,
 *   2. compares every key and field with the mr_* collection (tolerance below),
 *   3. records both run times in meta_mr_timings, and prints a comparison table.
 * Run it after 02_mapreduce_jobs.js. Exit code is 1 if any check fails.
 */
const TOL = 0.02;
const EXCLUDED_STATUSES = ["CANCELED", "SUSPECTED_FRAUD"];       // keep in sync with step 2
const REV = { order_status: { $nin: EXCLUDED_STATUSES } };

const regionKey = {
  $concat: [{ $ifNull: ["$order_location.market", "Unknown"] }, " | ",
            { $ifNull: ["$order_location.region", "Unknown"] }]
};

const CHECKS = [
  {
    job: "mr_late_by_mode", coll: "orders", fields: ["n", "late", "slip"],
    pipeline: [
      { $match: { "shipping.mode": { $ne: null } } },
      { $group: { _id: "$shipping.mode", n: { $sum: 1 }, late: { $sum: "$shipping.late_risk" },
                  slip: { $sum: "$shipping.delay_days" } } }
    ]
  },
  {
    job: "mr_sales_by_region", coll: "orders", fields: ["n", "sales", "revenue", "profit"],
    pipeline: [
      { $match: REV },
      { $group: { _id: regionKey, n: { $sum: 1 }, sales: { $sum: "$financials.sales" },
                  revenue: { $sum: "$financials.item_total" },
                  profit: { $sum: "$financials.profit_per_order" } } }
    ]
  },
  {
    job: "mr_monthly_trend", coll: "orders", fields: ["n", "qty", "revenue", "profit"],
    pipeline: [
      { $match: Object.assign({ order_date: { $ne: null } }, REV) },
      { $group: { _id: { $dateToString: { format: "%Y-%m", date: "$order_date" } }, n: { $sum: 1 },
                  qty: { $sum: "$financials.quantity" }, revenue: { $sum: "$financials.item_total" },
                  profit: { $sum: "$financials.profit_per_order" } } }
    ]
  },
  {
    job: "mr_discount_impact", coll: "orders", fields: ["n", "revenue", "profit"],
    pipeline: [
      { $match: Object.assign({ "financials.discount_rate": { $ne: null } }, REV) },
      { $addFields: { lo: { $toInt: { $multiply: [
          { $floor: { $divide: [
            { $floor: { $add: [{ $multiply: ["$financials.discount_rate", 100] }, 0.5] } }, 5] } }, 5] } } } },
      { $group: { _id: { $cond: [{ $lt: ["$lo", 10] }, { $concat: ["0", { $toString: "$lo" }] }, { $toString: "$lo" }] },
                  n: { $sum: 1 }, revenue: { $sum: "$financials.item_total" },
                  profit: { $sum: "$financials.profit_per_order" } } }
    ]
  },
  {
    job: "mr_clicks_by_hour", coll: "clicks", fields: ["hits", "product_views"],
    pipeline: [
      { $match: { hour: { $ne: null } } },
      { $group: { _id: "$hour", hits: { $sum: 1 },
                  product_views: { $sum: { $cond: [{ $eq: ["$event_type", "product_view"] }, 1, 0] } } } }
    ]
  },
  {
    job: "mr_clicks_by_department", coll: "clicks", fields: ["hits", "product_views"],
    pipeline: [
      { $match: { department: { $ne: null } } },
      { $group: { _id: { $toLower: "$department" }, hits: { $sum: 1 },
                  product_views: { $sum: { $cond: [{ $eq: ["$event_type", "product_view"] }, 1, 0] } } } }
    ]
  }
];

function mrSeconds(job) {
  const t = db.getCollection("meta_mr_timings").findOne({ job: job, engine: "mapReduce" });
  return t ? t.seconds : null;
}

let failures = 0;
const rows = [];
db.getCollection("meta_mr_timings").deleteMany({ engine: "aggregate" });

CHECKS.forEach(function (c) {
  const started = Date.now();
  const agg = db.getCollection(c.coll).aggregate(c.pipeline, { allowDiskUse: true }).toArray();
  const aggSec = (Date.now() - started) / 1000;

  const mr = db.getCollection(c.job).find({}).toArray();
  const aggMap = {}, mrMap = {};
  agg.forEach(function (d) { aggMap[String(d._id)] = d; });
  mr.forEach(function (d) { mrMap[String(d._id)] = d.value; });

  const problems = [];
  Object.keys(aggMap).forEach(function (k) {
    if (!(k in mrMap)) { problems.push("key only in aggregation: " + k); return; }
    c.fields.forEach(function (f) {
      const a = Number(aggMap[k][f] || 0), m = Number(mrMap[k][f] || 0);
      if (Math.abs(a - m) > TOL) problems.push(k + "." + f + ": aggregate=" + a + " mapReduce=" + m);
    });
  });
  Object.keys(mrMap).forEach(function (k) {
    if (!(k in aggMap)) problems.push("key only in mapReduce: " + k);
  });

  db.getCollection("meta_mr_timings").insertOne({
    job: c.job, source: c.coll, engine: "aggregate", seconds: aggSec,
    output_docs: agg.length, run_at: new Date()
  });
  if (problems.length) failures++;
  rows.push({
    job: c.job, keys: agg.length,
    mapReduce_s: mrSeconds(c.job), aggregate_s: Number(aggSec.toFixed(2)),
    result: problems.length ? "FAIL" : "PASS"
  });
  problems.slice(0, 5).forEach(function (p) { print("    ! " + c.job + " -> " + p); });
});

print("");
console.table(rows);
print(failures === 0 ? "All validated jobs match." : failures + " job(s) differ - see messages above.");
quit(failures === 0 ? 0 : 1);
