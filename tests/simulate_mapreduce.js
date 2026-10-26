/*
 * simulate_mapreduce.js
 * ---------------------
 * Runs 02_mapreduce_jobs.js against the NDJSON files written by
 *     python 01_ingest_clean.py ... --dry-run
 * using a small emulation of MongoDB's mapReduce, so the map/reduce/finalize logic can be
 * tested WITHOUT a MongoDB server:
 *
 *     node tests/simulate_mapreduce.js [out_dir]
 *
 * What the emulation reproduces from the real thing:
 *  - map / reduce / finalize are re-created from their source text inside an isolated sandbox
 *    that contains only `emit` (so a hidden dependency on an outer variable fails here);
 *  - values for a key are reduced in several partial batches and the partial results are
 *    reduced again (MongoDB may do this), and reduce is skipped for a single value;
 *  - `query` supports the simple forms used by the jobs: equality and $nin.
 * It is NOT a MongoDB server: it cannot catch version-specific server errors.
 *
 * Output: <out_dir>/mr_json/<job>.json   ([{_id, value}, ...])
 */
const fs = require("fs");
const path = require("path");
const vm = require("vm");

const outDir = process.argv[2] || "out";
const root = path.resolve(__dirname, "..");
const ISO = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/;

function loadNdjson(file) {
  const full = path.join(outDir, file);
  if (!fs.existsSync(full)) return [];
  return fs.readFileSync(full, "utf8").split("\n").filter(Boolean).map((line) =>
    JSON.parse(line, (k, v) => (typeof v === "string" && ISO.test(v) ? new Date(v + "Z") : v)));
}

const data = { orders: loadNdjson("orders.ndjson"), clicks: loadNdjson("clicks.ndjson") };
console.log(`loaded ${data.orders.length} orders, ${data.clicks.length} clicks`);

function matches(doc, query) {
  return Object.entries(query || {}).every(([field, cond]) => {
    const val = field.split(".").reduce((o, k) => (o == null ? o : o[k]), doc);
    if (cond && typeof cond === "object" && "$nin" in cond) return !cond.$nin.includes(val);
    return val === cond;
  });
}

// Re-create a function from its source in a sandbox that only knows `emit`.
function isolate(fn, emit) {
  const sandbox = vm.createContext({ emit, Math, Date });
  return vm.runInContext("(" + fn.toString() + ")", sandbox);
}

function simulateMapReduce(docs, map, reduce, options) {
  const groups = new Map();
  const emit = (k, v) => {
    const key = typeof k === "object" ? JSON.stringify(k) : k;
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(v);
  };
  const m = isolate(map, emit);
  const r = isolate(reduce, emit);
  const f = options.finalize ? isolate(options.finalize, emit) : null;

  docs.filter((d) => matches(d, options.query)).forEach((d) => m.call(d));

  const out = [];
  for (const [key, values] of groups) {
    let value;
    if (values.length === 1) {
      value = values[0];
    } else {
      // partial reduces in chunks of 7, then reduce the partials (re-reduce)
      let partials = [];
      for (let i = 0; i < values.length; i += 7) {
        const chunk = values.slice(i, i + 7);
        partials.push(chunk.length === 1 ? chunk[0] : r(key, chunk));
      }
      while (partials.length > 1) {
        const next = [];
        for (let i = 0; i < partials.length; i += 3) {
          const chunk = partials.slice(i, i + 3);
          next.push(chunk.length === 1 ? chunk[0] : r(key, chunk));
        }
        partials = next;
      }
      value = partials[0];
    }
    if (f) value = f(key, value);
    out.push({ _id: key, value });
  }
  return out;
}

// ---- fake mongosh environment ---------------------------------------------------------------
const results = {};
const collections = (name) => ({
  mapReduce(map, reduce, options) {
    const rows = simulateMapReduce(data[name] || [], map, reduce, options);
    results[options.out.replace] = rows;
  },
  countDocuments() {
    return (results[name] || data[name] || []).length;
  },
  insertOne() {}, deleteMany() {},
});
const fakeDb = {
  getCollection: collections,
  getName: () => "simulated",
  orders: collections("orders"),
  clicks: collections("clicks"),
};

const ctx = vm.createContext({ db: fakeDb, print: console.log, console, Math, Date, String });
vm.runInContext(fs.readFileSync(path.join(root, "02_mapreduce_jobs.js"), "utf8"), ctx, { filename: "02_mapreduce_jobs.js" });

const dest = path.join(outDir, "mr_json");
fs.mkdirSync(dest, { recursive: true });
for (const [name, rows] of Object.entries(results)) {
  fs.writeFileSync(path.join(dest, name + ".json"), JSON.stringify(rows, null, 1));
}
console.log(`wrote ${Object.keys(results).length} result files to ${dest}`);
