# SupplyChainIQ — Industrial Supply Chain & Logistics Intelligence

Big-data mini-project: ingest the Kaggle **DataCo Smart Supply Chain** dataset into MongoDB, clean and
reshape it, analyse it with **MapReduce**, and turn the results into charts and findings.

- Dataset: https://www.kaggle.com/datasets/shashwatwork/dataco-smart-supply-chain-for-big-data-analysis
- Original source: Constante, F., Silva, F., Pereira, A. (2019), *DataCo Smart Supply Chain for Big Data
  Analysis*, Mendeley Data, DOI 10.17632/8gx2fvg2k6
- Files used: `DataCoSupplyChainDataset.csv` (orders) and `tokenized_access_logs.csv` (clickstream)

## What is in the box

| File | Purpose |
|---|---|
| `01_ingest_clean.py` | Reads both CSVs, cleans them, reshapes orders into nested documents, parses the clickstream, loads MongoDB, builds indexes, writes `out/cleaning_log.json` |
| `02_mapreduce_jobs.js` | 12 MapReduce jobs (8 on orders, 4 on clickstream) for `mongosh` |
| `03_validate_aggregation.js` | Re-computes 6 jobs with aggregation pipelines, compares results, records run times |
| `04_export_and_charts.py` | Exports `mr_*` collections to CSV, draws 8 charts, writes `tables.md` and `findings_draft.md` |
| `tests/` | Offline smoke test (synthetic data, Node emulation of mapReduce, pandas cross-check) |

## Run it

```bash
# 0. environment
pip install -r requirements.txt
#    MongoDB Community Server (a local install is safest for mapReduce) + mongosh

# 1. data: download from Kaggle into ./data (website, or:)
kaggle datasets download -d shashwatwork/dataco-smart-supply-chain-for-big-data-analysis -p data --unzip

# 2. ingest + clean (use --dry-run first if you want to check the cleaning without MongoDB)
python 01_ingest_clean.py --orders data/DataCoSupplyChainDataset.csv \
                          --logs   data/tokenized_access_logs.csv --drop

# 3. MapReduce
mongosh "mongodb://localhost:27017/supplychainiq" --file 02_mapreduce_jobs.js

# 4. validate against aggregation pipelines (also gives mapReduce-vs-aggregate timings)
mongosh "mongodb://localhost:27017/supplychainiq" --file 03_validate_aggregation.js

# 5. CSVs, charts, findings draft  ->  ./results
python 04_export_and_charts.py
```

## Data model

`orders` — one document per order line item (`_id` = order item id):
`order_id, order_date, order_status, payment_type, customer{}, product{}, shipping{}, order_location{},
geo (GeoJSON point), financials{}, derived{order_month, is_loss}, dq{flags[]}`.
`dq.flags` is a variable-length array naming every data-quality issue found on that row.

`clicks` — one document per page view: `ts, hour, dow, month, event_type, department, category, product,
visitor_id, path`. Department/category/product are recovered from the URL when the columns are blank;
the IP address is replaced by a salted hash (`visitor_id`).

## Cleaning decisions (all counted in `cleaning_log.json`)

- CSVs are read as **Latin-1** (the dataset is commonly loaded as ISO-8859-1; use `--encoding utf-8` if yours differs).
- Dropped for privacy: customer email, password, first/last name, street. Dropped for emptiness: any column
  more than 95% null (e.g. `product_description`, `order_zipcode`).
- Duplicate `order_item_id` rows are removed (first kept); rows without an order/item id are removed.
- Dates are parsed (`M/D/YYYY H:MM`); unparseable dates become null and are flagged, not silently dropped.
- Negative shipping days are set to null and flagged. Delivery status vs `late_delivery_risk`
  disagreements, ship-before-order dates, non-positive quantities and out-of-range discounts are flagged.
- Status text is upper-cased/trimmed. Revenue and profit jobs exclude `CANCELED` and `SUSPECTED_FRAUD`
  orders (change `EXCLUDED_STATUSES` at the top of `02_mapreduce_jobs.js` and `03_...js`).
- Each document is an order **line item**, so counts are items, not distinct orders.

## About MapReduce in MongoDB

`db.collection.mapReduce()` has been deprecated since MongoDB 5.0 in favour of aggregation pipelines. It is
used here because the assignment asks for MapReduce; step 3 validates it against aggregation. If your server
version rejects `mapReduce`, run the pipelines in `03_validate_aggregation.js` instead and say so in the report.
Map/reduce/finalize functions are sent to the server as text, so they must not use outside variables.

## Testing status — read this

Tested here (sandbox, no real dataset, no MongoDB server):

- `01_ingest_clean.py` on a synthetic 2,000-row / 6,000-event sample with deliberate dirt (duplicates, bad
  dates, padded text, negative days, accented characters) — under pandas 2.2.3 and 3.0.2.
- All 12 map/reduce/finalize functions through a Node emulation of mapReduce (isolated sandbox, partial
  re-reduce), with results matching an independent pandas computation for every job.
- `04_export_and_charts.py` in offline mode.

**Not** tested: the real Kaggle files, a live MongoDB server (the `mapReduce` call itself, the
aggregation pipelines in `03_validate_aggregation.js`, index creation, `pymongo` inserts). Expect to fix small
things on first contact — the column names are taken from the published dataset description, and the script
stops with a clear message if a required column is missing. Run `bash tests/run_smoke_test.sh` any time to confirm the offline parts still work.

Numbers produced from the synthetic sample are random. Never put them in the report.
