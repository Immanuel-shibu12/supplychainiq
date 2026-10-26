#!/usr/bin/env bash
# Offline smoke test: no MongoDB and no Kaggle download needed.
# Generates a small SYNTHETIC sample, cleans it, runs every MapReduce job through a Node
# emulation of mapReduce, cross-checks the results with pandas and builds charts.
set -euo pipefail
cd "$(dirname "$0")/.."
python3 tests/make_sample_data.py --out data/sample
python3 01_ingest_clean.py --orders data/sample/DataCoSupplyChainDataset.csv \
                           --logs data/sample/tokenized_access_logs.csv \
                           --dry-run --out-dir out_smoke
node tests/simulate_mapreduce.js out_smoke
python3 tests/check_against_pandas.py out_smoke
python3 04_export_and_charts.py --json-dir out_smoke/mr_json --out results_smoke
echo "Smoke test finished. (Numbers come from random synthetic data - do not quote them.)"
