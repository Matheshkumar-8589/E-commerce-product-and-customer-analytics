# E-Commerce Big Data Analytics
**Customer Behaviour and Product Trend Analysis Using Big Data Technologies**
Stack: Hadoop HDFS (storage) · Apache Spark / PySpark (processing) · Python (analytics) · Web dashboard + Microsoft Power BI (visualisation)

## 1. Objective
Collect, store, process and analyse large e-commerce transactions to find purchasing behaviour, popular products, revenue trends, customer spending patterns, category performance and regional sales distribution.

## 2. Folder structure
```
ecommerce-bigdata/
├── COMMANDS.txt                  # every terminal command (install -> run -> test)
├── README.md   requirements.txt   run_all.sh
├── data/raw/                     # raw CSV (local copy; HDFS copy under /ecommerce/raw)
├── backend/
│   ├── dataset/    generate_synthetic.py, download_dataset.py     (1. collection)
│   ├── hadoop/     hdfs_setup.sh                                  (2. HDFS storage)
│   ├── spark/      spark_pipeline.py                              (3. cleaning, analytics, RFM)
│   ├── analytics/  build_outputs.py                               (4. Python analytics + exports)
│   ├── tests/      test_pipeline.py                               (pytest, 7 tests)
│   └── outputs/    CSV results, charts/, kpis.json, summary_report.txt
└── frontend/
    ├── web_dashboard/  index.html + data/data.js                  (interactive dashboard)
    └── powerbi/        data/*.csv + POWERBI_GUIDE.md              (5. Power BI)
```

## 3. Dataset
| Option | Source |
|---|---|
| Real (recommended for report) | UCI **Online Retail II**, ~1M rows, 2009-2011: https://archive.ics.uci.edu/dataset/502/online+retail+ii (also Kaggle: `mashlyn/online-retail-ii-uci`). `download_dataset.py` converts it to the project schema; product category is derived from description keywords because the original has no category column. |
| Synthetic (scalable) | `generate_synthetic.py --rows N` creates 8 categories, 8 regions, seasonality, yearly growth, Pareto-distributed customers and ~4% deliberately dirty rows (duplicates, nulls, negative quantity). Use it to demonstrate Volume (millions of rows). |

Unified schema: `order_id, customer_id, product_id, product_name, category, quantity, unit_price, order_timestamp, region`.

## 4. Setup
All install steps (Java 11, Hadoop 3.3.6, Spark 3.5.1, SSH, Python venv) are in **COMMANDS.txt, section A**. Python libraries: see `requirements.txt` (pyspark 3.5.1, pandas, numpy, matplotlib, openpyxl, requests, pytest).

## 5. Implementation steps
1. **Collect**: generate or download the CSV into `data/raw/`.
2. **Store**: `hdfs_setup.sh` creates `/ecommerce/{raw,processed,results}` and uploads the file; HDFS splits it into 128 MB blocks.
3. **Process (Spark)** `spark_pipeline.py`:
   - reads CSV with an explicit schema (no costly schema inference);
   - `dropDuplicates()`; fills missing category/region with `Unknown`; drops rows missing customer, product, price, quantity or timestamp; removes quantity <= 0 and price <= 0;
   - derives `revenue`, `year`, `year_month`; writes **Parquet partitioned by year**;
   - computes KPIs, monthly (with MoM %) and yearly (with YoY %) trends, top products, category and region performance;
   - **RFM segmentation**: Recency, Frequency, Monetary scored 1-4 with `ntile(4)`; total score >= 10 Champions, 8-9 Loyal, 6-7 Potential, otherwise At Risk;
   - builds the aggregated "cube" tables used by both dashboards.
4. **Python analytics** `build_outputs.py`: matplotlib charts, `summary_report.txt`, `data.js` for the web dashboard, CSVs for Power BI.
5. **Visualise**: open the web dashboard or build the Power BI report (`POWERBI_GUIDE.md` has the model, DAX and layout).

## 6. Run
```bash
bash run_all.sh synthetic 500000 local        # quick run, no Hadoop needed
bash run_all.sh synthetic 2000000 hdfs        # full Hadoop + Spark run
python -m http.server 8080 --directory frontend/web_dashboard   # http://localhost:8080
pytest -v backend/tests
```

## 7. Sample output (real run on 300,000 generated rows)
```
DATA QUALITY : raw 305,910 -> after de-dup 298,879 -> after missing-value handling 294,392 -> final 292,913
KPIs         : revenue 90,905,066.30 | orders 99,598 | customers 7,475 | units 669,450 | AOV 912.72
Customer segments (customers): Champions 2126, At Risk 2107, Potential 1695, Loyal 1547
```
Excerpt of `backend/outputs/summary_report.txt`:
```
E-COMMERCE BIG DATA ANALYTICS - SUMMARY REPORT
==================================================
Total revenue        : 90,905,066.30
Total orders         : 99,598
Total customers      : 7,475
Average order value  : 912.72
Best month           : 2025-12 (4,796,110.94)
Top category         : Electronics (48,218,483.59)
Top region           : South India (21,316,687.06)
Top product          : Electronics Item 272 (7,655,665.92)

```
Findings: Electronics gives ~53% of revenue; November-December are the peak months; revenue grows each year. (Numbers differ per run size, seed and dataset.)

## 8. Dashboard features
KPI cards (revenue, units, orders, AOV, customers), revenue line chart, monthly comparison by year, category bar, region bar, segment donut, top-10 products table. **Filters:** month range, category, customer segment, region. Orders, AOV and customers do not follow the category filter because an order can contain several categories (stated on the page). The web dashboard embeds a top-30-product cube to stay light; Power BI uses the full cube.

## 9. Test cases (`pytest`, 7 passed)
| # | Test | Expected |
|---|---|---|
| 1 | duplicate row in input | 1 duplicate removed |
| 2 | null customer, qty -1, price 0 | 1 missing + 2 invalid rows dropped, 3 rows remain |
| 3 | null category/region | filled with `Unknown` |
| 4 | revenue total | 200 + 60 + 100 = 360 |
| 5 | monthly trend | Jan 200, Feb 60, Mar 100 |
| 6 | RFM output | only Champions/Loyal/Potential/At Risk |
| 7 | generator | correct schema, contains dirty rows |

## 10. Big Data 5 Vs
| V | In this project |
|---|---|
| Volume | Millions of order lines stored in HDFS blocks and processed in parallel partitions; generator scales rows. |
| Velocity | Timestamped transactions arriving continuously; the pipeline is batch, and the same transformations can move to Spark Structured Streaming reading Kafka or an HDFS landing folder. |
| Variety | CSV/Excel source, Parquet, JSON, CSV outputs, text descriptions, numeric and time data. |
| Veracity | Duplicates, nulls, negative quantity and zero price are measured and removed; the data-quality report is exported. |
| Value | Revenue trends, best products, RFM segments and regional insight support pricing, stock and marketing decisions. |

## 11. Why Hadoop and Spark
- **HDFS**: splits files into blocks, replicates them (fault tolerance), scales out on commodity nodes, moves computation to the data.
- **Spark**: in-memory processing, lazy evaluation with the Catalyst optimiser, partition parallelism, `cache()` for reused data and Parquet column pruning. It is much faster than MapReduce for iterative analytics, and the same code runs unchanged on a laptop or a cluster (`--master yarn`).
- **Python**: PySpark API, pandas/matplotlib for post-processing.

## 12. Limitations and future work
Batch only; RFM uses fixed quartile rules (could use K-Means in Spark MLlib); window functions without partitions are fine for a mini project but should be partitioned on a cluster; add Kafka streaming and a Hive table layer.
