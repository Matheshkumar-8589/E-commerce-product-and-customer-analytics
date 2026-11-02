#!/usr/bin/env bash
# One-command pipeline. Usage: bash run_all.sh [synthetic|real] [rows] [local|hdfs]
set -e
SRC=${1:-synthetic}; ROWS=${2:-500000}; MODE=${3:-local}
[ "$SRC" = "real" ] && python backend/dataset/download_dataset.py || python backend/dataset/generate_synthetic.py --rows "$ROWS"
if [ "$MODE" = "hdfs" ]; then
  bash backend/hadoop/hdfs_setup.sh data/raw/ecommerce_raw.csv
  spark-submit --master local[*] backend/spark/spark_pipeline.py \
    --input hdfs://localhost:9000/ecommerce/raw/ecommerce_raw.csv \
    --processed hdfs://localhost:9000/ecommerce/processed/sales_clean
else
  python backend/spark/spark_pipeline.py
fi
python backend/analytics/build_outputs.py
echo "Done. Open frontend/web_dashboard/index.html  |  Power BI files: frontend/powerbi/data/"
