#!/usr/bin/env bash
# Upload the raw dataset to HDFS. Usage: bash backend/hadoop/hdfs_setup.sh [local_csv]
set -e
LOCAL=${1:-data/raw/ecommerce_raw.csv}
HDFS_BASE=/ecommerce

hdfs dfs -mkdir -p $HDFS_BASE/raw $HDFS_BASE/processed $HDFS_BASE/results
hdfs dfs -put -f "$LOCAL" $HDFS_BASE/raw/ecommerce_raw.csv
echo "--- HDFS listing ---";  hdfs dfs -ls -h $HDFS_BASE/raw
echo "--- Disk usage ---";    hdfs dfs -du -h $HDFS_BASE
echo "--- Block report ---";  hdfs fsck $HDFS_BASE/raw/ecommerce_raw.csv -files -blocks | tail -n 15
