#!/usr/bin/env python3
"""Spark (PySpark) processing: cleaning, analytics, RFM segmentation, export.

Local  : python backend/spark/spark_pipeline.py --input data/raw/ecommerce_raw.csv
HDFS   : spark-submit backend/spark/spark_pipeline.py \
           --input hdfs://localhost:9000/ecommerce/raw/ecommerce_raw.csv \
           --processed hdfs://localhost:9000/ecommerce/processed/sales_clean
"""
import argparse, json, os
from pyspark.sql import SparkSession, Window, functions as F
from pyspark.sql.types import (StructType, StructField, StringType, LongType,
                               IntegerType, DoubleType)

SCHEMA = StructType([
    StructField("order_id", StringType()), StructField("customer_id", LongType()),
    StructField("product_id", StringType()), StructField("product_name", StringType()),
    StructField("category", StringType()), StructField("quantity", IntegerType()),
    StructField("unit_price", DoubleType()), StructField("order_timestamp", StringType()),
    StructField("region", StringType()),
])


def get_spark(name="EcommerceBigDataAnalytics"):
    return (SparkSession.builder.appName(name)
            .config("spark.sql.shuffle.partitions", "16")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.ui.showConsoleProgress", "false")
            .getOrCreate())


# ------------------------------------------------------------------ cleaning
def clean(df):
    """Remove duplicates, handle missing values, drop invalid rows, add derived columns."""
    stats = {"raw_rows": df.count()}
    d = df.dropDuplicates()
    stats["after_duplicate_removal"] = d.count()
    d = (d.withColumn("order_timestamp", F.to_timestamp("order_timestamp"))
          .withColumn("category", F.coalesce(F.trim("category"), F.lit("Unknown")))
          .withColumn("region", F.coalesce(F.trim("region"), F.lit("Unknown"))))
    d = d.dropna(subset=["order_id", "customer_id", "product_id", "order_timestamp",
                         "quantity", "unit_price"])
    stats["after_missing_value_handling"] = d.count()
    d = d.filter((F.col("quantity") > 0) & (F.col("unit_price") > 0))
    d = (d.withColumn("revenue", F.round(F.col("quantity") * F.col("unit_price"), 2))
          .withColumn("order_date", F.to_date("order_timestamp"))
          .withColumn("year", F.year("order_timestamp"))
          .withColumn("year_month", F.date_format("order_timestamp", "yyyy-MM")))
    stats["final_clean_rows"] = d.count()
    stats["duplicates_removed"] = stats["raw_rows"] - stats["after_duplicate_removal"]
    stats["rows_dropped_missing"] = stats["after_duplicate_removal"] - stats["after_missing_value_handling"]
    stats["rows_dropped_invalid"] = stats["after_missing_value_handling"] - stats["final_clean_rows"]
    return d, stats


# ----------------------------------------------------------- RFM segmentation
def rfm_segments(sales):
    """Recency/Frequency/Monetary quartile scoring -> customer segment."""
    ref = sales.agg(F.max("order_timestamp")).first()[0]
    cust = sales.groupBy("customer_id").agg(
        F.datediff(F.lit(ref), F.max("order_timestamp")).alias("recency_days"),
        F.countDistinct("order_id").alias("frequency"),
        F.round(F.sum("revenue"), 2).alias("monetary"),
        F.first("region").alias("region"))
    r = cust.withColumn("r", F.ntile(4).over(Window.orderBy(F.col("recency_days").desc())))
    r = r.withColumn("f", F.ntile(4).over(Window.orderBy("frequency", "monetary")))
    r = r.withColumn("m", F.ntile(4).over(Window.orderBy("monetary")))
    r = r.withColumn("rfm_score", F.col("r") + F.col("f") + F.col("m"))
    return r.withColumn("segment",
        F.when(F.col("rfm_score") >= 10, "Champions")
         .when(F.col("rfm_score") >= 8, "Loyal")
         .when(F.col("rfm_score") >= 6, "Potential")
         .otherwise("At Risk"))


# ------------------------------------------------------------------ analytics
def analyse(sales):
    seg = rfm_segments(sales)
    j = sales.join(seg.select("customer_id", "segment"), "customer_id")
    res = {}
    n_orders = sales.select("order_id").distinct().count()
    tot = sales.agg(F.sum("revenue").alias("rev"), F.sum("quantity").alias("units")).first()
    res["kpis"] = {"total_revenue": round(tot["rev"], 2), "total_orders": n_orders,
                   "total_customers": seg.count(), "total_units": int(tot["units"]),
                   "avg_order_value": round(tot["rev"] / n_orders, 2)}

    w = Window.orderBy("year_month")
    res["monthly"] = (sales.groupBy("year_month").agg(
        F.round(F.sum("revenue"), 2).alias("revenue"),
        F.countDistinct("order_id").alias("orders"), F.sum("quantity").alias("units"))
        .withColumn("prev", F.lag("revenue").over(w))
        .withColumn("mom_growth_pct", F.round((F.col("revenue") - F.col("prev")) / F.col("prev") * 100, 2))
        .drop("prev").orderBy("year_month"))
    yearly = sales.groupBy("year").agg(F.round(F.sum("revenue"), 2).alias("revenue"),
                                       F.countDistinct("order_id").alias("orders"),
                                       F.sum("quantity").alias("units")).orderBy("year")
    res["yearly"] = (yearly.withColumn("prev", F.lag("revenue").over(Window.orderBy("year")))
                     .withColumn("yoy_growth_pct", F.round((F.col("revenue") - F.col("prev")) / F.col("prev") * 100, 2))
                     .drop("prev"))
    res["top_products"] = (sales.groupBy("product_id", "product_name", "category").agg(
        F.sum("quantity").alias("units_sold"), F.countDistinct("order_id").alias("orders"),
        F.round(F.sum("revenue"), 2).alias("revenue")).orderBy(F.desc("revenue")).limit(50))
    res["categories"] = (sales.groupBy("category").agg(
        F.round(F.sum("revenue"), 2).alias("revenue"), F.sum("quantity").alias("units"),
        F.countDistinct("order_id").alias("orders")).orderBy(F.desc("revenue")))
    res["regions"] = (sales.groupBy("region").agg(
        F.round(F.sum("revenue"), 2).alias("revenue"), F.countDistinct("order_id").alias("orders"),
        F.countDistinct("customer_id").alias("customers")).orderBy(F.desc("revenue")))
    res["customer_rfm"] = seg
    res["segment_summary"] = (seg.groupBy("segment", "region").agg(
        F.count("*").alias("customers"), F.round(F.avg("monetary"), 2).alias("avg_spend"),
        F.round(F.avg("frequency"), 2).alias("avg_orders")))
    res["cube"] = (j.groupBy("year_month", "category", "region", "segment", "product_id", "product_name")
                   .agg(F.round(F.sum("revenue"), 2).alias("revenue"), F.sum("quantity").alias("units"),
                        F.count("*").alias("lines")))
    res["order_cube"] = (j.groupBy("year_month", "region", "segment").agg(
        F.countDistinct("order_id").alias("orders"), F.round(F.sum("revenue"), 2).alias("revenue")))
    return res


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="data/raw/ecommerce_raw.csv")
    ap.add_argument("--processed", default="data/processed/sales_clean", help="Parquet output (local or hdfs://)")
    ap.add_argument("--export-dir", default="backend/outputs")
    a = ap.parse_args()

    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")
    raw = spark.read.csv(a.input, header=True, schema=SCHEMA)
    sales, stats = clean(raw)
    sales.write.mode("overwrite").partitionBy("year").parquet(a.processed)   # columnar, partitioned
    sales = spark.read.parquet(a.processed).cache()

    res = analyse(sales)
    os.makedirs(a.export_dir, exist_ok=True)
    for name, df in res.items():
        if name == "kpis":
            continue
        df.toPandas().to_csv(os.path.join(a.export_dir, f"{name}.csv"), index=False)
    with open(os.path.join(a.export_dir, "kpis.json"), "w") as fh:
        json.dump(res["kpis"], fh, indent=2)
    with open(os.path.join(a.export_dir, "data_quality.json"), "w") as fh:
        json.dump(stats, fh, indent=2)
    print("DATA QUALITY :", json.dumps(stats))
    print("KPIs         :", json.dumps(res["kpis"]))
    print("Exports ->", a.export_dir)
    spark.stop()


if __name__ == "__main__":
    main()
