"""
Batch PySpark job: reads Parquet from landing zone, aggregates product purchase
metrics, and writes partitioned Parquet to the gold layer.

Run via:
    spark-submit --master spark://spark-master:7077 \
        /opt/spark/apps/metrics_aggregation.py \
        --start 2026-06-12T00:00:00 --end 2026-06-12T01:00:00
"""
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("FlowStack-MetricsAggregation")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        # Avoid full-table scans by pushing filters down to Parquet readers
        .config("spark.sql.parquet.filterPushdown", "true")
        .getOrCreate()
    )


EVENT_SCHEMA = StructType([
    StructField("id", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("product_id", IntegerType(), True),
    StructField("timestamp", TimestampType(), False),
])


def run(start: str, end: str) -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw_df = spark.read.schema(EVENT_SCHEMA).parquet("/data/landing/*/*/*/*.parquet")

    # Filter to only the current processing window (incremental — avoids full scans)
    window_df = raw_df.filter(
        (col("timestamp") >= start) & (col("timestamp") < end)
    )

    purchase_df = (
        window_df
        .filter(col("event_type") == "purchase")
        .groupBy("product_id")
        .count()
        .withColumnRenamed("count", "total_conversions")
        .withColumn("processed_at", current_timestamp())
    )

    purchase_df.write.mode("overwrite").parquet("/data/gold/product_performance_metrics/")
    print(f"Aggregated {purchase_df.count()} product rows → /data/gold/product_performance_metrics/")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    args = parser.parse_args()
    run(args.start, args.end)
