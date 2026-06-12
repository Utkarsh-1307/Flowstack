"""
Multi-tenant Spark batch job invoked by the dynamic DAG factory.
Processes one tenant's events and writes to their dedicated analytics table.
"""
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit


def run(tenant: str, target_table: str, retention_days: int) -> None:
    spark = (
        SparkSession.builder
        .appName(f"FlowStack-TenantJob-{tenant}")
        .config("spark.sql.shuffle.partitions", "50")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    input_path = f"/data/landing/tenants/{tenant}/*/*.parquet"
    df = spark.read.parquet(input_path)

    result = (
        df
        .filter(col("event_type") == "purchase")
        .groupBy("product_id", "event_type")
        .count()
        .withColumnRenamed("count", "total_conversions")
        .withColumn("tenant_id", lit(tenant))
        .withColumn("processed_at", current_timestamp())
    )

    out_path = f"/data/gold/{target_table}/"
    result.write.mode("overwrite").parquet(out_path)
    print(f"[{tenant}] Wrote {result.count()} rows → {out_path}")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tenant", required=True)
    parser.add_argument("--target-table", required=True)
    parser.add_argument("--retention-days", type=int, default=90)
    args = parser.parse_args()
    run(args.tenant, args.target_table, args.retention_days)
