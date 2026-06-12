"""
Spark Structured Streaming job: consumes the `user-events` Kafka topic,
applies a 1-minute sliding window aggregation, and sinks results to Postgres.

Run via:
    spark-submit --master spark://spark-master:7077 \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,\
                   org.postgresql:postgresql:42.7.3 \
        /opt/spark/apps/streaming_consumer.py
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-broker:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_USER_EVENTS", "user-events")
POSTGRES_URL = os.getenv("DATABASE_URL", "").replace("+asyncpg", "")
POSTGRES_JDBC = f"jdbc:{POSTGRES_URL}" if POSTGRES_URL else "jdbc:postgresql://postgres-db:5432/analytics_platform"
POSTGRES_PROPS = {
    "user": "engine_admin",
    "password": os.getenv("DATABASE_SECURE_PASSWORD", ""),
    "driver": "org.postgresql.Driver",
}

PAYLOAD_SCHEMA = StructType([
    StructField("userId", StringType(), False),
    StructField("eventType", StringType(), False),
    StructField("productId", IntegerType(), True),
    StructField("timestamp", TimestampType(), False),
])


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("FlowStack-StreamingConsumer")
        .config("spark.sql.shuffle.partitions", "8")  # lower for streaming micro-batches
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    events_df = (
        raw_stream
        .selectExpr("CAST(value AS STRING) as json_value", "timestamp as kafka_ts")
        .select(from_json(col("json_value"), PAYLOAD_SCHEMA).alias("data"), col("kafka_ts"))
        .select("data.*", "kafka_ts")
    )

    # 1-minute tumbling window: count events per type every minute
    windowed_df = (
        events_df
        .withWatermark("kafka_ts", "2 minutes")
        .groupBy(window(col("kafka_ts"), "1 minute"), col("eventType"))
        .agg(count("*").alias("event_count"))
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("eventType").alias("event_type"),
            col("event_count"),
        )
    )

    def write_to_postgres(batch_df, batch_id):
        if batch_df.count() == 0:
            return
        batch_df.write.jdbc(
            url=POSTGRES_JDBC,
            table="live_event_metrics",
            mode="append",
            properties=POSTGRES_PROPS,
        )

    query = (
        windowed_df.writeStream
        .foreachBatch(write_to_postgres)
        .outputMode("update")
        .option("checkpointLocation", "/data/checkpoints/streaming_consumer")
        .trigger(processingTime="10 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
