from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

EVENT_SCHEMA = StructType([
    StructField("userId", StringType(), False),
    StructField("eventType", StringType(), False),
    StructField("productId", IntegerType(), True),
    StructField("timestamp", TimestampType(), False),
])

LANDING_SCHEMA = StructType([
    StructField("id", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("product_id", IntegerType(), True),
    StructField("created_at", TimestampType(), False),
])
