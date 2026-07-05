"""Đọc raw event từ Kafka bằng Spark Structured Streaming và ghi Bronze Parquet."""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, from_json, to_date
from pyspark.sql.types import LongType, MapType, StringType, StructField, StructType
from bluesky_pipeline.spark_session import create_spark_session


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "bluesky.raw.events.v1"
CHECKPOINT_LOCATION = "s3a://bluesky-lake/checkpoints/spark_read_kafka_raw"
BRONZE_OUTPUT_PATH = "s3a://bluesky-lake/bronze/bluesky_raw_events"

RAW_EVENT_SCHEMA = StructType(
    [
        StructField("schema_version", LongType()),
        StructField("source", StringType()),
        StructField("received_at", StringType()),
        StructField("collection", StringType()),
        StructField("operation", StringType()),
        StructField("repository_did", StringType()),
        StructField("jetstream_time_us", LongType()),
        StructField("payload", MapType(StringType(), StringType())),
    ]
)

def main() -> None:
    """Đọc Kafka topic, parse envelope JSON và ghi dữ liệu ra Bronze Parquet local."""
    # Bước 1: Tạo SparkSession.
    spark = create_spark_session(
        "bluesky-read-kafka-raw",
        extra_packages=["org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"],
    )
    spark.sparkContext.setLogLevel("WARN")

    # Bước 2: Đọc stream từ Kafka topic raw events.
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    # Bước 3: Chuyển key/value từ binary sang string để dễ quan sát.
    raw_events_df = kafka_df.select(
        col("key").cast("string").alias("message_key"),
        col("value").cast("string").alias("message_value"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp"),
    )

    parsed_events_df = raw_events_df.select(
        col("message_key"),
        col("message_value"),
        from_json(col("message_value"), RAW_EVENT_SCHEMA).alias("event"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp"),
    ).select(
        col("message_key"),
        col("message_value"),
        col("event.schema_version"),
        col("event.source"),
        col("event.received_at"),
        col("event.collection"),
        col("event.operation"),
        col("event.repository_did"),
        col("event.jetstream_time_us"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_timestamp"),
        to_date(col("timestamp")).alias("ingest_date"),
        date_format(col("timestamp"), "HH").alias("ingest_hour"),
    )

    # Bước 4: In stream ra console để kiểm chứng Spark đã đọc được Kafka.
    query = (
        parsed_events_df.writeStream
        .format("parquet")
        .option("path", BRONZE_OUTPUT_PATH)
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .partitionBy("ingest_date", "ingest_hour", "collection")
        .outputMode("append")
        .start()
    )

    # Bước 5: Giữ process chạy cho tới khi người dùng dừng bằng Ctrl+C.
    query.awaitTermination()


if __name__ == "__main__":
    main()