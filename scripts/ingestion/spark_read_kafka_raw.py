"""Đọc raw event từ Kafka bằng Spark Structured Streaming và ghi Bronze Parquet."""

from pyspark.sql.functions import col, date_format, from_json, to_date
from pyspark.sql.types import LongType, MapType, StringType, StructField, StructType

from bluesky_pipeline.schemas.bronze_tables import (
    BRONZE_ACCOUNT_CHECKPOINT_LOCATION,
    BRONZE_ACCOUNT_EVENTS_PATH,
    BRONZE_COMMIT_CHECKPOINT_LOCATION,
    BRONZE_COMMIT_EVENTS_PATH,
    BRONZE_IDENTITY_CHECKPOINT_LOCATION,
    BRONZE_IDENTITY_EVENTS_PATH,
)

from bluesky_pipeline.config.spark import create_spark_session

from bluesky_pipeline.config.kafka import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_RAW_EVENTS_TOPIC,
    SPARK_KAFKA_CONNECTOR_PACKAGE,
    SPARK_KAFKA_MAX_OFFSETS_PER_TRIGGER,
)

RAW_EVENT_SCHEMA = StructType(
    [
        StructField("schema_version", LongType()),
        StructField("source", StringType()),
        StructField("event_kind", StringType()),
        StructField("received_at", StringType()),
        StructField("collection", StringType()),
        StructField("operation", StringType()),
        StructField("repository_did", StringType()),
        StructField("jetstream_time_us", LongType()),
        StructField("payload", MapType(StringType(), StringType())),
    ]
)


def main() -> None:
    """Đọc Kafka topic, parse envelope JSON và ghi các nhóm event ra Bronze."""
    # Bước 1: Tạo SparkSession có Kafka connector và cấu hình S3A cho MinIO.
    spark = create_spark_session(
        "bluesky-read-kafka-raw",
        extra_packages=[SPARK_KAFKA_CONNECTOR_PACKAGE],
    )
    spark.sparkContext.setLogLevel("WARN")

    # Bước 2: Đọc stream từ Kafka topic raw events.
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_RAW_EVENTS_TOPIC)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", SPARK_KAFKA_MAX_OFFSETS_PER_TRIGGER)
        .load()
    )

    # Bước 3: Chuyển key/value từ binary sang string để parse JSON.
    raw_events_df = kafka_df.select(
        col("key").cast("string").alias("message_key"),
        col("value").cast("string").alias("message_value"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp"),
    )

    # Bước 4: Parse event envelope và giữ lại raw JSON để audit/replay.
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
        col("event.event_kind"),
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

    # Bước 5: Tách event theo family để mỗi Bronze path có layout phù hợp.
    commit_events_df = parsed_events_df.filter(col("event_kind") == "commit")
    identity_events_df = parsed_events_df.filter(col("event_kind") == "identity")
    account_events_df = parsed_events_df.filter(col("event_kind") == "account")

    # Bước 6: Ghi commit events, có thêm partition collection.
    commit_events_df.writeStream.format("parquet").option(
        "path",
        BRONZE_COMMIT_EVENTS_PATH,
    ).option(
        "checkpointLocation",
        BRONZE_COMMIT_CHECKPOINT_LOCATION,
    ).partitionBy(
        "ingest_date",
        "ingest_hour",
        "collection",
    ).outputMode("append").trigger(processingTime="60 seconds").start()

    # Bước 7: Ghi identity events, không partition theo collection vì không có field này.
    identity_events_df.writeStream.format("parquet").option(
        "path",
        BRONZE_IDENTITY_EVENTS_PATH,
    ).option(
        "checkpointLocation",
        BRONZE_IDENTITY_CHECKPOINT_LOCATION,
    ).partitionBy(
        "ingest_date",
        "ingest_hour",
    ).outputMode("append").trigger(processingTime="60 seconds").start()

    # Bước 8: Ghi account events, không partition theo collection vì không có field này.
    account_events_df.writeStream.format("parquet").option(
        "path",
        BRONZE_ACCOUNT_EVENTS_PATH,
    ).option(
        "checkpointLocation",
        BRONZE_ACCOUNT_CHECKPOINT_LOCATION,
    ).partitionBy(
        "ingest_date",
        "ingest_hour",
    ).outputMode("append").trigger(processingTime="60 seconds").start()

    # Bước 9: Giữ process chạy cho tới khi có query lỗi hoặc người dùng dừng.
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
