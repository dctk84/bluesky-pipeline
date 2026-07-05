"""Khai báo cấu hình Kafka dùng chung cho local pipeline."""

import os


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_RAW_EVENTS_TOPIC = os.getenv("KAFKA_TOPIC", "bluesky.raw.events.v2")
SPARK_KAFKA_CONNECTOR_PACKAGE = os.getenv(
    "SPARK_KAFKA_CONNECTOR_PACKAGE",
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
)