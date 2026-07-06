"""Stream event volume theo phút từ Kafka vào ClickHouse."""

import json
from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, date_trunc, from_json, lit, when

from bluesky_pipeline.bronze_schemas import build_commit_envelope_schema
from bluesky_pipeline.clickhouse_client import execute_clickhouse
from bluesky_pipeline.gold_tables import (
    GOLD_EVENT_VOLUME_1M_STREAM_CHECKPOINT_LOCATION,
    GOLD_EVENT_VOLUME_1M_STREAM_TABLE,
)
from bluesky_pipeline.kafka_config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_RAW_EVENTS_TOPIC,
    SPARK_KAFKA_CONNECTOR_PACKAGE,
)
from bluesky_pipeline.spark_session import create_spark_session


def build_clickhouse_payload(batch_df: DataFrame, batch_id: int) -> str:
    """Chuyển batch aggregate thành JSONEachRow để insert ClickHouse.

    Input chính là DataFrame gồm window_start, event_type và event_count.
    Output là chuỗi JSONEachRow dùng làm HTTP body cho ClickHouse.
    """
    rows = batch_df.select("window_start", "event_type", "event_count").collect()
    lines = []

    for row in rows:
        # ClickHouse DateTime nhận chuỗi dạng YYYY-MM-DD HH:MM:SS.
        window_start = row.window_start
        if isinstance(window_start, datetime):
            window_start = window_start.strftime("%Y-%m-%d %H:%M:%S")

        lines.append(
            json.dumps(
                {
                    "window_start": window_start,
                    "event_type": row.event_type,
                    "event_count": int(row.event_count),
                    "spark_batch_id": int(batch_id),
                },
                ensure_ascii=False,
            )
        )

    return "\n".join(lines) + ("\n" if lines else "")


def write_batch_to_clickhouse(batch_df: DataFrame, batch_id: int) -> None:
    """Ghi một micro-batch aggregate vào ClickHouse.

    Input gồm DataFrame của micro-batch và batch_id do Spark cung cấp.
    Output là dữ liệu được append vào bảng ClickHouse streaming aggregate.
    """
    if batch_df.rdd.isEmpty():
        print(f"batch_id={batch_id}: empty batch")
        return

    payload = build_clickhouse_payload(batch_df, batch_id)

    # Insert dạng append; SummingMergeTree sẽ cộng các dòng cùng key khi query/merge.
    execute_clickhouse(
        f"""
        INSERT INTO {GOLD_EVENT_VOLUME_1M_STREAM_TABLE}
        FORMAT JSONEachRow
        """,
        body=payload,
    )

    print(f"batch_id={batch_id}: inserted_rows={batch_df.count()}")


def main() -> None:
    """Đọc Kafka stream, aggregate event volume theo phút và ghi vào ClickHouse."""
    # Tạo SparkSession có Kafka connector.
    spark = create_spark_session(
        "bluesky-stream-event-volume-clickhouse",
        extra_packages=[SPARK_KAFKA_CONNECTOR_PACKAGE],
    )
    spark.sparkContext.setLogLevel("WARN")

    # Đọc raw events từ Kafka.
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_RAW_EVENTS_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    # Parse event envelope commit từ Kafka message value.
    parsed_df = kafka_df.select(
        col("value").cast("string").alias("message_value"),
        col("timestamp").alias("kafka_timestamp"),
    ).select(
        from_json(
            col("message_value"),
            build_commit_envelope_schema(),
        ).alias("event"),
        col("kafka_timestamp"),
    )

    # Map collection/operation thành event_type phục vụ dashboard.
    typed_events_df = parsed_df.select(
        col("event.event_kind").alias("event_kind"),
        col("event.collection").alias("collection"),
        col("event.operation").alias("operation"),
        col("kafka_timestamp").alias("event_timestamp"),
    ).filter(
        col("event_kind") == "commit"
    ).withColumn(
        "event_type",
        when(col("operation") == "delete", lit("deleted_record"))
        .when(col("collection") == "app.bsky.feed.post", lit("post"))
        .when(col("collection") == "app.bsky.feed.like", lit("like"))
        .when(col("collection") == "app.bsky.feed.repost", lit("repost"))
        .when(col("collection") == "app.bsky.graph.follow", lit("follow")),
    ).filter(
        col("event_type").isNotNull()
    )

    # Aggregate trong từng micro-batch theo phút ingest Kafka.
    event_volume_df = typed_events_df.groupBy(
        date_trunc("minute", col("event_timestamp")).alias("window_start"),
        col("event_type"),
    ).agg(
        count(lit(1)).alias("event_count")
    )

    # Ghi từng micro-batch vào ClickHouse.
    query = (
        event_volume_df.writeStream
        .foreachBatch(write_batch_to_clickhouse)
        .option("checkpointLocation", GOLD_EVENT_VOLUME_1M_STREAM_CHECKPOINT_LOCATION)
        .outputMode("update")
        .trigger(processingTime="30 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()