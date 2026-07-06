"""Stream các realtime metrics theo phút từ Kafka vào ClickHouse."""

import json
from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, date_trunc, from_json, lit, when

from bluesky_pipeline.bronze_schemas import (
    POST_RECORD_SCHEMA,
    build_commit_envelope_schema,
)
from bluesky_pipeline.clickhouse_client import execute_clickhouse
from bluesky_pipeline.gold_tables import (
    GOLD_CONTENT_ACTIVITY_1M_STREAM_TABLE,
    GOLD_ENGAGEMENT_1M_STREAM_TABLE,
    GOLD_EVENT_VOLUME_1M_STREAM_TABLE,
    GOLD_NETWORK_ACTIVITY_1M_STREAM_TABLE,
    GOLD_REALTIME_METRICS_1M_STREAM_CHECKPOINT_LOCATION,
)
from bluesky_pipeline.kafka_config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_RAW_EVENTS_TOPIC,
    SPARK_KAFKA_CONNECTOR_PACKAGE,
)
from bluesky_pipeline.spark_session import create_spark_session


def build_clickhouse_payload(
    batch_df: DataFrame,
    metric_column: str,
    count_column: str,
    batch_id: int,
) -> str:
    """Chuyển batch aggregate thành JSONEachRow để insert ClickHouse.

    Input chính là DataFrame gồm window_start, metric column và count column.
    Output là chuỗi JSONEachRow dùng làm HTTP body cho ClickHouse.
    """
    rows = batch_df.select("window_start", metric_column, count_column).collect()
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
                    metric_column: row[metric_column],
                    count_column: int(row[count_column]),
                    "spark_batch_id": int(batch_id),
                },
                ensure_ascii=False,
            )
        )

    return "\n".join(lines) + ("\n" if lines else "")


def insert_aggregate_to_clickhouse(
    batch_df: DataFrame,
    table_name: str,
    metric_column: str,
    count_column: str,
    batch_id: int,
) -> int:
    """Insert một DataFrame aggregate vào ClickHouse.

    Input là DataFrame aggregate và metadata table/column cần ghi.
    Output là số dòng aggregate đã insert vào ClickHouse.
    """
    payload = build_clickhouse_payload(
        batch_df=batch_df,
        metric_column=metric_column,
        count_column=count_column,
        batch_id=batch_id,
    )

    if not payload:
        return 0

    execute_clickhouse(
        f"""
        INSERT INTO {table_name}
        FORMAT JSONEachRow
        """,
        body=payload,
    )

    return payload.count("\n")


def build_event_volume_aggregate(batch_df: DataFrame) -> DataFrame:
    """Tạo aggregate event volume theo phút và event type."""
    return batch_df.groupBy(
        date_trunc("minute", col("event_timestamp")).alias("window_start"),
        col("event_type"),
    ).agg(
        count(lit(1)).alias("event_count")
    )


def build_content_activity_events(batch_df: DataFrame) -> DataFrame:
    """Tạo các event content activity từ một micro-batch đã parse.

    Output có thể chứa nhiều dòng metric từ cùng một raw event, ví dụ một reply
    vừa được tính vào post vừa được tính vào reply.
    """
    post_create_df = batch_df.filter(
        (col("collection") == "app.bsky.feed.post")
        & (col("operation") == "create")
    ).select(
        col("event_timestamp"),
        lit("post").alias("content_activity_type"),
    )

    original_post_df = batch_df.filter(
        (col("collection") == "app.bsky.feed.post")
        & (col("operation") == "create")
        & col("reply_root_uri").isNull()
    ).select(
        col("event_timestamp"),
        lit("original_post").alias("content_activity_type"),
    )

    reply_df = batch_df.filter(
        (col("collection") == "app.bsky.feed.post")
        & (col("operation") == "create")
        & col("reply_root_uri").isNotNull()
    ).select(
        col("event_timestamp"),
        lit("reply").alias("content_activity_type"),
    )

    post_update_df = batch_df.filter(
        (col("collection") == "app.bsky.feed.post")
        & (col("operation") == "update")
    ).select(
        col("event_timestamp"),
        lit("post_update").alias("content_activity_type"),
    )

    post_delete_df = batch_df.filter(
        (col("collection") == "app.bsky.feed.post")
        & (col("operation") == "delete")
    ).select(
        col("event_timestamp"),
        lit("post_delete").alias("content_activity_type"),
    )

    return (
        post_create_df
        .unionByName(original_post_df)
        .unionByName(reply_df)
        .unionByName(post_update_df)
        .unionByName(post_delete_df)
    )


def build_content_activity_aggregate(batch_df: DataFrame) -> DataFrame:
    """Tạo aggregate content activity theo phút và activity type."""
    return build_content_activity_events(batch_df).groupBy(
        date_trunc("minute", col("event_timestamp")).alias("window_start"),
        col("content_activity_type"),
    ).agg(
        count(lit(1)).alias("activity_count")
    )


def build_engagement_events(batch_df: DataFrame) -> DataFrame:
    """Tạo các event engagement từ một micro-batch đã parse."""
    like_df = batch_df.filter(
        (col("collection") == "app.bsky.feed.like")
        & (col("operation") == "create")
    ).select(
        col("event_timestamp"),
        lit("like").alias("engagement_type"),
    )

    repost_df = batch_df.filter(
        (col("collection") == "app.bsky.feed.repost")
        & (col("operation") == "create")
    ).select(
        col("event_timestamp"),
        lit("repost").alias("engagement_type"),
    )

    reply_df = batch_df.filter(
        (col("collection") == "app.bsky.feed.post")
        & (col("operation") == "create")
        & col("reply_root_uri").isNotNull()
    ).select(
        col("event_timestamp"),
        lit("reply").alias("engagement_type"),
    )

    return like_df.unionByName(repost_df).unionByName(reply_df)


def build_engagement_aggregate(batch_df: DataFrame) -> DataFrame:
    """Tạo aggregate engagement theo phút và engagement type."""
    return build_engagement_events(batch_df).groupBy(
        date_trunc("minute", col("event_timestamp")).alias("window_start"),
        col("engagement_type"),
    ).agg(
        count(lit(1)).alias("engagement_count")
    )


def build_network_activity_events(batch_df: DataFrame) -> DataFrame:
    """Tạo các event network activity từ một micro-batch đã parse."""
    follow_df = batch_df.filter(
        (col("collection") == "app.bsky.graph.follow")
        & (col("operation") == "create")
    ).select(
        col("event_timestamp"),
        lit("follow").alias("network_activity_type"),
    )

    unfollow_df = batch_df.filter(
        (col("collection") == "app.bsky.graph.follow")
        & (col("operation") == "delete")
    ).select(
        col("event_timestamp"),
        lit("unfollow").alias("network_activity_type"),
    )

    return follow_df.unionByName(unfollow_df)


def build_network_activity_aggregate(batch_df: DataFrame) -> DataFrame:
    """Tạo aggregate network activity theo phút và activity type."""
    return build_network_activity_events(batch_df).groupBy(
        date_trunc("minute", col("event_timestamp")).alias("window_start"),
        col("network_activity_type"),
    ).agg(
        count(lit(1)).alias("activity_count")
    )


def write_batch_to_clickhouse(batch_df: DataFrame, batch_id: int) -> None:
    """Aggregate một micro-batch và ghi các realtime metrics vào ClickHouse.

    Input là các event đã parse trong một micro-batch Spark.
    Output là các dòng aggregate theo phút trong các bảng ClickHouse realtime.
    """
    if batch_df.rdd.isEmpty():
        print(f"batch_id={batch_id}: empty batch")
        return

    event_volume_rows = insert_aggregate_to_clickhouse(
        batch_df=build_event_volume_aggregate(batch_df),
        table_name=GOLD_EVENT_VOLUME_1M_STREAM_TABLE,
        metric_column="event_type",
        count_column="event_count",
        batch_id=batch_id,
    )

    content_activity_rows = insert_aggregate_to_clickhouse(
        batch_df=build_content_activity_aggregate(batch_df),
        table_name=GOLD_CONTENT_ACTIVITY_1M_STREAM_TABLE,
        metric_column="content_activity_type",
        count_column="activity_count",
        batch_id=batch_id,
    )

    engagement_rows = insert_aggregate_to_clickhouse(
        batch_df=build_engagement_aggregate(batch_df),
        table_name=GOLD_ENGAGEMENT_1M_STREAM_TABLE,
        metric_column="engagement_type",
        count_column="engagement_count",
        batch_id=batch_id,
    )

    network_activity_rows = insert_aggregate_to_clickhouse(
        batch_df=build_network_activity_aggregate(batch_df),
        table_name=GOLD_NETWORK_ACTIVITY_1M_STREAM_TABLE,
        metric_column="network_activity_type",
        count_column="activity_count",
        batch_id=batch_id,
    )

    print(
        f"batch_id={batch_id}: "
        f"event_volume_rows={event_volume_rows} "
        f"content_activity_rows={content_activity_rows} "
        f"engagement_rows={engagement_rows} "
        f"network_activity_rows={network_activity_rows}"
    )


def main() -> None:
    """Đọc Kafka stream, aggregate realtime metrics và ghi vào ClickHouse."""
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
            build_commit_envelope_schema(POST_RECORD_SCHEMA),
        ).alias("event"),
        col("kafka_timestamp"),
    )

    # Map collection/operation thành event_type phục vụ dashboard.
    typed_events_df = parsed_df.select(
        col("event.event_kind").alias("event_kind"),
        col("event.collection").alias("collection"),
        col("event.operation").alias("operation"),
        col("event.payload.commit.record.reply.root.uri").alias("reply_root_uri"),
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

    # Ghi từng micro-batch vào ClickHouse.
    query = (
        typed_events_df.writeStream
        .foreachBatch(write_batch_to_clickhouse)
        .option(
            "checkpointLocation",
            GOLD_REALTIME_METRICS_1M_STREAM_CHECKPOINT_LOCATION,
        )
        .outputMode("append")
        .trigger(processingTime="60 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
