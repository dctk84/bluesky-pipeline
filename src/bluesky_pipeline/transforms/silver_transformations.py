"""Transformation dùng chung để build Silver v1 từ Bronze commit events."""

from collections.abc import Callable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, concat, from_json, length, lit, when

from bluesky_pipeline.schemas.bronze_schemas import (
    BRONZE_COMMIT_EVENTS_PARQUET_SCHEMA,
    ENGAGEMENT_RECORD_SCHEMA,
    FOLLOW_RECORD_SCHEMA,
    POST_RECORD_SCHEMA,
    build_commit_envelope_schema,
)
from bluesky_pipeline.schemas.bronze_tables import BRONZE_COMMIT_EVENTS_PATH


# Mỗi family dùng schema record riêng để parse đúng phần payload.commit.record.
POST_ENVELOPE_SCHEMA = build_commit_envelope_schema(POST_RECORD_SCHEMA)
ENGAGEMENT_ENVELOPE_SCHEMA = build_commit_envelope_schema(ENGAGEMENT_RECORD_SCHEMA)
FOLLOW_ENVELOPE_SCHEMA = build_commit_envelope_schema(FOLLOW_RECORD_SCHEMA)
DELETE_ENVELOPE_SCHEMA = build_commit_envelope_schema()


def read_bronze_commit_events(spark: SparkSession) -> DataFrame:
    """Đọc Bronze commit events từ MinIO.

    Input chính là SparkSession đã cấu hình S3A.
    Output là DataFrame chứa raw commit events từ Bronze.
    """
    return spark.read.parquet(BRONZE_COMMIT_EVENTS_PATH)


def read_bronze_commit_events_stream(spark: SparkSession) -> DataFrame:
    """Đọc Bronze commit events dạng streaming từ MinIO.

    Input chính là SparkSession đã cấu hình S3A.
    Output là streaming DataFrame chứa raw commit events mới ghi vào Bronze.
    """
    return (
        spark.readStream
        .schema(BRONZE_COMMIT_EVENTS_PARQUET_SCHEMA)
        .parquet(BRONZE_COMMIT_EVENTS_PATH)
    )


def build_silver_posts(bronze_df: DataFrame) -> DataFrame:
    """Chuẩn hóa post create/update events thành Silver posts."""
    # Post cần schema riêng vì record chứa text và reply metadata.
    parsed_df = bronze_df.withColumn(
        "envelope",
        from_json(col("message_value"), POST_ENVELOPE_SCHEMA),
    )

    posts_df = parsed_df.filter(
        (col("collection") == "app.bsky.feed.post")
        & (col("operation").isin("create", "update"))
    )

    # post_uri được dựng theo AT URI để downstream join với engagement subject_uri.
    return posts_df.select(
        concat(
            lit("at://"),
            col("repository_did"),
            lit("/app.bsky.feed.post/"),
            col("envelope.payload.commit.rkey"),
        ).alias("post_uri"),
        col("repository_did").alias("author_did"),
        col("operation"),
        col("envelope.payload.commit.rkey").alias("rkey"),
        col("envelope.payload.commit.cid").alias("cid"),
        col("envelope.payload.commit.record.$type").alias("record_type"),
        col("envelope.payload.commit.record.createdAt").alias("record_created_at"),
        col("received_at"),
        col("jetstream_time_us"),
        col("envelope.payload.commit.record.text").alias("text"),
        length(col("envelope.payload.commit.record.text")).alias("text_length"),
        col("envelope.payload.commit.record.reply.root.uri")
        .isNotNull()
        .alias("is_reply"),
        col("envelope.payload.commit.record.reply.root.uri").alias("reply_root_uri"),
        col("envelope.payload.commit.record.reply.parent.uri").alias(
            "reply_parent_uri"
        ),
        col("ingest_date"),
        col("ingest_hour"),
    )


def build_silver_engagements(bronze_df: DataFrame) -> DataFrame:
    """Chuẩn hóa like/repost create events thành Silver engagements."""
    # Like và repost có cùng shape subject nên gom chung thành một bảng engagement.
    parsed_df = bronze_df.withColumn(
        "envelope",
        from_json(col("message_value"), ENGAGEMENT_ENVELOPE_SCHEMA),
    )

    engagements_df = parsed_df.filter(
        (col("collection").isin("app.bsky.feed.like", "app.bsky.feed.repost"))
        & (col("operation") == "create")
    )

    # engagement_type chuẩn hóa collection dài thành metric key ngắn cho Gold.
    return engagements_df.select(
        concat(
            lit("at://"),
            col("repository_did"),
            lit("/"),
            col("collection"),
            lit("/"),
            col("envelope.payload.commit.rkey"),
        ).alias("engagement_uri"),
        col("repository_did").alias("actor_did"),
        when(col("collection") == "app.bsky.feed.like", lit("like"))
        .when(col("collection") == "app.bsky.feed.repost", lit("repost"))
        .alias("engagement_type"),
        col("operation"),
        col("envelope.payload.commit.rkey").alias("rkey"),
        col("envelope.payload.commit.cid").alias("cid"),
        col("envelope.payload.commit.record.$type").alias("record_type"),
        col("envelope.payload.commit.record.createdAt").alias("record_created_at"),
        col("received_at"),
        col("jetstream_time_us"),
        col("envelope.payload.commit.record.subject.uri").alias("subject_uri"),
        col("envelope.payload.commit.record.subject.cid").alias("subject_cid"),
        col("ingest_date"),
        col("ingest_hour"),
    )


def build_silver_follows(bronze_df: DataFrame) -> DataFrame:
    """Chuẩn hóa follow create events thành Silver follows."""
    # Follow record chứa subject là DID của account được follow.
    parsed_df = bronze_df.withColumn(
        "envelope",
        from_json(col("message_value"), FOLLOW_ENVELOPE_SCHEMA),
    )

    follows_df = parsed_df.filter(
        (col("collection") == "app.bsky.graph.follow")
        & (col("operation") == "create")
    )

    return follows_df.select(
        concat(
            lit("at://"),
            col("repository_did"),
            lit("/app.bsky.graph.follow/"),
            col("envelope.payload.commit.rkey"),
        ).alias("follow_uri"),
        col("repository_did").alias("actor_did"),
        col("envelope.payload.commit.record.subject").alias("target_actor_did"),
        col("operation"),
        col("envelope.payload.commit.rkey").alias("rkey"),
        col("envelope.payload.commit.cid").alias("cid"),
        col("envelope.payload.commit.record.$type").alias("record_type"),
        col("envelope.payload.commit.record.createdAt").alias("record_created_at"),
        col("received_at"),
        col("jetstream_time_us"),
        col("ingest_date"),
        col("ingest_hour"),
    )


def build_silver_deleted_records(bronze_df: DataFrame) -> DataFrame:
    """Chuẩn hóa delete events thành Silver deleted records."""
    # Delete event thường không có record body đầy đủ, nên chỉ parse envelope chung.
    parsed_df = bronze_df.withColumn(
        "envelope",
        from_json(col("message_value"), DELETE_ENVELOPE_SCHEMA),
    )

    deleted_df = parsed_df.filter(col("operation") == "delete")

    # record_uri giữ lại identity của record bị xóa để phục vụ audit/reconciliation.
    return deleted_df.select(
        concat(
            lit("at://"),
            col("repository_did"),
            lit("/"),
            col("collection"),
            lit("/"),
            col("envelope.payload.commit.rkey"),
        ).alias("record_uri"),
        col("repository_did"),
        col("collection"),
        col("operation"),
        col("envelope.payload.commit.rkey").alias("rkey"),
        col("received_at"),
        col("jetstream_time_us"),
        col("ingest_date"),
        col("ingest_hour"),
    )


SILVER_TRANSFORMATIONS: dict[str, Callable[[DataFrame], DataFrame]] = {
    # Registry này cho phép build/check Silver Iceberg iterate theo table contract.
    "silver_posts": build_silver_posts,
    "silver_engagements": build_silver_engagements,
    "silver_follows": build_silver_follows,
    "silver_deleted_records": build_silver_deleted_records,
}
