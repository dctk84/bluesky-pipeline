"""Profile Bronze commit events trên MinIO để chuẩn bị thiết kế Silver schema."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import BooleanType, LongType, StringType, StructField, StructType

from bluesky_pipeline.bronze_tables import BRONZE_COMMIT_EVENTS_PATH
from bluesky_pipeline.spark_session import create_spark_session


SUBJECT_SCHEMA = StructType(
    [
        StructField("uri", StringType()),
        StructField("cid", StringType()),
    ]
)

REPLY_REF_SCHEMA = StructType(
    [
        StructField("uri", StringType()),
        StructField("cid", StringType()),
    ]
)

REPLY_SCHEMA = StructType(
    [
        StructField("root", REPLY_REF_SCHEMA),
        StructField("parent", REPLY_REF_SCHEMA),
    ]
)

RECORD_SCHEMA = StructType(
    [
        StructField("$type", StringType()),
        StructField("createdAt", StringType()),
        StructField("text", StringType()),
        StructField("subject", SUBJECT_SCHEMA),
        StructField("reply", REPLY_SCHEMA),
    ]
)

COMMIT_SCHEMA = StructType(
    [
        StructField("operation", StringType()),
        StructField("collection", StringType()),
        StructField("rkey", StringType()),
        StructField("cid", StringType()),
        StructField("rev", StringType()),
        StructField("record", RECORD_SCHEMA),
    ]
)

PAYLOAD_SCHEMA = StructType(
    [
        StructField("did", StringType()),
        StructField("time_us", LongType()),
        StructField("kind", StringType()),
        StructField("commit", COMMIT_SCHEMA),
    ]
)

ENVELOPE_SCHEMA = StructType(
    [
        StructField("schema_version", LongType()),
        StructField("source", StringType()),
        StructField("event_kind", StringType()),
        StructField("received_at", StringType()),
        StructField("collection", StringType()),
        StructField("operation", StringType()),
        StructField("repository_did", StringType()),
        StructField("jetstream_time_us", LongType()),
        StructField("payload", PAYLOAD_SCHEMA),
    ]
)


def read_commit_events(spark: SparkSession) -> DataFrame:
    """Đọc Bronze commit events từ MinIO.

    Input chính là SparkSession đã cấu hình S3A.
    Output là DataFrame chứa commit events ở tầng Bronze.
    """
    return spark.read.parquet(BRONZE_COMMIT_EVENTS_PATH)

def parse_commit_payload(commit_df: DataFrame) -> DataFrame:
    """Parse message_value thành các cột nested quan trọng của commit event.

    Input chính là Bronze commit DataFrame có cột message_value.
    Output là DataFrame phẳng hơn để profile field cho Silver schema.
    """
    parsed_df = commit_df.withColumn(
        "envelope",
        from_json(col("message_value"), ENVELOPE_SCHEMA),
    )

    return parsed_df.select(
        col("collection"),
        col("operation"),
        col("repository_did"),
        col("envelope.payload.commit.rkey").alias("rkey"),
        col("envelope.payload.commit.cid").alias("cid"),
        col("envelope.payload.commit.record.$type").alias("record_type"),
        col("envelope.payload.commit.record.createdAt").alias("record_created_at"),
        col("envelope.payload.commit.record.text").alias("text"),
        col("envelope.payload.commit.record.subject.uri").alias("subject_uri"),
        col("envelope.payload.commit.record.subject.cid").alias("subject_cid"),
        col("envelope.payload.commit.record.reply.root.uri").alias("reply_root_uri"),
        col("envelope.payload.commit.record.reply.parent.uri").alias("reply_parent_uri"),
    )

def show_commit_profile(commit_df: DataFrame) -> None:
    """In các thống kê cơ bản để hiểu phân bố commit events."""
    # Đếm tổng số commit event hiện có trong Bronze.
    print(f"commit_count: {commit_df.count()}")

    # Đếm theo collection và operation để chuẩn bị tách schema Silver.
    commit_df.groupBy("collection", "operation").count().orderBy(
        col("collection"),
        col("operation"),
    ).show(truncate=False)

    # In một vài record mẫu theo từng collection để quan sát raw JSON.
    for collection in [
        "app.bsky.feed.post",
        "app.bsky.feed.like",
        "app.bsky.feed.repost",
        "app.bsky.graph.follow",
    ]:
        print(f"\n=== sample: {collection} ===")
        commit_df.filter(col("collection") == collection).select(
            "collection",
            "operation",
            "repository_did",
            "message_value",
        ).show(3, truncate=False)

    parsed_df = parse_commit_payload(commit_df)

    print("\n=== parsed field availability ===")
    parsed_df.select(
        col("collection"),
        col("operation"),
        col("rkey").isNotNull().alias("has_rkey"),
        col("cid").isNotNull().alias("has_cid"),
        col("record_type").isNotNull().alias("has_record_type"),
        col("record_created_at").isNotNull().alias("has_record_created_at"),
        col("text").isNotNull().alias("has_text"),
        col("subject_uri").isNotNull().alias("has_subject_uri"),
        col("reply_root_uri").isNotNull().alias("has_reply_root_uri"),
    ).groupBy(
        "collection",
        "operation",
        "has_rkey",
        "has_cid",
        "has_record_type",
        "has_record_created_at",
        "has_text",
        "has_subject_uri",
        "has_reply_root_uri",
    ).count().orderBy("collection", "operation").show(100, truncate=False)


def main() -> None:
    """Đọc Bronze commit events và in profile phục vụ thiết kế Silver."""
    # Tạo SparkSession local có cấu hình đọc MinIO.
    spark = create_spark_session("bluesky-profile-bronze-commit-events")
    spark.sparkContext.setLogLevel("WARN")

    # Đọc và profile dữ liệu commit events.
    commit_df = read_commit_events(spark)
    show_commit_profile(commit_df)


if __name__ == "__main__":
    main()
