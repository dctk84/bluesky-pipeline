"""Tạo bảng Silver posts từ Bronze commit events trên MinIO."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, concat, from_json, length, lit
from pyspark.sql.types import LongType, StringType, StructField, StructType

from bluesky_pipeline.spark_session import create_spark_session


BRONZE_COMMIT_PATH = "s3a://bluesky-lake/bronze/bluesky_commit_events"
SILVER_POSTS_PATH = "s3a://bluesky-lake/silver/silver_posts"

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


def read_bronze_commit_events(spark: SparkSession) -> DataFrame:
    """Đọc Bronze commit events từ MinIO.

    Input chính là SparkSession đã cấu hình S3A.
    Output là DataFrame chứa raw commit events từ Bronze.
    """
    return spark.read.parquet(BRONZE_COMMIT_PATH)


def build_silver_posts(bronze_df: DataFrame) -> DataFrame:
    """Chuẩn hóa post create/update events thành Silver posts.

    Input chính là Bronze commit DataFrame có cột message_value.
    Output là DataFrame chỉ gồm post create/update với các cột Silver v1.
    """
    parsed_df = bronze_df.withColumn(
        "envelope",
        from_json(col("message_value"), ENVELOPE_SCHEMA),
    )

    posts_df = parsed_df.filter(
        (col("collection") == "app.bsky.feed.post")
        & (col("operation").isin("create", "update"))
    )

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
        col("envelope.payload.commit.record.reply.root.uri").isNotNull().alias("is_reply"),
        col("envelope.payload.commit.record.reply.root.uri").alias("reply_root_uri"),
        col("envelope.payload.commit.record.reply.parent.uri").alias("reply_parent_uri"),
        col("ingest_date"),
        col("ingest_hour"),
    )

def write_silver_posts(silver_posts_df: DataFrame) -> None:
    """Ghi Silver posts xuống MinIO dạng Parquet.

    Input chính là DataFrame Silver posts đã chuẩn hóa.
    Output là dữ liệu Parquet ở path Silver posts.
    """
    # Ghi overwrite vì đây là batch build local có thể chạy lại từ Bronze.
    silver_posts_df.write.mode("overwrite").parquet(SILVER_POSTS_PATH)


def main() -> None:
    """Build Silver posts từ Bronze commit events và in count kiểm chứng."""
    # Tạo SparkSession local có cấu hình đọc/ghi MinIO.
    spark = create_spark_session("bluesky-build-silver-posts")
    spark.sparkContext.setLogLevel("WARN")

    # Đọc Bronze, transform sang Silver posts và ghi ra MinIO.
    bronze_df = read_bronze_commit_events(spark)
    silver_posts_df = build_silver_posts(bronze_df)
    write_silver_posts(silver_posts_df)

    print(f"silver_posts_count: {silver_posts_df.count()}")
    silver_posts_df.show(10, truncate=False)


if __name__ == "__main__":
    main()