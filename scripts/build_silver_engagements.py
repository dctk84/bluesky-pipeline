"""Tạo bảng Silver engagements từ Bronze commit events trên MinIO."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, concat, from_json, lit, when
from bluesky_pipeline.bronze_schemas import (
    ENGAGEMENT_RECORD_SCHEMA,
    build_commit_envelope_schema,
)
from bluesky_pipeline.silver_tables import SILVER_ENGAGEMENTS_PATH

from bluesky_pipeline.spark_session import create_spark_session


BRONZE_COMMIT_PATH = "s3a://bluesky-lake/bronze/bluesky_commit_events"

ENVELOPE_SCHEMA = build_commit_envelope_schema(ENGAGEMENT_RECORD_SCHEMA)

def read_bronze_commit_events(spark: SparkSession) -> DataFrame:
    """Đọc Bronze commit events từ MinIO.

    Input chính là SparkSession đã cấu hình S3A.
    Output là DataFrame chứa raw commit events từ Bronze.
    """
    return spark.read.parquet(BRONZE_COMMIT_PATH)


def build_silver_engagements(bronze_df: DataFrame) -> DataFrame:
    """Chuẩn hóa like/repost create events thành Silver engagements.

    Input chính là Bronze commit DataFrame có cột message_value.
    Output là DataFrame gồm like/repost create với các cột Silver v1.
    """
    parsed_df = bronze_df.withColumn(
        "envelope",
        from_json(col("message_value"), ENVELOPE_SCHEMA),
    )

    engagements_df = parsed_df.filter(
        (col("collection").isin("app.bsky.feed.like", "app.bsky.feed.repost"))
        & (col("operation") == "create")
    )

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


def write_silver_engagements(silver_engagements_df: DataFrame) -> None:
    """Ghi Silver engagements xuống MinIO dạng Parquet.

    Input chính là DataFrame Silver engagements đã chuẩn hóa.
    Output là dữ liệu Parquet ở path Silver engagements.
    """
    # Ghi overwrite vì đây là batch build local có thể chạy lại từ Bronze.
    silver_engagements_df.write.mode("overwrite").parquet(SILVER_ENGAGEMENTS_PATH)


def main() -> None:
    """Build Silver engagements từ Bronze commit events và in count kiểm chứng."""
    # Tạo SparkSession local có cấu hình đọc/ghi MinIO.
    spark = create_spark_session("bluesky-build-silver-engagements")
    spark.sparkContext.setLogLevel("WARN")

    # Đọc Bronze, transform sang Silver engagements và ghi ra MinIO.
    bronze_df = read_bronze_commit_events(spark)
    silver_engagements_df = build_silver_engagements(bronze_df)
    write_silver_engagements(silver_engagements_df)

    print(f"silver_engagements_count: {silver_engagements_df.count()}")
    silver_engagements_df.groupBy("engagement_type").count().show(truncate=False)
    silver_engagements_df.show(10, truncate=False)


if __name__ == "__main__":
    main()
