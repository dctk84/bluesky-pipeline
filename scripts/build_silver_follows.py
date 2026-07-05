"""Tạo bảng Silver follows từ Bronze commit events trên MinIO."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, concat, from_json, lit
from bluesky_pipeline.bronze_schemas import (
    FOLLOW_RECORD_SCHEMA,
    build_commit_envelope_schema,
)
from bluesky_pipeline.silver_tables import SILVER_FOLLOWS_PATH

from bluesky_pipeline.spark_session import create_spark_session


BRONZE_COMMIT_PATH = "s3a://bluesky-lake/bronze/bluesky_commit_events"

ENVELOPE_SCHEMA = build_commit_envelope_schema(FOLLOW_RECORD_SCHEMA)

def read_bronze_commit_events(spark: SparkSession) -> DataFrame:
    """Đọc Bronze commit events từ MinIO.

    Input chính là SparkSession đã cấu hình S3A.
    Output là DataFrame chứa raw commit events từ Bronze.
    """
    return spark.read.parquet(BRONZE_COMMIT_PATH)


def build_silver_follows(bronze_df: DataFrame) -> DataFrame:
    """Chuẩn hóa follow create events thành Silver follows.

    Input chính là Bronze commit DataFrame có cột message_value.
    Output là DataFrame gồm follow create với các cột Silver v1.
    """
    parsed_df = bronze_df.withColumn(
        "envelope",
        from_json(col("message_value"), ENVELOPE_SCHEMA),
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


def write_silver_follows(silver_follows_df: DataFrame) -> None:
    """Ghi Silver follows xuống MinIO dạng Parquet.

    Input chính là DataFrame Silver follows đã chuẩn hóa.
    Output là dữ liệu Parquet ở path Silver follows.
    """
    # Ghi overwrite vì đây là batch build local có thể chạy lại từ Bronze.
    silver_follows_df.write.mode("overwrite").parquet(SILVER_FOLLOWS_PATH)


def main() -> None:
    """Build Silver follows từ Bronze commit events và in count kiểm chứng."""
    # Tạo SparkSession local có cấu hình đọc/ghi MinIO.
    spark = create_spark_session("bluesky-build-silver-follows")
    spark.sparkContext.setLogLevel("WARN")

    # Đọc Bronze, transform sang Silver follows và ghi ra MinIO.
    bronze_df = read_bronze_commit_events(spark)
    silver_follows_df = build_silver_follows(bronze_df)
    write_silver_follows(silver_follows_df)

    print(f"silver_follows_count: {silver_follows_df.count()}")
    silver_follows_df.groupBy(
        col("target_actor_did").isNotNull().alias("has_target_actor_did")
    ).count().show(truncate=False)
    silver_follows_df.show(10, truncate=False)


if __name__ == "__main__":
    main()
