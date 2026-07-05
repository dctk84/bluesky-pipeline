"""Tạo bảng Silver deleted records từ Bronze commit events trên MinIO."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, concat, from_json, lit
from bluesky_pipeline.bronze_schemas import build_commit_envelope_schema
from bluesky_pipeline.silver_tables import SILVER_DELETED_RECORDS_PATH

from bluesky_pipeline.spark_session import create_spark_session


BRONZE_COMMIT_PATH = "s3a://bluesky-lake/bronze/bluesky_commit_events"

ENVELOPE_SCHEMA = build_commit_envelope_schema()

def read_bronze_commit_events(spark: SparkSession) -> DataFrame:
    """Đọc Bronze commit events từ MinIO.

    Input chính là SparkSession đã cấu hình S3A.
    Output là DataFrame chứa raw commit events từ Bronze.
    """
    return spark.read.parquet(BRONZE_COMMIT_PATH)


def build_silver_deleted_records(bronze_df: DataFrame) -> DataFrame:
    """Chuẩn hóa delete events thành Silver deleted records.

    Input chính là Bronze commit DataFrame có cột message_value.
    Output là DataFrame gồm các record delete từ mọi collection trong scope.
    """
    parsed_df = bronze_df.withColumn(
        "envelope",
        from_json(col("message_value"), ENVELOPE_SCHEMA),
    )

    deleted_df = parsed_df.filter(col("operation") == "delete")

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


def write_silver_deleted_records(silver_deleted_records_df: DataFrame) -> None:
    """Ghi Silver deleted records xuống MinIO dạng Parquet.

    Input chính là DataFrame Silver deleted records đã chuẩn hóa.
    Output là dữ liệu Parquet ở path Silver deleted records.
    """
    # Ghi overwrite vì đây là batch build local có thể chạy lại từ Bronze.
    silver_deleted_records_df.write.mode("overwrite").parquet(
        SILVER_DELETED_RECORDS_PATH
    )


def main() -> None:
    """Build Silver deleted records từ Bronze commit events và in count kiểm chứng."""
    # Tạo SparkSession local có cấu hình đọc/ghi MinIO.
    spark = create_spark_session("bluesky-build-silver-deleted-records")
    spark.sparkContext.setLogLevel("WARN")

    # Đọc Bronze, transform sang Silver deleted records và ghi ra MinIO.
    bronze_df = read_bronze_commit_events(spark)
    silver_deleted_records_df = build_silver_deleted_records(bronze_df)
    write_silver_deleted_records(silver_deleted_records_df)

    print(f"silver_deleted_records_count: {silver_deleted_records_df.count()}")
    silver_deleted_records_df.groupBy("collection").count().show(truncate=False)
    silver_deleted_records_df.show(10, truncate=False)


if __name__ == "__main__":
    main()
