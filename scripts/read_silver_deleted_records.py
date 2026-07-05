"""Đọc Silver deleted records trên MinIO để kiểm chứng dữ liệu đã chuẩn hóa."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from bluesky_pipeline.spark_session import create_spark_session


SILVER_DELETED_RECORDS_PATH = "s3a://bluesky-lake/silver/silver_deleted_records"


def read_silver_deleted_records(spark: SparkSession) -> DataFrame:
    """Đọc bảng Silver deleted records từ MinIO.

    Input chính là SparkSession đã cấu hình S3A.
    Output là DataFrame chứa dữ liệu Silver deleted records.
    """
    return spark.read.parquet(SILVER_DELETED_RECORDS_PATH)


def show_silver_deleted_records_summary(
    silver_deleted_records_df: DataFrame,
) -> None:
    """In schema, count và vài thống kê cơ bản của Silver deleted records."""
    silver_deleted_records_df.printSchema()
    print(f"silver_deleted_records_count: {silver_deleted_records_df.count()}")

    # Đếm delete events theo collection để kiểm tra mọi collection đều được ghi nhận.
    silver_deleted_records_df.groupBy("collection").count().orderBy(
        col("collection")
    ).show(truncate=False)

    # Kiểm tra record_uri vì đây là định danh record bị xóa.
    silver_deleted_records_df.groupBy(
        col("record_uri").isNotNull().alias("has_record_uri")
    ).count().show(truncate=False)

    silver_deleted_records_df.select(
        "record_uri",
        "repository_did",
        "collection",
        "rkey",
    ).show(10, truncate=False)


def main() -> None:
    """Đọc Silver deleted records và in summary để kiểm chứng dữ liệu usable."""
    # Tạo SparkSession local có cấu hình đọc MinIO.
    spark = create_spark_session("bluesky-read-silver-deleted-records")
    spark.sparkContext.setLogLevel("WARN")

    # Đọc Silver deleted records và in các kiểm tra cơ bản.
    silver_deleted_records_df = read_silver_deleted_records(spark)
    show_silver_deleted_records_summary(silver_deleted_records_df)


if __name__ == "__main__":
    main()