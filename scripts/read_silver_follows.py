"""Đọc Silver follows trên MinIO để kiểm chứng dữ liệu đã chuẩn hóa."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from bluesky_pipeline.spark_session import create_spark_session


SILVER_FOLLOWS_PATH = "s3a://bluesky-lake/silver/silver_follows"


def read_silver_follows(spark: SparkSession) -> DataFrame:
    """Đọc bảng Silver follows từ MinIO.

    Input chính là SparkSession đã cấu hình S3A.
    Output là DataFrame chứa dữ liệu Silver follows.
    """
    return spark.read.parquet(SILVER_FOLLOWS_PATH)


def show_silver_follows_summary(silver_follows_df: DataFrame) -> None:
    """In schema, count và vài thống kê cơ bản của Silver follows."""
    silver_follows_df.printSchema()
    print(f"silver_follows_count: {silver_follows_df.count()}")

    # Kiểm tra target_actor_did vì đây là actor được follow.
    silver_follows_df.groupBy(
        col("target_actor_did").isNotNull().alias("has_target_actor_did")
    ).count().show(truncate=False)

    silver_follows_df.select(
        "follow_uri",
        "actor_did",
        "target_actor_did",
        "record_created_at",
    ).show(10, truncate=False)


def main() -> None:
    """Đọc Silver follows và in summary để kiểm chứng dữ liệu usable."""
    # Tạo SparkSession local có cấu hình đọc MinIO.
    spark = create_spark_session("bluesky-read-silver-follows")
    spark.sparkContext.setLogLevel("WARN")

    # Đọc Silver follows và in các kiểm tra cơ bản.
    silver_follows_df = read_silver_follows(spark)
    show_silver_follows_summary(silver_follows_df)


if __name__ == "__main__":
    main()