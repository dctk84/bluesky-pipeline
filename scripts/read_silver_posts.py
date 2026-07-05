"""Đọc Silver posts trên MinIO để kiểm chứng dữ liệu đã chuẩn hóa."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from bluesky_pipeline.spark_session import create_spark_session


SILVER_POSTS_PATH = "s3a://bluesky-lake/silver/silver_posts"


def read_silver_posts(spark: SparkSession) -> DataFrame:
    """Đọc bảng Silver posts từ MinIO.

    Input chính là SparkSession đã cấu hình S3A.
    Output là DataFrame chứa dữ liệu Silver posts.
    """
    return spark.read.parquet(SILVER_POSTS_PATH)


def show_silver_posts_summary(silver_posts_df: DataFrame) -> None:
    """In schema, count và vài thống kê cơ bản của Silver posts."""
    silver_posts_df.printSchema()
    print(f"silver_posts_count: {silver_posts_df.count()}")

    # Đếm post theo operation để kiểm tra create/update đã được đưa vào Silver.
    silver_posts_df.groupBy("operation").count().orderBy(
        col("operation")
    ).show(truncate=False)

    # Đếm reply/non-reply để kiểm tra cột dẫn xuất is_reply.
    silver_posts_df.groupBy("is_reply").count().orderBy(
        col("is_reply")
    ).show(truncate=False)

    silver_posts_df.select(
        "post_uri",
        "author_did",
        "operation",
        "text_length",
        "is_reply",
        "reply_root_uri",
    ).show(10, truncate=False)


def main() -> None:
    """Đọc Silver posts và in summary để kiểm chứng dữ liệu usable."""
    # Tạo SparkSession local có cấu hình đọc MinIO.
    spark = create_spark_session("bluesky-read-silver-posts")
    spark.sparkContext.setLogLevel("WARN")

    # Đọc Silver posts và in các kiểm tra cơ bản.
    silver_posts_df = read_silver_posts(spark)
    show_silver_posts_summary(silver_posts_df)


if __name__ == "__main__":
    main()