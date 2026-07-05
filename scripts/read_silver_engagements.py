"""Đọc Silver engagements trên MinIO để kiểm chứng dữ liệu đã chuẩn hóa."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from bluesky_pipeline.spark_session import create_spark_session


SILVER_ENGAGEMENTS_PATH = "s3a://bluesky-lake/silver/silver_engagements"


def read_silver_engagements(spark: SparkSession) -> DataFrame:
    """Đọc bảng Silver engagements từ MinIO.

    Input chính là SparkSession đã cấu hình S3A.
    Output là DataFrame chứa dữ liệu Silver engagements.
    """
    return spark.read.parquet(SILVER_ENGAGEMENTS_PATH)


def show_silver_engagements_summary(silver_engagements_df: DataFrame) -> None:
    """In schema, count và vài thống kê cơ bản của Silver engagements."""
    silver_engagements_df.printSchema()
    print(f"silver_engagements_count: {silver_engagements_df.count()}")

    # Đếm engagement theo type để kiểm tra like/repost đã được chuẩn hóa đúng.
    silver_engagements_df.groupBy("engagement_type").count().orderBy(
        col("engagement_type")
    ).show(truncate=False)

    # Kiểm tra subject_uri vì đây là target post của like/repost.
    silver_engagements_df.groupBy(
        col("subject_uri").isNotNull().alias("has_subject_uri")
    ).count().show(truncate=False)

    silver_engagements_df.select(
        "engagement_uri",
        "actor_did",
        "engagement_type",
        "subject_uri",
        "subject_cid",
    ).show(10, truncate=False)


def main() -> None:
    """Đọc Silver engagements và in summary để kiểm chứng dữ liệu usable."""
    # Tạo SparkSession local có cấu hình đọc MinIO.
    spark = create_spark_session("bluesky-read-silver-engagements")
    spark.sparkContext.setLogLevel("WARN")

    # Đọc Silver engagements và in các kiểm tra cơ bản.
    silver_engagements_df = read_silver_engagements(spark)
    show_silver_engagements_summary(silver_engagements_df)


if __name__ == "__main__":
    main()