"""Kiểm tra tổng quan các bảng Silver v1 trên MinIO."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from bluesky_pipeline.silver_tables import (
    SILVER_DELETED_RECORDS_PATH,
    SILVER_ENGAGEMENTS_PATH,
    SILVER_FOLLOWS_PATH,
    SILVER_POSTS_PATH,
)
from bluesky_pipeline.spark_session import create_spark_session


SILVER_TABLES = {
    "silver_posts": SILVER_POSTS_PATH,
    "silver_engagements": SILVER_ENGAGEMENTS_PATH,
    "silver_follows": SILVER_FOLLOWS_PATH,
    "silver_deleted_records": SILVER_DELETED_RECORDS_PATH,
}


def read_table(spark: SparkSession, table_name: str) -> DataFrame:
    """Đọc một bảng Silver từ MinIO theo tên bảng.

    Input chính là SparkSession và tên bảng trong SILVER_TABLES.
    Output là DataFrame của bảng Silver tương ứng.
    """
    return spark.read.parquet(SILVER_TABLES[table_name])


def print_table_count(spark: SparkSession, table_name: str) -> None:
    """In số dòng của một bảng Silver để kiểm tra bảng có dữ liệu."""
    table_df = read_table(spark, table_name)
    print(f"{table_name}_count: {table_df.count()}")


def show_posts_checks(spark: SparkSession) -> None:
    """In các kiểm tra riêng cho silver_posts."""
    posts_df = read_table(spark, "silver_posts")

    # Kiểm tra phân bố create/update và reply/non-reply.
    posts_df.groupBy("operation").count().orderBy(col("operation")).show(
        truncate=False
    )
    posts_df.groupBy("is_reply").count().orderBy(col("is_reply")).show(
        truncate=False
    )


def show_engagement_checks(spark: SparkSession) -> None:
    """In các kiểm tra riêng cho silver_engagements."""
    engagements_df = read_table(spark, "silver_engagements")

    # Kiểm tra phân bố like/repost và target post URI.
    engagements_df.groupBy("engagement_type").count().orderBy(
        col("engagement_type")
    ).show(truncate=False)
    engagements_df.groupBy(
        col("subject_uri").isNotNull().alias("has_subject_uri")
    ).count().show(truncate=False)


def show_follow_checks(spark: SparkSession) -> None:
    """In các kiểm tra riêng cho silver_follows."""
    follows_df = read_table(spark, "silver_follows")

    # Kiểm tra target actor DID của follow event.
    follows_df.groupBy(
        col("target_actor_did").isNotNull().alias("has_target_actor_did")
    ).count().show(truncate=False)


def show_deleted_record_checks(spark: SparkSession) -> None:
    """In các kiểm tra riêng cho silver_deleted_records."""
    deleted_records_df = read_table(spark, "silver_deleted_records")

    # Kiểm tra delete event theo collection và record URI.
    deleted_records_df.groupBy("collection").count().orderBy("collection").show(
        truncate=False
    )
    deleted_records_df.groupBy(
        col("record_uri").isNotNull().alias("has_record_uri")
    ).count().show(truncate=False)


def main() -> None:
    """Đọc toàn bộ Silver v1 và in các kiểm tra tổng quan."""
    # Tạo SparkSession local có cấu hình đọc MinIO.
    spark = create_spark_session("bluesky-check-silver-v1")
    spark.sparkContext.setLogLevel("WARN")

    # In count tổng cho từng bảng Silver.
    for table_name in SILVER_TABLES:
        print_table_count(spark, table_name)

    show_posts_checks(spark)
    show_engagement_checks(spark)
    show_follow_checks(spark)
    show_deleted_record_checks(spark)


if __name__ == "__main__":
    main()
