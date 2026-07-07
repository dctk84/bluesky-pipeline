"""Đọc dữ liệu Bronze Parquet trên MinIO để kiểm chứng dữ liệu usable."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from bluesky_pipeline.bronze_tables import BRONZE_EVENT_PATHS
from bluesky_pipeline.spark_session import create_spark_session


def read_bronze_events(spark: SparkSession, event_kind: str) -> DataFrame:
    """Đọc một nhóm Bronze event từ MinIO theo event kind.

    Input chính là SparkSession và event kind cần đọc.
    Output là DataFrame chứa dữ liệu Bronze tương ứng.
    """
    return spark.read.parquet(BRONZE_EVENT_PATHS[event_kind])


def show_bronze_summary(bronze_df: DataFrame, event_kind: str) -> None:
    """In schema, sample rows và count cho một nhóm Bronze event."""
    print(f"\n=== {event_kind} bronze schema ===")
    bronze_df.printSchema()

    print(f"\n=== {event_kind} bronze sample ===")
    bronze_df.show(10, truncate=False)

    print(f"{event_kind}_count: {bronze_df.count()}")


def main() -> None:
    """Đọc ba Bronze path trên MinIO và in summary để kiểm chứng dữ liệu."""
    # Bước 1: Tạo SparkSession có cấu hình S3A để đọc MinIO.
    spark = create_spark_session("bluesky-read-bronze-parquet")
    spark.sparkContext.setLogLevel("WARN")

    # Bước 2: Đọc và kiểm chứng commit events.
    commit_df = read_bronze_events(spark, "commit")
    show_bronze_summary(commit_df, "commit")

    # Bước 3: Đếm commit events theo collection để kiểm tra partition collection.
    commit_df.groupBy("collection").count().orderBy(col("count").desc()).show(
        truncate=False
    )

    # Bước 4: Đọc và kiểm chứng identity events.
    identity_df = read_bronze_events(spark, "identity")
    show_bronze_summary(identity_df, "identity")

    # Bước 5: Đọc và kiểm chứng account events.
    account_df = read_bronze_events(spark, "account")
    show_bronze_summary(account_df, "account")


if __name__ == "__main__":
    main()
