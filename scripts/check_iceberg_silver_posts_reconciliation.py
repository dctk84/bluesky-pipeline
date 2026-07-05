"""Reconcile Silver posts Parquet v1 với Silver posts Iceberg."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import count, sum as spark_sum

from bluesky_pipeline.iceberg_config import (
    ICEBERG_SILVER_POSTS_TABLE,
    create_iceberg_spark_session,
)
from bluesky_pipeline.silver_tables import SILVER_POSTS_PATH


def read_parquet_silver_posts(spark) -> DataFrame:
    """Đọc Silver posts Parquet v1 từ MinIO.

    Input chính là SparkSession có cấu hình S3A.
    Output là DataFrame Silver posts bản Parquet hiện tại.
    """
    return spark.read.parquet(SILVER_POSTS_PATH)


def read_iceberg_silver_posts(spark) -> DataFrame:
    """Đọc Silver posts Iceberg từ catalog.

    Input chính là SparkSession có cấu hình Iceberg catalog.
    Output là DataFrame Silver posts bản Iceberg.
    """
    return spark.table(ICEBERG_SILVER_POSTS_TABLE)


def build_metrics(posts_df: DataFrame) -> dict[str, int]:
    """Tính các metric kiểm chứng cho Silver posts.

    Input chính là DataFrame Silver posts.
    Output là dict chứa row count và các tổng metric quan trọng.
    """
    metrics_row = posts_df.agg(
        count("*").alias("row_count"),
        spark_sum("text_length").alias("text_length_sum"),
        spark_sum(posts_df.is_reply.cast("int")).alias("reply_count"),
    ).collect()[0]

    return {
        "row_count": int(metrics_row.row_count or 0),
        "text_length_sum": int(metrics_row.text_length_sum or 0),
        "reply_count": int(metrics_row.reply_count or 0),
    }


def print_reconciliation(expected: dict[str, int], actual: dict[str, int]) -> None:
    """In kết quả reconciliation giữa Parquet và Iceberg."""
    has_mismatch = False

    print("metric\tparquet\ticeberg\tstatus")

    for metric in sorted(set(expected) | set(actual)):
        expected_value = expected.get(metric, 0)
        actual_value = actual.get(metric, 0)
        status = "OK" if expected_value == actual_value else "MISMATCH"

        if status != "OK":
            has_mismatch = True

        print(f"{metric}\t{expected_value}\t{actual_value}\t{status}")

    if has_mismatch:
        raise SystemExit("Iceberg Silver posts reconciliation failed")

    print("Iceberg Silver posts reconciliation passed")


def main() -> None:
    """So sánh Silver posts Parquet v1 với Iceberg table."""
    # Dùng Iceberg SparkSession để đọc được cả Parquet path và Iceberg catalog.
    spark = create_iceberg_spark_session("bluesky-check-iceberg-silver-posts")
    spark.sparkContext.setLogLevel("WARN")

    parquet_df = read_parquet_silver_posts(spark)
    iceberg_df = read_iceberg_silver_posts(spark)

    parquet_metrics = build_metrics(parquet_df)
    iceberg_metrics = build_metrics(iceberg_df)

    print_reconciliation(parquet_metrics, iceberg_metrics)


if __name__ == "__main__":
    main()
