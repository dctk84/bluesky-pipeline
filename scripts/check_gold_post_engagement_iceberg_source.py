"""Reconcile Gold post engagement summary giữa Parquet source và Iceberg source."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import sum as spark_sum

from bluesky_pipeline.gold_tables import (
    GOLD_POST_ENGAGEMENT_SUMMARY_ICEBERG_SOURCE_PATH,
    GOLD_POST_ENGAGEMENT_SUMMARY_PATH,
)
from bluesky_pipeline.spark_session import create_spark_session


def read_gold_table(spark: SparkSession, path: str) -> DataFrame:
    """Đọc một bảng Gold Parquet từ MinIO.

    Input chính là SparkSession và path Gold cần đọc.
    Output là DataFrame Gold tương ứng.
    """
    return spark.read.parquet(path)


def build_metrics(gold_df: DataFrame) -> dict[str, int]:
    """Tính metrics kiểm chứng cho Gold post engagement summary.

    Input chính là DataFrame Gold post engagement summary.
    Output là dict chứa count và tổng các engagement metrics.
    """
    metrics_row = gold_df.agg(
        spark_sum("like_count").alias("like_count"),
        spark_sum("repost_count").alias("repost_count"),
        spark_sum("engagement_count").alias("engagement_count"),
    ).collect()[0]

    return {
        "row_count": gold_df.count(),
        "like_count": int(metrics_row.like_count or 0),
        "repost_count": int(metrics_row.repost_count or 0),
        "engagement_count": int(metrics_row.engagement_count or 0),
    }


def print_reconciliation(expected: dict[str, int], actual: dict[str, int]) -> None:
    """In kết quả reconciliation giữa Gold cũ và Gold từ Iceberg source."""
    has_mismatch = False

    print("metric\tparquet_source\ticeberg_source\tstatus")

    for metric in sorted(set(expected) | set(actual)):
        expected_value = expected.get(metric, 0)
        actual_value = actual.get(metric, 0)
        status = "OK" if expected_value == actual_value else "MISMATCH"

        if status != "OK":
            has_mismatch = True

        print(f"{metric}\t{expected_value}\t{actual_value}\t{status}")

    if has_mismatch:
        raise SystemExit(
            "Gold post engagement Iceberg source reconciliation failed"
        )

    print("Gold post engagement Iceberg source reconciliation passed")


def main() -> None:
    """So sánh Gold post engagement từ Parquet Silver và Iceberg Silver."""
    # Tạo SparkSession local có cấu hình đọc MinIO.
    spark = create_spark_session("bluesky-check-gold-post-engagement-iceberg-source")
    spark.sparkContext.setLogLevel("WARN")

    parquet_source_df = read_gold_table(spark, GOLD_POST_ENGAGEMENT_SUMMARY_PATH)
    iceberg_source_df = read_gold_table(
        spark,
        GOLD_POST_ENGAGEMENT_SUMMARY_ICEBERG_SOURCE_PATH,
    )

    parquet_metrics = build_metrics(parquet_source_df)
    iceberg_metrics = build_metrics(iceberg_source_df)

    print_reconciliation(parquet_metrics, iceberg_metrics)


if __name__ == "__main__":
    main()