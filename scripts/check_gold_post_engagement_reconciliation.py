"""Reconcile Gold post engagement summary giữa MinIO và ClickHouse."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import sum as spark_sum

from bluesky_pipeline.clickhouse_client import execute_clickhouse
from bluesky_pipeline.gold_tables import (
    GOLD_POST_ENGAGEMENT_SUMMARY_PATH,
    GOLD_POST_ENGAGEMENT_SUMMARY_TABLE,
)
from bluesky_pipeline.spark_session import create_spark_session


def read_gold_post_engagement_summary(spark: SparkSession) -> DataFrame:
    """Đọc Gold post engagement summary từ MinIO.

    Input chính là SparkSession đã cấu hình S3A.
    Output là DataFrame Gold post engagement summary từ Parquet.
    """
    return spark.read.parquet(GOLD_POST_ENGAGEMENT_SUMMARY_PATH)


def build_expected_metrics(gold_df: DataFrame) -> dict[str, int]:
    """Tính metrics kỳ vọng từ Gold Parquet trên MinIO.

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


def read_clickhouse_metrics() -> dict[str, int]:
    """Đọc metrics thực tế từ ClickHouse serving table.

    Output là dict chứa count và tổng các engagement metrics trong ClickHouse.
    """
    result = execute_clickhouse(
        f"""
        SELECT
            count() AS row_count,
            sum(like_count) AS like_count,
            sum(repost_count) AS repost_count,
            sum(engagement_count) AS engagement_count
        FROM {GOLD_POST_ENGAGEMENT_SUMMARY_TABLE}
        """
    )

    row_count, like_count, repost_count, engagement_count = result.strip().split("\t")

    return {
        "row_count": int(row_count),
        "like_count": int(like_count),
        "repost_count": int(repost_count),
        "engagement_count": int(engagement_count),
    }


def print_reconciliation(expected: dict[str, int], actual: dict[str, int]) -> None:
    """In kết quả reconciliation giữa Gold Parquet và ClickHouse."""
    has_mismatch = False

    print("metric\texpected\tactual\tstatus")

    for metric in sorted(set(expected) | set(actual)):
        expected_value = expected.get(metric, 0)
        actual_value = actual.get(metric, 0)
        status = "OK" if expected_value == actual_value else "MISMATCH"

        if status != "OK":
            has_mismatch = True

        print(f"{metric}\t{expected_value}\t{actual_value}\t{status}")

    if has_mismatch:
        raise SystemExit("Gold post engagement reconciliation failed")

    print("Gold post engagement reconciliation passed")


def main() -> None:
    """So sánh Gold post engagement summary giữa MinIO và ClickHouse."""
    # Tạo SparkSession local có cấu hình đọc MinIO.
    spark = create_spark_session("bluesky-check-gold-post-engagement-reconciliation")
    spark.sparkContext.setLogLevel("WARN")

    # Tính expected từ Gold Parquet và actual từ ClickHouse rồi so sánh.
    gold_df = read_gold_post_engagement_summary(spark)
    expected_metrics = build_expected_metrics(gold_df)
    actual_metrics = read_clickhouse_metrics()

    print_reconciliation(expected_metrics, actual_metrics)


if __name__ == "__main__":
    main()
    