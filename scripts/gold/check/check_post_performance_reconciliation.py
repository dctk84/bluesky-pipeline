"""Reconcile Gold post performance giữa MinIO và ClickHouse."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import sum as spark_sum

from bluesky_pipeline.clients.clickhouse import execute_clickhouse
from bluesky_pipeline.schemas.gold_tables import (
    GOLD_POST_PERFORMANCE_PATH,
    GOLD_POST_PERFORMANCE_TABLE,
)
from bluesky_pipeline.config.spark import create_spark_session


def read_gold_post_performance(spark: SparkSession) -> DataFrame:
    """Đọc Gold post performance từ MinIO.

    Input chính là SparkSession đã cấu hình S3A.
    Output là DataFrame Gold post performance từ Parquet.
    """
    return spark.read.parquet(GOLD_POST_PERFORMANCE_PATH)


def build_expected_metrics(gold_df: DataFrame) -> dict[str, int]:
    """Tính metrics kỳ vọng từ Gold post performance Parquet.

    Input chính là DataFrame post performance.
    Output là dict chứa count và tổng các metric quan trọng.
    """
    metrics_row = gold_df.agg(
        spark_sum("like_count").alias("like_count"),
        spark_sum("repost_count").alias("repost_count"),
        spark_sum("engagement_count").alias("engagement_count"),
        spark_sum("engagement_score").alias("engagement_score"),
    ).collect()[0]

    return {
        "row_count": gold_df.count(),
        "like_count": int(metrics_row.like_count or 0),
        "repost_count": int(metrics_row.repost_count or 0),
        "engagement_count": int(metrics_row.engagement_count or 0),
        "engagement_score": int(metrics_row.engagement_score or 0),
    }


def read_clickhouse_metrics() -> dict[str, int]:
    """Đọc metrics thực tế từ ClickHouse post performance table."""
    result = execute_clickhouse(
        f"""
        SELECT
            count() AS row_count,
            sum(like_count) AS like_count,
            sum(repost_count) AS repost_count,
            sum(engagement_count) AS engagement_count,
            sum(engagement_score) AS engagement_score
        FROM {GOLD_POST_PERFORMANCE_TABLE}
        """
    )

    row_count, like_count, repost_count, engagement_count, engagement_score = (
        result.strip().split("\t")
    )

    return {
        "row_count": int(row_count),
        "like_count": int(like_count),
        "repost_count": int(repost_count),
        "engagement_count": int(engagement_count),
        "engagement_score": int(engagement_score),
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
        raise SystemExit("Gold post performance reconciliation failed")

    print("Gold post performance reconciliation passed")


def main() -> None:
    """So sánh Gold post performance giữa MinIO và ClickHouse."""
    spark = create_spark_session("bluesky-check-gold-post-performance")
    spark.sparkContext.setLogLevel("WARN")

    gold_df = read_gold_post_performance(spark)
    expected_metrics = build_expected_metrics(gold_df)
    actual_metrics = read_clickhouse_metrics()

    print_reconciliation(expected_metrics, actual_metrics)


if __name__ == "__main__":
    main()
