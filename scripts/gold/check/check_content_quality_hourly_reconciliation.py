"""Reconcile Gold content quality hourly giữa MinIO và ClickHouse."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import sum as spark_sum

from bluesky_pipeline.clients.clickhouse import execute_clickhouse
from bluesky_pipeline.schemas.gold_tables import (
    GOLD_CONTENT_QUALITY_HOURLY_PATH,
    GOLD_CONTENT_QUALITY_HOURLY_TABLE,
)
from bluesky_pipeline.config.spark import create_spark_session


COUNT_METRICS = [
    "original_post_create_count",
    "reply_create_count",
    "post_update_count",
    "reply_update_count",
    "post_delete_count",
    "reply_delete_count",
    "total_content_events",
]


def read_gold_content_quality_hourly(spark: SparkSession) -> DataFrame:
    """Đọc Gold content quality hourly từ MinIO.

    Input chính là SparkSession đã cấu hình S3A.
    Output là DataFrame Gold content quality hourly từ Parquet.
    """
    return spark.read.parquet(GOLD_CONTENT_QUALITY_HOURLY_PATH)


def build_expected_metrics(gold_df: DataFrame) -> dict[str, int]:
    """Tính metrics kỳ vọng từ Gold content quality hourly Parquet.

    Input chính là DataFrame content quality hourly.
    Output là dict chứa row count và tổng các count metrics.
    """
    metrics_row = gold_df.agg(
        *[spark_sum(metric).alias(metric) for metric in COUNT_METRICS]
    ).collect()[0]

    metrics = {"row_count": gold_df.count()}

    for metric in COUNT_METRICS:
        metrics[metric] = int(metrics_row[metric] or 0)

    return metrics


def read_clickhouse_metrics() -> dict[str, int]:
    """Đọc metrics thực tế từ ClickHouse content quality hourly table."""
    metric_selects = ",\n".join(
        f"sum({metric}) AS {metric}" for metric in COUNT_METRICS
    )
    result = execute_clickhouse(
        f"""
        SELECT
            count() AS row_count,
            {metric_selects}
        FROM {GOLD_CONTENT_QUALITY_HOURLY_TABLE}
        """
    )

    values = result.strip().split("\t")
    metric_names = ["row_count", *COUNT_METRICS]

    return {
        metric_name: int(metric_value if metric_value != "\\N" else 0)
        for metric_name, metric_value in zip(metric_names, values, strict=True)
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
        raise SystemExit("Gold content quality hourly reconciliation failed")

    print("Gold content quality hourly reconciliation passed")


def main() -> None:
    """So sánh Gold content quality hourly giữa MinIO và ClickHouse."""
    spark = create_spark_session("bluesky-check-gold-content-quality-hourly")
    spark.sparkContext.setLogLevel("WARN")

    gold_df = read_gold_content_quality_hourly(spark)
    expected_metrics = build_expected_metrics(gold_df)
    actual_metrics = read_clickhouse_metrics()

    print_reconciliation(expected_metrics, actual_metrics)


if __name__ == "__main__":
    main()
