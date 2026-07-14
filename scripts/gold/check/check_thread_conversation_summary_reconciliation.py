"""Reconcile Gold thread conversation summary giữa MinIO và ClickHouse."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import sum as spark_sum

from bluesky_pipeline.clients.clickhouse import execute_clickhouse
from bluesky_pipeline.schemas.gold_tables import (
    GOLD_THREAD_CONVERSATION_SUMMARY_PATH,
    GOLD_THREAD_CONVERSATION_SUMMARY_TABLE,
)
from bluesky_pipeline.config.spark import create_spark_session


COUNT_METRICS = [
    "reply_count",
    "reply_author_count",
    "deleted_reply_count",
]


def read_gold_thread_conversation_summary(spark: SparkSession) -> DataFrame:
    """Đọc Gold thread conversation summary từ MinIO.

    Input chính là SparkSession đã cấu hình S3A.
    Output là DataFrame Gold thread conversation summary từ Parquet.
    """
    return spark.read.parquet(GOLD_THREAD_CONVERSATION_SUMMARY_PATH)


def build_expected_metrics(gold_df: DataFrame) -> dict[str, int]:
    """Tính metrics kỳ vọng từ Gold thread conversation summary Parquet.

    Input chính là DataFrame thread conversation summary.
    Output là dict chứa row count và tổng các count metrics.
    """
    metrics_row = gold_df.agg(
        *[spark_sum(metric).alias(metric) for metric in COUNT_METRICS]
    ).collect()[0]

    metrics = {"row_count": gold_df.count()}

    for metric in COUNT_METRICS:
        metrics[metric] = int(metrics_row[metric] or 0)

    return metrics


def parse_clickhouse_int(metric_value: str) -> int:
    """Parse giá trị số từ ClickHouse, coi NULL aggregate là 0."""
    return int(metric_value if metric_value != "\\N" else 0)


def read_clickhouse_metrics() -> dict[str, int]:
    """Đọc metrics thực tế từ ClickHouse thread conversation summary table."""
    metric_selects = ",\n".join(
        f"sum({metric}) AS {metric}" for metric in COUNT_METRICS
    )
    result = execute_clickhouse(
        f"""
        SELECT
            count() AS row_count,
            {metric_selects}
        FROM {GOLD_THREAD_CONVERSATION_SUMMARY_TABLE}
        """
    )

    values = result.strip().split("\t")
    metric_names = ["row_count", *COUNT_METRICS]

    return {
        metric_name: parse_clickhouse_int(metric_value)
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
        raise SystemExit("Gold thread conversation summary reconciliation failed")

    print("Gold thread conversation summary reconciliation passed")


def main() -> None:
    """So sánh Gold thread conversation summary giữa MinIO và ClickHouse."""
    spark = create_spark_session("bluesky-check-gold-thread-conversation-summary")
    spark.sparkContext.setLogLevel("WARN")

    gold_df = read_gold_thread_conversation_summary(spark)
    expected_metrics = build_expected_metrics(gold_df)
    actual_metrics = read_clickhouse_metrics()

    print_reconciliation(expected_metrics, actual_metrics)


if __name__ == "__main__":
    main()
