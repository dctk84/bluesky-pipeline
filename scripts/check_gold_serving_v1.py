"""Chạy checkpoint tổng hợp cho Gold serving v1."""

import sys
from pathlib import Path

from bluesky_pipeline.iceberg_config import create_iceberg_spark_session

SCRIPTS_DIR = Path(__file__).resolve().parent
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from check_gold_post_engagement_reconciliation import (
    build_expected_metrics as build_post_engagement_expected_metrics,
    print_reconciliation as print_post_engagement_reconciliation,
    read_clickhouse_metrics as read_post_engagement_clickhouse_metrics,
    read_gold_post_engagement_summary,
)
from check_gold_reconciliation import (
    build_expected_counts as build_event_volume_expected_counts,
    print_reconciliation as print_event_volume_reconciliation,
    read_clickhouse_counts as read_event_volume_clickhouse_counts,
)


def check_event_volume(spark) -> None:
    """Kiểm tra reconciliation cho Gold event volume.

    Input chính là SparkSession dùng để đọc Silver Iceberg source.
    Output là exception nếu reconciliation bị lệch.
    """
    print("=== gold_event_volume_by_type ===")

    # So sánh event counts tính lại từ Silver Iceberg với dữ liệu ClickHouse.
    expected_counts = build_event_volume_expected_counts(spark)
    actual_counts = read_event_volume_clickhouse_counts()
    print_event_volume_reconciliation(expected_counts, actual_counts)


def check_post_engagement_summary(spark) -> None:
    """Kiểm tra reconciliation cho Gold post engagement summary.

    Input chính là SparkSession dùng để đọc Gold Parquet source.
    Output là exception nếu reconciliation bị lệch.
    """
    print("=== gold_post_engagement_summary ===")

    # So sánh metrics từ Gold Parquet với dữ liệu đã load trong ClickHouse.
    gold_df = read_gold_post_engagement_summary(spark)
    expected_metrics = build_post_engagement_expected_metrics(gold_df)
    actual_metrics = read_post_engagement_clickhouse_metrics()
    print_post_engagement_reconciliation(expected_metrics, actual_metrics)


def main() -> None:
    """Chạy toàn bộ checkpoint Gold serving v1."""
    # Dùng một Iceberg SparkSession để đọc được Silver source of truth.
    spark = create_iceberg_spark_session("bluesky-check-gold-serving-v1")
    spark.sparkContext.setLogLevel("WARN")

    check_event_volume(spark)
    check_post_engagement_summary(spark)

    print("Gold serving v1 check passed")


if __name__ == "__main__":
    main()
