"""Chạy checkpoint tổng hợp cho Gold serving v1."""

from bluesky_pipeline.iceberg_config import create_iceberg_spark_session

from scripts.gold.check_actor_activity_daily_reconciliation import (
    build_expected_metrics as build_actor_activity_expected_metrics,
    print_reconciliation as print_actor_activity_reconciliation,
    read_clickhouse_metrics as read_actor_activity_clickhouse_metrics,
    read_gold_actor_activity_daily,
)
from scripts.gold.check_content_quality_hourly_reconciliation import (
    build_expected_metrics as build_content_quality_expected_metrics,
    print_reconciliation as print_content_quality_reconciliation,
    read_clickhouse_metrics as read_content_quality_clickhouse_metrics,
    read_gold_content_quality_hourly,
)
from scripts.gold.check_network_growth_daily_reconciliation import (
    build_expected_metrics as build_network_growth_expected_metrics,
    print_reconciliation as print_network_growth_reconciliation,
    read_clickhouse_metrics as read_network_growth_clickhouse_metrics,
    read_gold_network_growth_daily,
)
from scripts.gold.check_post_engagement_reconciliation import (
    build_expected_metrics as build_post_engagement_expected_metrics,
    print_reconciliation as print_post_engagement_reconciliation,
    read_clickhouse_metrics as read_post_engagement_clickhouse_metrics,
    read_gold_post_engagement_summary,
)
from scripts.gold.check_post_performance_reconciliation import (
    build_expected_metrics as build_post_performance_expected_metrics,
    print_reconciliation as print_post_performance_reconciliation,
    read_clickhouse_metrics as read_post_performance_clickhouse_metrics,
    read_gold_post_performance,
)
from scripts.gold.check_thread_conversation_summary_reconciliation import (
    build_expected_metrics as build_thread_conversation_expected_metrics,
    print_reconciliation as print_thread_conversation_reconciliation,
    read_clickhouse_metrics as read_thread_conversation_clickhouse_metrics,
    read_gold_thread_conversation_summary,
)
from scripts.gold.check_event_volume_reconciliation import (
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


def check_post_performance(spark) -> None:
    """Kiểm tra reconciliation cho Gold post performance.

    Input chính là SparkSession dùng để đọc Gold Parquet source.
    Output là exception nếu reconciliation bị lệch.
    """
    print("=== gold_post_performance ===")

    # So sánh analytics mart từ Gold Parquet với dữ liệu đã load trong ClickHouse.
    gold_df = read_gold_post_performance(spark)
    expected_metrics = build_post_performance_expected_metrics(gold_df)
    actual_metrics = read_post_performance_clickhouse_metrics()
    print_post_performance_reconciliation(expected_metrics, actual_metrics)


def check_content_quality_hourly(spark) -> None:
    """Kiểm tra reconciliation cho Gold content quality hourly.

    Input chính là SparkSession dùng để đọc Gold Parquet source.
    Output là exception nếu reconciliation bị lệch.
    """
    print("=== gold_content_quality_hourly ===")

    # So sánh hourly mart từ Gold Parquet với dữ liệu đã load trong ClickHouse.
    gold_df = read_gold_content_quality_hourly(spark)
    expected_metrics = build_content_quality_expected_metrics(gold_df)
    actual_metrics = read_content_quality_clickhouse_metrics()
    print_content_quality_reconciliation(expected_metrics, actual_metrics)


def check_thread_conversation_summary(spark) -> None:
    """Kiểm tra reconciliation cho Gold thread conversation summary.

    Input chính là SparkSession dùng để đọc Gold Parquet source.
    Output là exception nếu reconciliation bị lệch.
    """
    print("=== gold_thread_conversation_summary ===")

    # So sánh thread mart từ Gold Parquet với dữ liệu đã load trong ClickHouse.
    gold_df = read_gold_thread_conversation_summary(spark)
    expected_metrics = build_thread_conversation_expected_metrics(gold_df)
    actual_metrics = read_thread_conversation_clickhouse_metrics()
    print_thread_conversation_reconciliation(expected_metrics, actual_metrics)


def check_actor_activity_daily(spark) -> None:
    """Kiểm tra reconciliation cho Gold actor activity daily.

    Input chính là SparkSession dùng để đọc Gold Parquet source.
    Output là exception nếu reconciliation bị lệch.
    """
    print("=== gold_actor_activity_daily ===")

    # So sánh actor mart từ Gold Parquet với dữ liệu đã load trong ClickHouse.
    gold_df = read_gold_actor_activity_daily(spark)
    expected_metrics = build_actor_activity_expected_metrics(gold_df)
    actual_metrics = read_actor_activity_clickhouse_metrics()
    print_actor_activity_reconciliation(expected_metrics, actual_metrics)


def check_network_growth_daily(spark) -> None:
    """Kiểm tra reconciliation cho Gold network growth daily.

    Input chính là SparkSession dùng để đọc Gold Parquet source.
    Output là exception nếu reconciliation bị lệch.
    """
    print("=== gold_network_growth_daily ===")

    # So sánh network mart từ Gold Parquet với dữ liệu đã load trong ClickHouse.
    gold_df = read_gold_network_growth_daily(spark)
    expected_metrics = build_network_growth_expected_metrics(gold_df)
    actual_metrics = read_network_growth_clickhouse_metrics()
    print_network_growth_reconciliation(expected_metrics, actual_metrics)


def main() -> None:
    """Chạy toàn bộ checkpoint Gold serving v1."""
    # Dùng một Iceberg SparkSession để đọc được Silver source of truth.
    spark = create_iceberg_spark_session("bluesky-check-gold-serving-v1")
    spark.sparkContext.setLogLevel("WARN")

    check_event_volume(spark)
    check_post_engagement_summary(spark)
    check_post_performance(spark)
    check_content_quality_hourly(spark)
    check_thread_conversation_summary(spark)
    check_actor_activity_daily(spark)
    check_network_growth_daily(spark)

    print("Gold serving v1 check passed")


if __name__ == "__main__":
    main()
