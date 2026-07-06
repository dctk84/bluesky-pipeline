"""Kiểm tra bảng streaming event volume trong ClickHouse."""

from bluesky_pipeline.clickhouse_client import execute_clickhouse
from bluesky_pipeline.gold_tables import GOLD_EVENT_VOLUME_1M_STREAM_TABLE


def print_table_summary() -> None:
    """In tổng quan dữ liệu streaming event volume trong ClickHouse."""
    # Kiểm tra bảng có dữ liệu và khoảng thời gian dữ liệu đang nằm ở đâu.
    result = execute_clickhouse(
        f"""
        SELECT
            min(window_start) AS min_time,
            max(window_start) AS max_time,
            count() AS raw_rows,
            sum(event_count) AS total_events
        FROM {GOLD_EVENT_VOLUME_1M_STREAM_TABLE}
        """
    )

    print("=== streaming_event_volume_summary ===")
    print(result)


def print_latest_event_volume() -> None:
    """In các bucket event volume mới nhất theo phút và event type."""
    # Dùng sum vì SummingMergeTree có thể có nhiều row cùng key trước khi merge.
    result = execute_clickhouse(
        f"""
            SELECT
                window_start,
                event_type,
                spark_batch_id,
                sum(event_count) AS event_count
            FROM {GOLD_EVENT_VOLUME_1M_STREAM_TABLE}
            GROUP BY
                window_start,
                event_type,
                spark_batch_id
            ORDER BY
                window_start DESC,
                spark_batch_id DESC,
                event_type ASC
            LIMIT 20
        """
    )

    print("=== latest_streaming_event_volume ===")
    print(result)


def main() -> None:
    """Chạy checkpoint đọc bảng streaming event volume từ ClickHouse."""
    print_table_summary()
    print_latest_event_volume()


if __name__ == "__main__":
    main()