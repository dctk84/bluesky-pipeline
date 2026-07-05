"""Kiểm tra Gold event volume serving table trong ClickHouse."""

from bluesky_pipeline.clickhouse_client import execute_clickhouse

CLICKHOUSE_TABLE = "bluesky.gold_event_volume_by_type"

def main() -> None:
    """Query Gold serving table và in kết quả kiểm chứng."""
    # Đọc dữ liệu aggregate đã load vào ClickHouse.
    result = execute_clickhouse(
        f"""
        SELECT event_type, event_count
        FROM {CLICKHOUSE_TABLE}
        ORDER BY event_type
        """
    )

    print(result)


if __name__ == "__main__":
    main()