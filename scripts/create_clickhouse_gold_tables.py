"""Tạo database và các Gold serving tables trong ClickHouse local."""

from bluesky_pipeline.clickhouse_client import execute_clickhouse


CLICKHOUSE_DATABASE = "bluesky"
GOLD_EVENT_VOLUME_TABLE = "bluesky.gold_event_volume_by_type"


def create_database() -> None:
    """Tạo database ClickHouse cho project nếu chưa tồn tại."""
    # Database dùng để gom các bảng serving phục vụ query/dashboard.
    execute_clickhouse(f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DATABASE}")


def create_gold_event_volume_table() -> None:
    """Tạo bảng Gold event volume serving nếu chưa tồn tại."""
    # Bảng aggregate nhỏ, phục vụ phân tích số lượng event theo loại.
    execute_clickhouse(
        f"""
        CREATE TABLE IF NOT EXISTS {GOLD_EVENT_VOLUME_TABLE}
        (
            event_type String,
            event_count UInt64,
            loaded_at DateTime DEFAULT now()
        )
        ENGINE = MergeTree
        ORDER BY event_type
        """
    )


def main() -> None:
    """Tạo các ClickHouse objects cần thiết cho Gold serving layer."""
    create_database()
    print(f"created_database: {CLICKHOUSE_DATABASE}")

    create_gold_event_volume_table()
    print(f"created_table: {GOLD_EVENT_VOLUME_TABLE}")


if __name__ == "__main__":
    main()