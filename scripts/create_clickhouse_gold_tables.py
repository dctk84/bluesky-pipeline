"""Tạo database và các Gold serving tables trong ClickHouse local."""

from bluesky_pipeline.clickhouse_client import execute_clickhouse
from bluesky_pipeline.gold_tables import (
    CLICKHOUSE_DATABASE,
    GOLD_EVENT_VOLUME_1M_STREAM_TABLE,
    GOLD_EVENT_VOLUME_TABLE,
    GOLD_POST_ENGAGEMENT_SUMMARY_TABLE,
    build_gold_event_volume_1m_stream_ddl,
    build_gold_event_volume_ddl,
    build_gold_post_engagement_summary_ddl,
)

def create_database() -> None:
    """Tạo database ClickHouse cho project nếu chưa tồn tại."""
    # Database dùng để gom các bảng serving phục vụ query/dashboard.
    execute_clickhouse(f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DATABASE}")


def create_gold_event_volume_table() -> None:
    """Tạo bảng Gold event volume serving nếu chưa tồn tại."""
    # Bảng aggregate nhỏ, phục vụ phân tích số lượng event theo loại.
    execute_clickhouse(build_gold_event_volume_ddl())

def create_gold_post_engagement_summary_table() -> None:
    """Tạo bảng Gold post engagement summary serving nếu chưa tồn tại."""
    # Bảng này phục vụ dashboard/query top posts theo engagement.
    execute_clickhouse(build_gold_post_engagement_summary_ddl())

def create_gold_event_volume_1m_stream_table() -> None:
    """Tạo bảng Gold event volume realtime theo phút nếu chưa tồn tại."""
    # Bảng streaming aggregate dùng cho dashboard time series.
    execute_clickhouse(build_gold_event_volume_1m_stream_ddl())

def main() -> None:
    """Tạo các ClickHouse objects cần thiết cho Gold serving layer."""
    create_database()
    print(f"created_database: {CLICKHOUSE_DATABASE}")

    create_gold_event_volume_table()
    print(f"created_table: {GOLD_EVENT_VOLUME_TABLE}")

    create_gold_post_engagement_summary_table()
    print(f"created_table: {GOLD_POST_ENGAGEMENT_SUMMARY_TABLE}")

    create_gold_event_volume_1m_stream_table()
    print(f"created_table: {GOLD_EVENT_VOLUME_1M_STREAM_TABLE}")

if __name__ == "__main__":
    main()