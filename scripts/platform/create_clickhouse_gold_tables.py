"""Tạo database và các Gold serving tables trong ClickHouse local."""

from bluesky_pipeline.clickhouse_client import execute_clickhouse
from bluesky_pipeline.gold_tables import (
    CLICKHOUSE_DATABASE,
    GOLD_ACTOR_ACTIVITY_DAILY_TABLE,
    GOLD_CONTENT_QUALITY_HOURLY_TABLE,
    GOLD_CONTENT_ACTIVITY_1M_STREAM_TABLE,
    GOLD_ENGAGEMENT_1M_STREAM_TABLE,
    GOLD_EVENT_VOLUME_1M_STREAM_TABLE,
    GOLD_EVENT_VOLUME_TABLE,
    GOLD_NETWORK_GROWTH_DAILY_TABLE,
    GOLD_NETWORK_ACTIVITY_1M_STREAM_TABLE,
    GOLD_POST_ENGAGEMENT_SUMMARY_TABLE,
    GOLD_POST_PERFORMANCE_TABLE,
    GOLD_REALTIME_STREAM_BATCHES_TABLE,
    GOLD_THREAD_CONVERSATION_SUMMARY_TABLE,
    build_gold_actor_activity_daily_ddl,
    build_gold_content_quality_hourly_ddl,
    build_gold_content_activity_1m_stream_ddl,
    build_gold_engagement_1m_stream_ddl,
    build_gold_event_volume_1m_stream_ddl,
    build_gold_event_volume_ddl,
    build_gold_network_growth_daily_ddl,
    build_gold_network_activity_1m_stream_ddl,
    build_gold_post_engagement_summary_ddl,
    build_gold_post_performance_ddl,
    build_gold_realtime_stream_batches_ddl,
    build_gold_thread_conversation_summary_ddl,
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


def create_gold_post_performance_table() -> None:
    """Tạo bảng Gold post performance serving nếu chưa tồn tại."""
    # Bảng này phục vụ phân tích sâu hơn về performance từng post.
    execute_clickhouse(build_gold_post_performance_ddl())


def create_gold_content_quality_hourly_table() -> None:
    """Tạo bảng Gold content quality hourly serving nếu chưa tồn tại."""
    # Bảng này phục vụ dashboard phân tích content quality theo thời gian.
    execute_clickhouse(build_gold_content_quality_hourly_ddl())


def create_gold_thread_conversation_summary_table() -> None:
    """Tạo bảng Gold thread conversation summary serving nếu chưa tồn tại."""
    # Bảng này phục vụ phân tích thread/conversation theo reply root.
    execute_clickhouse(build_gold_thread_conversation_summary_ddl())


def create_gold_actor_activity_daily_table() -> None:
    """Tạo bảng Gold actor activity daily serving nếu chưa tồn tại."""
    # Bảng này phục vụ phân tích hành vi actor theo ngày.
    execute_clickhouse(build_gold_actor_activity_daily_ddl())


def create_gold_network_growth_daily_table() -> None:
    """Tạo bảng Gold network growth daily serving nếu chưa tồn tại."""
    # Bảng này phục vụ phân tích observed follow/unfollow theo target actor.
    execute_clickhouse(build_gold_network_growth_daily_ddl())


def create_gold_event_volume_1m_stream_table() -> None:
    """Tạo bảng Gold event volume realtime theo phút nếu chưa tồn tại."""
    # Bảng streaming aggregate dùng cho dashboard time series.
    execute_clickhouse(build_gold_event_volume_1m_stream_ddl())


def create_gold_content_activity_1m_stream_table() -> None:
    """Tạo bảng Gold content activity realtime theo phút nếu chưa tồn tại."""
    # Bảng này phục vụ dashboard hoạt động tạo nội dung realtime.
    execute_clickhouse(build_gold_content_activity_1m_stream_ddl())


def create_gold_engagement_1m_stream_table() -> None:
    """Tạo bảng Gold engagement realtime theo phút nếu chưa tồn tại."""
    # Bảng này phục vụ dashboard tương tác realtime.
    execute_clickhouse(build_gold_engagement_1m_stream_ddl())


def create_gold_network_activity_1m_stream_table() -> None:
    """Tạo bảng Gold network activity realtime theo phút nếu chưa tồn tại."""
    # Bảng này phục vụ dashboard follow/unfollow realtime.
    execute_clickhouse(build_gold_network_activity_1m_stream_ddl())


def create_gold_realtime_stream_batches_table() -> None:
    """Tạo bảng health cho realtime Spark micro-batches nếu chưa tồn tại."""
    # Bảng này phục vụ dashboard vận hành của fast path.
    execute_clickhouse(build_gold_realtime_stream_batches_ddl())


def main() -> None:
    """Tạo các ClickHouse objects cần thiết cho Gold serving layer."""
    create_database()
    print(f"created_database: {CLICKHOUSE_DATABASE}")

    create_gold_event_volume_table()
    print(f"created_table: {GOLD_EVENT_VOLUME_TABLE}")

    create_gold_post_engagement_summary_table()
    print(f"created_table: {GOLD_POST_ENGAGEMENT_SUMMARY_TABLE}")

    create_gold_post_performance_table()
    print(f"created_table: {GOLD_POST_PERFORMANCE_TABLE}")

    create_gold_content_quality_hourly_table()
    print(f"created_table: {GOLD_CONTENT_QUALITY_HOURLY_TABLE}")

    create_gold_thread_conversation_summary_table()
    print(f"created_table: {GOLD_THREAD_CONVERSATION_SUMMARY_TABLE}")

    create_gold_actor_activity_daily_table()
    print(f"created_table: {GOLD_ACTOR_ACTIVITY_DAILY_TABLE}")

    create_gold_network_growth_daily_table()
    print(f"created_table: {GOLD_NETWORK_GROWTH_DAILY_TABLE}")

    create_gold_event_volume_1m_stream_table()
    print(f"created_table: {GOLD_EVENT_VOLUME_1M_STREAM_TABLE}")

    create_gold_content_activity_1m_stream_table()
    print(f"created_table: {GOLD_CONTENT_ACTIVITY_1M_STREAM_TABLE}")

    create_gold_engagement_1m_stream_table()
    print(f"created_table: {GOLD_ENGAGEMENT_1M_STREAM_TABLE}")

    create_gold_network_activity_1m_stream_table()
    print(f"created_table: {GOLD_NETWORK_ACTIVITY_1M_STREAM_TABLE}")

    create_gold_realtime_stream_batches_table()
    print(f"created_table: {GOLD_REALTIME_STREAM_BATCHES_TABLE}")


if __name__ == "__main__":
    main()
