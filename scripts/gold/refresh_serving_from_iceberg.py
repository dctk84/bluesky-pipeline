"""Refresh toàn bộ Gold serving từ lakehouse Iceberg sources."""

from scripts.gold.build_actor_activity_daily_from_gold_modeled import (
    main as build_actor_activity_daily,
)
from scripts.gold.build_content_quality_hourly_from_gold_modeled import (
    main as build_content_quality_hourly,
)
from scripts.gold.build_event_volume_from_iceberg import main as build_event_volume
from scripts.gold.build_post_engagement_summary_from_iceberg import (
    main as build_post_engagement_summary,
)
from scripts.gold.build_post_performance_from_gold_modeled import (
    main as build_post_performance,
)
from scripts.gold.build_thread_conversation_summary_from_gold_modeled import (
    main as build_thread_conversation_summary,
)
from scripts.gold.load_actor_activity_daily_to_clickhouse import (
    main as load_actor_activity_daily,
)
from scripts.gold.load_content_quality_hourly_to_clickhouse import (
    main as load_content_quality_hourly,
)
from scripts.gold.load_event_volume_to_clickhouse import main as load_event_volume
from scripts.gold.load_post_engagement_summary_to_clickhouse import (
    main as load_post_engagement_summary,
)
from scripts.gold.load_post_performance_to_clickhouse import (
    main as load_post_performance,
)
from scripts.gold.load_thread_conversation_summary_to_clickhouse import (
    main as load_thread_conversation_summary,
)
from scripts.gold.check_serving_v1 import main as check_gold_serving


def main() -> None:
    """Refresh Gold staging, load ClickHouse và chạy checkpoint."""
    # Legacy marts còn đọc từ Silver, mart phân tích mới đọc từ Gold modeled.
    build_event_volume()
    build_post_engagement_summary()
    build_post_performance()
    build_content_quality_hourly()
    build_thread_conversation_summary()
    build_actor_activity_daily()

    # Refresh ClickHouse serving tables từ Gold staging.
    load_event_volume()
    load_post_engagement_summary()
    load_post_performance()
    load_content_quality_hourly()
    load_thread_conversation_summary()
    load_actor_activity_daily()

    # Kiểm chứng ClickHouse serving tables sau khi refresh.
    check_gold_serving()


if __name__ == "__main__":
    main()
