"""Refresh toàn bộ Gold serving từ Silver Iceberg."""

from scripts.build_gold_event_volume_from_iceberg import main as build_event_volume
from scripts.build_gold_post_engagement_summary_from_iceberg import (
    main as build_post_engagement_summary,
)
from scripts.load_gold_event_volume_to_clickhouse import main as load_event_volume
from scripts.load_gold_post_engagement_summary_to_clickhouse import (
    main as load_post_engagement_summary,
)
from scripts.check_gold_serving_v1 import main as check_gold_serving


def main() -> None:
    """Refresh Gold staging từ Silver Iceberg, load ClickHouse và chạy checkpoint."""
    # Build lại Gold staging outputs từ Silver Iceberg.
    build_event_volume()
    build_post_engagement_summary()

    # Refresh ClickHouse serving tables từ Gold staging.
    load_event_volume()
    load_post_engagement_summary()

    # Kiểm chứng ClickHouse serving tables sau khi refresh.
    check_gold_serving()


if __name__ == "__main__":
    main()
