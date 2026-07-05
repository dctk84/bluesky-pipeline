"""Kiểm tra Gold post engagement summary serving table trong ClickHouse."""

from bluesky_pipeline.clickhouse_client import execute_clickhouse
from bluesky_pipeline.gold_tables import GOLD_POST_ENGAGEMENT_SUMMARY_TABLE


def main() -> None:
    """Query Gold post engagement summary và in kết quả kiểm chứng."""
    # Kiểm tra tổng số dòng và top posts theo engagement trong ClickHouse.
    count_result = execute_clickhouse(
        f"SELECT count() FROM {GOLD_POST_ENGAGEMENT_SUMMARY_TABLE}"
    )
    print(f"gold_post_engagement_summary_count: {count_result.strip()}")

    top_rows = execute_clickhouse(
        f"""
        SELECT
            post_uri,
            like_count,
            repost_count,
            engagement_count
        FROM {GOLD_POST_ENGAGEMENT_SUMMARY_TABLE}
        ORDER BY engagement_count DESC, post_uri
        LIMIT 20
        """
    )
    print(top_rows)


if __name__ == "__main__":
    main()