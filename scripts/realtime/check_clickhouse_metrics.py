"""Kiểm tra toàn bộ realtime metrics trong ClickHouse."""

from bluesky_pipeline.clickhouse_client import execute_clickhouse
from bluesky_pipeline.gold_tables import (
    GOLD_CONTENT_ACTIVITY_1M_STREAM_TABLE,
    GOLD_ENGAGEMENT_1M_STREAM_TABLE,
    GOLD_EVENT_VOLUME_1M_STREAM_TABLE,
    GOLD_NETWORK_ACTIVITY_1M_STREAM_TABLE,
    GOLD_REALTIME_STREAM_BATCHES_TABLE,
)


def print_realtime_table_summary() -> None:
    """In summary và freshness của các bảng realtime metrics."""
    # Chuẩn hóa output để so sánh freshness giữa các realtime marts.
    result = execute_clickhouse(
        f"""
        SELECT
            metric_table,
            min_time,
            max_time,
            raw_rows,
            total_metric_count,
            last_loaded_at,
            seconds_since_last_load
        FROM
        (
            SELECT
                'event_volume' AS metric_table,
                min(window_start) AS min_time,
                max(window_start) AS max_time,
                count() AS raw_rows,
                sum(event_count) AS total_metric_count,
                max(loaded_at) AS last_loaded_at,
                dateDiff('second', max(loaded_at), now()) AS seconds_since_last_load
            FROM {GOLD_EVENT_VOLUME_1M_STREAM_TABLE}

            UNION ALL

            SELECT
                'content_activity' AS metric_table,
                min(window_start) AS min_time,
                max(window_start) AS max_time,
                count() AS raw_rows,
                sum(activity_count) AS total_metric_count,
                max(loaded_at) AS last_loaded_at,
                dateDiff('second', max(loaded_at), now()) AS seconds_since_last_load
            FROM {GOLD_CONTENT_ACTIVITY_1M_STREAM_TABLE}

            UNION ALL

            SELECT
                'engagement' AS metric_table,
                min(window_start) AS min_time,
                max(window_start) AS max_time,
                count() AS raw_rows,
                sum(engagement_count) AS total_metric_count,
                max(loaded_at) AS last_loaded_at,
                dateDiff('second', max(loaded_at), now()) AS seconds_since_last_load
            FROM {GOLD_ENGAGEMENT_1M_STREAM_TABLE}

            UNION ALL

            SELECT
                'network_activity' AS metric_table,
                min(window_start) AS min_time,
                max(window_start) AS max_time,
                count() AS raw_rows,
                sum(activity_count) AS total_metric_count,
                max(loaded_at) AS last_loaded_at,
                dateDiff('second', max(loaded_at), now()) AS seconds_since_last_load
            FROM {GOLD_NETWORK_ACTIVITY_1M_STREAM_TABLE}
        )
        ORDER BY metric_table
        FORMAT PrettyCompact
        """
    )

    print("=== realtime_metrics_summary ===")
    print(result)


def print_batch_health_summary() -> None:
    """In summary vận hành của Spark micro-batches."""
    # Bảng này cho biết job có batch mới, batch rỗng và duration bất thường không.
    result = execute_clickhouse(
        f"""
        SELECT
            count() AS total_batches,
            max(spark_batch_id) AS latest_batch_id,
            max(batch_finished_at) AS last_batch_finished_at,
            dateDiff('second', max(batch_finished_at), now())
                AS seconds_since_last_batch,
            sum(is_empty) AS empty_batches,
            round(avg(batch_duration_ms), 2) AS avg_batch_duration_ms,
            max(batch_duration_ms) AS max_batch_duration_ms,
            sum(input_rows) AS total_input_rows
        FROM {GOLD_REALTIME_STREAM_BATCHES_TABLE}
        FORMAT PrettyCompact
        """
    )

    print("=== realtime_batch_health_summary ===")
    print(result)


def print_latest_batch_health() -> None:
    """In các Spark micro-batches mới nhất của realtime path."""
    result = execute_clickhouse(
        f"""
        SELECT
            spark_batch_id,
            batch_started_at,
            batch_finished_at,
            batch_duration_ms,
            input_rows,
            event_volume_rows,
            content_activity_rows,
            engagement_rows,
            network_activity_rows,
            is_empty
        FROM {GOLD_REALTIME_STREAM_BATCHES_TABLE}
        ORDER BY spark_batch_id DESC
        LIMIT 20
        FORMAT PrettyCompact
        """
    )

    print("=== latest_realtime_batches ===")
    print(result)


def print_latest_realtime_metrics() -> None:
    """In các metric mới nhất theo window và metric type."""
    # Mỗi bảng có tên metric/count khác nhau, nên normalize về metric_group/name/count.
    result = execute_clickhouse(
        f"""
        SELECT
            metric_group,
            window_start,
            metric_name,
            metric_count
        FROM
        (
            SELECT
                'event_volume' AS metric_group,
                window_start,
                event_type AS metric_name,
                sum(event_count) AS metric_count
            FROM {GOLD_EVENT_VOLUME_1M_STREAM_TABLE}
            GROUP BY window_start, event_type

            UNION ALL

            SELECT
                'content_activity' AS metric_group,
                window_start,
                content_activity_type AS metric_name,
                sum(activity_count) AS metric_count
            FROM {GOLD_CONTENT_ACTIVITY_1M_STREAM_TABLE}
            GROUP BY window_start, content_activity_type

            UNION ALL

            SELECT
                'engagement' AS metric_group,
                window_start,
                engagement_type AS metric_name,
                sum(engagement_count) AS metric_count
            FROM {GOLD_ENGAGEMENT_1M_STREAM_TABLE}
            GROUP BY window_start, engagement_type

            UNION ALL

            SELECT
                'network_activity' AS metric_group,
                window_start,
                network_activity_type AS metric_name,
                sum(activity_count) AS metric_count
            FROM {GOLD_NETWORK_ACTIVITY_1M_STREAM_TABLE}
            GROUP BY window_start, network_activity_type
        )
        ORDER BY window_start DESC, metric_group, metric_name
        LIMIT 60
        FORMAT PrettyCompact
        """
    )

    print("=== latest_realtime_metrics ===")
    print(result)


def print_event_type_share() -> None:
    """In tỷ trọng event type trong các window mới nhất."""
    # Tỷ trọng là metric dẫn xuất từ bảng event volume, không cần lưu bảng riêng.
    result = execute_clickhouse(
        f"""
        SELECT
            events.window_start,
            events.event_type,
            events.event_count,
            totals.total_event_count,
            round(events.event_count * 100.0 / totals.total_event_count, 2) AS share_pct
        FROM
        (
            SELECT
                window_start,
                event_type,
                sum(event_count) AS event_count
            FROM {GOLD_EVENT_VOLUME_1M_STREAM_TABLE}
            GROUP BY window_start, event_type
        ) AS events
        INNER JOIN
        (
            SELECT
                window_start,
                sum(event_count) AS total_event_count
            FROM {GOLD_EVENT_VOLUME_1M_STREAM_TABLE}
            GROUP BY window_start
        ) AS totals
        USING window_start
        ORDER BY events.window_start DESC, share_pct DESC
        LIMIT 30
        FORMAT PrettyCompact
        """
    )

    print("=== event_type_share_latest_windows ===")
    print(result)


def print_engagement_to_post_ratio() -> None:
    """In engagement/post ratio theo các window mới nhất."""
    # Ratio này nối engagement total với post count trong cùng window.
    result = execute_clickhouse(
        f"""
        SELECT
            posts.window_start,
            posts.post_count,
            engagements.engagement_count,
            round(engagements.engagement_count / posts.post_count, 2)
                AS engagement_to_post_ratio
        FROM
        (
            SELECT
                window_start,
                sumIf(activity_count, content_activity_type = 'post') AS post_count
            FROM {GOLD_CONTENT_ACTIVITY_1M_STREAM_TABLE}
            GROUP BY window_start
        ) AS posts
        INNER JOIN
        (
            SELECT
                window_start,
                sum(engagement_count) AS engagement_count
            FROM {GOLD_ENGAGEMENT_1M_STREAM_TABLE}
            GROUP BY window_start
        ) AS engagements
        USING window_start
        WHERE posts.post_count > 0
        ORDER BY posts.window_start DESC
        LIMIT 20
        FORMAT PrettyCompact
        """
    )

    print("=== engagement_to_post_ratio_latest_windows ===")
    print(result)


def print_network_activity_summary() -> None:
    """In follow, unfollow và net follow theo window."""
    # Net follow là metric dẫn xuất từ bảng network activity.
    result = execute_clickhouse(
        f"""
        SELECT
            window_start,
            follow_count,
            unfollow_count,
            follow_count - unfollow_count AS net_follow_count
        FROM
        (
            SELECT
                window_start,
                sumIf(activity_count, network_activity_type = 'follow')
                    AS follow_count,
                sumIf(activity_count, network_activity_type = 'unfollow')
                    AS unfollow_count
            FROM {GOLD_NETWORK_ACTIVITY_1M_STREAM_TABLE}
            GROUP BY window_start
        )
        ORDER BY window_start DESC
        LIMIT 20
        FORMAT PrettyCompact
        """
    )

    print("=== network_activity_latest_windows ===")
    print(result)


def main() -> None:
    """Chạy checkpoint cho toàn bộ realtime metrics."""
    print_realtime_table_summary()
    print_batch_health_summary()
    print_latest_batch_health()
    print_latest_realtime_metrics()
    print_event_type_share()
    print_engagement_to_post_ratio()
    print_network_activity_summary()


if __name__ == "__main__":
    main()
