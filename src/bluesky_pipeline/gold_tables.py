"""Khai báo metadata dùng chung cho các bảng Gold serving."""

CLICKHOUSE_DATABASE = "bluesky"

GOLD_EVENT_VOLUME_TABLE = "bluesky.gold_event_volume_by_type"
GOLD_EVENT_VOLUME_PATH = "s3a://bluesky-lake/gold/gold_event_volume_by_type"
GOLD_EVENT_VOLUME_CLICKHOUSE_SOURCE_PATH = GOLD_EVENT_VOLUME_PATH

GOLD_POST_ENGAGEMENT_SUMMARY_TABLE = "bluesky.gold_post_engagement_summary"
GOLD_POST_ENGAGEMENT_SUMMARY_PATH = (
    "s3a://bluesky-lake/gold/gold_post_engagement_summary"
)
GOLD_POST_ENGAGEMENT_SUMMARY_CLICKHOUSE_SOURCE_PATH = (
    GOLD_POST_ENGAGEMENT_SUMMARY_PATH
)

GOLD_EVENT_VOLUME_1M_STREAM_TABLE = "bluesky.gold_event_volume_1m_stream"
GOLD_CONTENT_ACTIVITY_1M_STREAM_TABLE = (
    "bluesky.gold_content_activity_1m_stream"
)
GOLD_ENGAGEMENT_1M_STREAM_TABLE = "bluesky.gold_engagement_1m_stream"
GOLD_NETWORK_ACTIVITY_1M_STREAM_TABLE = "bluesky.gold_network_activity_1m_stream"

GOLD_REALTIME_METRICS_1M_STREAM_CHECKPOINT_LOCATION = (
    "s3a://bluesky-lake/checkpoints/gold_realtime_metrics_1m_stream"
)


def build_gold_post_engagement_summary_ddl() -> str:
    """Tạo câu DDL cho bảng Gold post engagement summary trong ClickHouse."""
    # Bảng phục vụ truy vấn top posts theo số lượng like/repost.
    return f"""
    CREATE TABLE IF NOT EXISTS {GOLD_POST_ENGAGEMENT_SUMMARY_TABLE}
    (
        post_uri String,
        post_cid String,
        author_did String,
        post_text String,
        post_created_at String,
        like_count UInt64,
        repost_count UInt64,
        engagement_count UInt64,
        loaded_at DateTime DEFAULT now()
    )
    ENGINE = MergeTree
    ORDER BY (engagement_count, post_uri)
    """


def build_gold_event_volume_ddl() -> str:
    """Tạo câu DDL cho bảng Gold event volume trong ClickHouse."""
    # DDL được gom tại một nơi để create/load/check dùng cùng table contract.
    return f"""
    CREATE TABLE IF NOT EXISTS {GOLD_EVENT_VOLUME_TABLE}
    (
        event_type String,
        event_count UInt64,
        loaded_at DateTime DEFAULT now()
    )
    ENGINE = MergeTree
    ORDER BY event_type
    """


def build_gold_event_volume_1m_stream_ddl() -> str:
    """Tạo câu DDL cho bảng event volume realtime theo phút trong ClickHouse."""
    # Bảng này phục vụ Grafana time series cho luồng streaming end-to-end.
    return f"""
    CREATE TABLE IF NOT EXISTS {GOLD_EVENT_VOLUME_1M_STREAM_TABLE}
    (
        window_start DateTime,
        event_type String,
        event_count UInt64,
        spark_batch_id UInt64,
        loaded_at DateTime DEFAULT now()
    )
    ENGINE = SummingMergeTree
    ORDER BY (window_start, event_type, spark_batch_id)
    """


def build_gold_content_activity_1m_stream_ddl() -> str:
    """Tạo DDL cho bảng content activity realtime theo phút."""
    # Lưu các hoạt động tạo/xóa/cập nhật nội dung để dashboard query nhanh.
    return f"""
    CREATE TABLE IF NOT EXISTS {GOLD_CONTENT_ACTIVITY_1M_STREAM_TABLE}
    (
        window_start DateTime,
        content_activity_type String,
        activity_count UInt64,
        spark_batch_id UInt64,
        loaded_at DateTime DEFAULT now()
    )
    ENGINE = SummingMergeTree
    ORDER BY (window_start, content_activity_type, spark_batch_id)
    """


def build_gold_engagement_1m_stream_ddl() -> str:
    """Tạo DDL cho bảng engagement realtime theo phút."""
    # Lưu like/repost/reply để dashboard phân tích mức độ tương tác.
    return f"""
    CREATE TABLE IF NOT EXISTS {GOLD_ENGAGEMENT_1M_STREAM_TABLE}
    (
        window_start DateTime,
        engagement_type String,
        engagement_count UInt64,
        spark_batch_id UInt64,
        loaded_at DateTime DEFAULT now()
    )
    ENGINE = SummingMergeTree
    ORDER BY (window_start, engagement_type, spark_batch_id)
    """


def build_gold_network_activity_1m_stream_ddl() -> str:
    """Tạo DDL cho bảng network activity realtime theo phút."""
    # Lưu follow/unfollow để quan sát biến động social graph.
    return f"""
    CREATE TABLE IF NOT EXISTS {GOLD_NETWORK_ACTIVITY_1M_STREAM_TABLE}
    (
        window_start DateTime,
        network_activity_type String,
        activity_count UInt64,
        spark_batch_id UInt64,
        loaded_at DateTime DEFAULT now()
    )
    ENGINE = SummingMergeTree
    ORDER BY (window_start, network_activity_type, spark_batch_id)
    """
