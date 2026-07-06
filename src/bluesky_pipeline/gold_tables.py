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

GOLD_EVENT_VOLUME_1M_STREAM_CHECKPOINT_LOCATION = (
    "s3a://bluesky-lake/checkpoints/gold_event_volume_1m_stream"
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