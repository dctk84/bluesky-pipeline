"""Khai báo metadata dùng chung cho các bảng Gold serving."""

CLICKHOUSE_DATABASE = "bluesky"
GOLD_EVENT_VOLUME_TABLE = "bluesky.gold_event_volume_by_type"
GOLD_EVENT_VOLUME_PATH = "s3a://bluesky-lake/gold/gold_event_volume_by_type"
GOLD_POST_ENGAGEMENT_SUMMARY_PATH = (
    "s3a://bluesky-lake/gold/gold_post_engagement_summary"
)

GOLD_EVENT_VOLUME_ICEBERG_SOURCE_PATH = (
    "s3a://bluesky-lake/gold/gold_event_volume_by_type_iceberg_source"
)

GOLD_POST_ENGAGEMENT_SUMMARY_TABLE = "bluesky.gold_post_engagement_summary"

GOLD_POST_ENGAGEMENT_SUMMARY_ICEBERG_SOURCE_PATH = (
    "s3a://bluesky-lake/gold/gold_post_engagement_summary_iceberg_source"
)

GOLD_EVENT_VOLUME_CLICKHOUSE_SOURCE_PATH = GOLD_EVENT_VOLUME_ICEBERG_SOURCE_PATH
GOLD_POST_ENGAGEMENT_SUMMARY_CLICKHOUSE_SOURCE_PATH = (
    GOLD_POST_ENGAGEMENT_SUMMARY_ICEBERG_SOURCE_PATH
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