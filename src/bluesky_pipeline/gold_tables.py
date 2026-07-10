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

GOLD_POST_PERFORMANCE_TABLE = "bluesky.gold_post_performance"
GOLD_POST_PERFORMANCE_PATH = "s3a://bluesky-lake/gold/gold_post_performance"
GOLD_POST_PERFORMANCE_CLICKHOUSE_SOURCE_PATH = GOLD_POST_PERFORMANCE_PATH

GOLD_CONTENT_QUALITY_HOURLY_TABLE = "bluesky.gold_content_quality_hourly"
GOLD_CONTENT_QUALITY_HOURLY_PATH = (
    "s3a://bluesky-lake/gold/gold_content_quality_hourly"
)
GOLD_CONTENT_QUALITY_HOURLY_CLICKHOUSE_SOURCE_PATH = (
    GOLD_CONTENT_QUALITY_HOURLY_PATH
)

GOLD_THREAD_CONVERSATION_SUMMARY_TABLE = "bluesky.gold_thread_conversation_summary"
GOLD_THREAD_CONVERSATION_SUMMARY_PATH = (
    "s3a://bluesky-lake/gold/gold_thread_conversation_summary"
)
GOLD_THREAD_CONVERSATION_SUMMARY_CLICKHOUSE_SOURCE_PATH = (
    GOLD_THREAD_CONVERSATION_SUMMARY_PATH
)

GOLD_ACTOR_ACTIVITY_DAILY_TABLE = "bluesky.gold_actor_activity_daily"
GOLD_ACTOR_ACTIVITY_DAILY_PATH = "s3a://bluesky-lake/gold/gold_actor_activity_daily"
GOLD_ACTOR_ACTIVITY_DAILY_CLICKHOUSE_SOURCE_PATH = GOLD_ACTOR_ACTIVITY_DAILY_PATH

GOLD_EVENT_VOLUME_1M_STREAM_TABLE = "bluesky.gold_event_volume_1m_stream"
GOLD_CONTENT_ACTIVITY_1M_STREAM_TABLE = (
    "bluesky.gold_content_activity_1m_stream"
)
GOLD_ENGAGEMENT_1M_STREAM_TABLE = "bluesky.gold_engagement_1m_stream"
GOLD_NETWORK_ACTIVITY_1M_STREAM_TABLE = "bluesky.gold_network_activity_1m_stream"
GOLD_REALTIME_STREAM_BATCHES_TABLE = "bluesky.gold_realtime_stream_batches"

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


def build_gold_post_performance_ddl() -> str:
    """Tạo câu DDL cho bảng Gold post performance trong ClickHouse."""
    # Bảng này là serving mart sâu hơn cho phân tích performance từng post.
    return f"""
    CREATE TABLE IF NOT EXISTS {GOLD_POST_PERFORMANCE_TABLE}
    (
        post_uri String,
        author_did Nullable(String),
        post_created_at Nullable(DateTime),
        is_reply Nullable(UInt8),
        reply_root_uri Nullable(String),
        reply_parent_uri Nullable(String),
        text_length Nullable(UInt64),
        is_deleted UInt8,
        deleted_at Nullable(DateTime),
        post_lifetime_seconds Nullable(Int64),
        like_count UInt64,
        repost_count UInt64,
        engagement_count UInt64,
        engagement_actor_count UInt64,
        first_engagement_at Nullable(DateTime),
        last_engagement_at Nullable(DateTime),
        time_to_first_engagement_seconds Nullable(Int64),
        repost_to_like_ratio Nullable(Float64),
        engagement_score UInt64,
        loaded_at DateTime DEFAULT now()
    )
    ENGINE = MergeTree
    ORDER BY (engagement_score, engagement_count, post_uri)
    """


def build_gold_content_quality_hourly_ddl() -> str:
    """Tạo câu DDL cho bảng Gold content quality hourly trong ClickHouse."""
    # Bảng này phục vụ phân tích content lifecycle và content mix theo giờ.
    return f"""
    CREATE TABLE IF NOT EXISTS {GOLD_CONTENT_QUALITY_HOURLY_TABLE}
    (
        window_start DateTime,
        original_post_create_count UInt64,
        reply_create_count UInt64,
        post_update_count UInt64,
        reply_update_count UInt64,
        post_delete_count UInt64,
        reply_delete_count UInt64,
        total_content_events UInt64,
        reply_ratio Nullable(Float64),
        delete_ratio Nullable(Float64),
        update_ratio Nullable(Float64),
        avg_text_length Nullable(Float64),
        loaded_at DateTime DEFAULT now()
    )
    ENGINE = MergeTree
    ORDER BY window_start
    """


def build_gold_thread_conversation_summary_ddl() -> str:
    """Tạo DDL cho bảng Gold thread conversation summary trong ClickHouse."""
    # Bảng này phục vụ phân tích conversation theo từng root thread.
    return f"""
    CREATE TABLE IF NOT EXISTS {GOLD_THREAD_CONVERSATION_SUMMARY_TABLE}
    (
        reply_root_uri String,
        root_author_did Nullable(String),
        root_post_created_at Nullable(DateTime),
        reply_count UInt64,
        reply_author_count UInt64,
        first_reply_at Nullable(DateTime),
        last_reply_at Nullable(DateTime),
        conversation_duration_seconds Nullable(Int64),
        avg_reply_text_length Nullable(Float64),
        deleted_reply_count UInt64,
        loaded_at DateTime DEFAULT now()
    )
    ENGINE = MergeTree
    ORDER BY (reply_count, reply_root_uri)
    """


def build_gold_actor_activity_daily_ddl() -> str:
    """Tạo DDL cho bảng Gold actor activity daily trong ClickHouse."""
    # Bảng này phục vụ phân tích hành vi creator/engager/network builder.
    return f"""
    CREATE TABLE IF NOT EXISTS {GOLD_ACTOR_ACTIVITY_DAILY_TABLE}
    (
        activity_date Date,
        actor_did String,
        original_posts_created UInt64,
        replies_created UInt64,
        posts_updated UInt64,
        posts_deleted UInt64,
        likes_given UInt64,
        reposts_given UInt64,
        follows_created UInt64,
        follows_deleted UInt64,
        engagements_given UInt64,
        content_events_created UInt64,
        received_likes UInt64,
        received_reposts UInt64,
        received_engagements UInt64,
        unique_posts_engaged UInt64,
        unique_actors_followed UInt64,
        activity_score UInt64,
        creator_engager_ratio Nullable(Float64),
        loaded_at DateTime DEFAULT now()
    )
    ENGINE = MergeTree
    ORDER BY (activity_date, activity_score, actor_did)
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


def build_gold_realtime_stream_batches_ddl() -> str:
    """Tạo DDL cho bảng health của realtime Spark micro-batches."""
    # Bảng này phục vụ dashboard vận hành cho fast path.
    return f"""
    CREATE TABLE IF NOT EXISTS {GOLD_REALTIME_STREAM_BATCHES_TABLE}
    (
        spark_batch_id UInt64,
        batch_started_at DateTime,
        batch_finished_at DateTime,
        batch_duration_ms UInt64,
        input_rows UInt64,
        event_volume_rows UInt64,
        content_activity_rows UInt64,
        engagement_rows UInt64,
        network_activity_rows UInt64,
        is_empty UInt8,
        loaded_at DateTime DEFAULT now()
    )
    ENGINE = MergeTree
    ORDER BY spark_batch_id
    """
