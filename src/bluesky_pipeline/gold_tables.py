"""Khai báo metadata dùng chung cho các bảng Gold serving."""

CLICKHOUSE_DATABASE = "bluesky"
GOLD_EVENT_VOLUME_TABLE = "bluesky.gold_event_volume_by_type"
GOLD_EVENT_VOLUME_PATH = "s3a://bluesky-lake/gold/gold_event_volume_by_type"


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