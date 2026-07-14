"""Load Gold thread conversation summary mart từ MinIO vào ClickHouse."""

from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from bluesky_pipeline.clients.clickhouse import execute_clickhouse
from bluesky_pipeline.schemas.gold_tables import (
    GOLD_THREAD_CONVERSATION_SUMMARY_CLICKHOUSE_SOURCE_PATH,
    GOLD_THREAD_CONVERSATION_SUMMARY_TABLE,
)
from bluesky_pipeline.config.spark import create_spark_session


CLICKHOUSE_COLUMNS = [
    "reply_root_uri",
    "root_author_did",
    "root_post_created_at",
    "reply_count",
    "reply_author_count",
    "first_reply_at",
    "last_reply_at",
    "conversation_duration_seconds",
    "avg_reply_text_length",
    "deleted_reply_count",
]


def read_gold_thread_conversation_summary(spark: SparkSession) -> DataFrame:
    """Đọc Gold thread conversation summary mart từ MinIO.

    Input chính là SparkSession đã cấu hình S3A.
    Output là DataFrame thread conversation summary đã build từ Gold modeled.
    """
    return spark.read.parquet(GOLD_THREAD_CONVERSATION_SUMMARY_CLICKHOUSE_SOURCE_PATH)


def serialize_clickhouse_value(value: Any) -> Any:
    """Chuẩn hóa giá trị Python thành JSON value ClickHouse đọc được."""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")

    if isinstance(value, date):
        return value.isoformat()

    return value


def build_json_each_row_payload(gold_df: DataFrame) -> str:
    """Chuyển thread conversation summary thành JSONEachRow payload.

    Input chính là DataFrame `gold_thread_conversation_summary`.
    Output là chuỗi JSONEachRow để insert ClickHouse.
    """
    rows = gold_df.select(*CLICKHOUSE_COLUMNS).collect()
    lines = []

    for row in rows:
        row_dict = row.asDict()
        lines.append(
            json.dumps(
                {
                    column: serialize_clickhouse_value(row_dict[column])
                    for column in CLICKHOUSE_COLUMNS
                },
                ensure_ascii=False,
            )
        )

    return "\n".join(lines) + "\n"


def load_gold_thread_conversation_summary(gold_df: DataFrame) -> None:
    """Truncate và load lại thread conversation summary vào ClickHouse.

    Input chính là DataFrame thread conversation summary.
    Output là dữ liệu được ghi vào ClickHouse serving table.
    """
    # Local rebuild truncate trước để chạy lại script không nhân đôi dữ liệu.
    execute_clickhouse(f"TRUNCATE TABLE {GOLD_THREAD_CONVERSATION_SUMMARY_TABLE}")

    json_payload = build_json_each_row_payload(gold_df)
    execute_clickhouse(
        f"""
        INSERT INTO {GOLD_THREAD_CONVERSATION_SUMMARY_TABLE}
        FORMAT JSONEachRow
        """,
        body=json_payload,
    )


def main() -> None:
    """Load thread conversation summary vào ClickHouse và in top rows."""
    spark = create_spark_session("bluesky-load-gold-thread-conversation-summary")
    spark.sparkContext.setLogLevel("WARN")

    gold_df = read_gold_thread_conversation_summary(spark)
    load_gold_thread_conversation_summary(gold_df)

    result = execute_clickhouse(
        f"""
        SELECT
            reply_root_uri,
            reply_count,
            reply_author_count,
            conversation_duration_seconds,
            deleted_reply_count
        FROM {GOLD_THREAD_CONVERSATION_SUMMARY_TABLE}
        ORDER BY reply_count DESC, reply_author_count DESC, reply_root_uri
        LIMIT 20
        """
    )
    print(result)


if __name__ == "__main__":
    main()
