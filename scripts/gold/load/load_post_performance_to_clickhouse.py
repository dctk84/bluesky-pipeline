"""Load Gold post performance mart từ MinIO vào ClickHouse."""

from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from bluesky_pipeline.clients.clickhouse import execute_clickhouse
from bluesky_pipeline.schemas.gold_tables import (
    GOLD_POST_PERFORMANCE_CLICKHOUSE_SOURCE_PATH,
    GOLD_POST_PERFORMANCE_TABLE,
)
from bluesky_pipeline.config.spark import create_spark_session


CLICKHOUSE_COLUMNS = [
    "post_uri",
    "author_did",
    "post_created_at",
    "is_reply",
    "reply_root_uri",
    "reply_parent_uri",
    "text_length",
    "is_deleted",
    "deleted_at",
    "post_lifetime_seconds",
    "like_count",
    "repost_count",
    "engagement_count",
    "engagement_actor_count",
    "first_engagement_at",
    "last_engagement_at",
    "time_to_first_engagement_seconds",
    "repost_to_like_ratio",
    "engagement_score",
]


def read_gold_post_performance(spark: SparkSession) -> DataFrame:
    """Đọc Gold post performance mart từ MinIO.

    Input chính là SparkSession đã cấu hình S3A.
    Output là DataFrame post performance đã build từ Gold modeled.
    """
    return spark.read.parquet(GOLD_POST_PERFORMANCE_CLICKHOUSE_SOURCE_PATH)


def serialize_clickhouse_value(value: Any) -> Any:
    """Chuẩn hóa giá trị Python thành JSON value ClickHouse đọc được."""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")

    if isinstance(value, date):
        return value.isoformat()

    return value


def build_json_each_row_payload(gold_df: DataFrame) -> str:
    """Chuyển DataFrame post performance thành JSONEachRow payload.

    Input chính là DataFrame `gold_post_performance`.
    Output là chuỗi JSONEachRow để insert ClickHouse.
    """
    rows = gold_df.select(*CLICKHOUSE_COLUMNS).collect()
    lines = []

    for row in rows:
        row_dict = row.asDict()
        # Boolean trong Spark/Python được cast sang 0/1 cho cột UInt8 của ClickHouse.
        row_dict["is_reply"] = (
            None if row_dict["is_reply"] is None else int(bool(row_dict["is_reply"]))
        )
        row_dict["is_deleted"] = int(bool(row_dict["is_deleted"]))

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


def load_gold_post_performance(gold_df: DataFrame) -> None:
    """Truncate và load lại post performance vào ClickHouse.

    Input chính là DataFrame post performance.
    Output là dữ liệu được ghi vào ClickHouse serving table.
    """
    # Local rebuild truncate trước để chạy lại script không nhân đôi dữ liệu.
    execute_clickhouse(f"TRUNCATE TABLE {GOLD_POST_PERFORMANCE_TABLE}")

    json_payload = build_json_each_row_payload(gold_df)
    execute_clickhouse(
        f"""
        INSERT INTO {GOLD_POST_PERFORMANCE_TABLE}
        FORMAT JSONEachRow
        """,
        body=json_payload,
    )


def main() -> None:
    """Load Gold post performance vào ClickHouse và in top rows kiểm chứng."""
    spark = create_spark_session("bluesky-load-gold-post-performance-clickhouse")
    spark.sparkContext.setLogLevel("WARN")

    gold_df = read_gold_post_performance(spark)
    load_gold_post_performance(gold_df)

    result = execute_clickhouse(
        f"""
        SELECT
            post_uri,
            like_count,
            repost_count,
            engagement_count,
            engagement_score,
            time_to_first_engagement_seconds
        FROM {GOLD_POST_PERFORMANCE_TABLE}
        ORDER BY engagement_score DESC, engagement_count DESC, post_uri
        LIMIT 20
        """
    )
    print(result)


if __name__ == "__main__":
    main()
