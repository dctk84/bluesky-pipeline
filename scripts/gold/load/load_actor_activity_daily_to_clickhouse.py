"""Load Gold actor activity daily mart từ MinIO vào ClickHouse."""

from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from bluesky_pipeline.clients.clickhouse import execute_clickhouse
from bluesky_pipeline.schemas.gold_tables import (
    GOLD_ACTOR_ACTIVITY_DAILY_CLICKHOUSE_SOURCE_PATH,
    GOLD_ACTOR_ACTIVITY_DAILY_TABLE,
)
from bluesky_pipeline.config.spark import create_spark_session


CLICKHOUSE_COLUMNS = [
    "activity_date",
    "actor_did",
    "original_posts_created",
    "replies_created",
    "posts_updated",
    "posts_deleted",
    "likes_given",
    "reposts_given",
    "follows_created",
    "follows_deleted",
    "engagements_given",
    "content_events_created",
    "received_likes",
    "received_reposts",
    "received_engagements",
    "unique_posts_engaged",
    "unique_actors_followed",
    "activity_score",
    "creator_engager_ratio",
]


def read_gold_actor_activity_daily(spark: SparkSession) -> DataFrame:
    """Đọc Gold actor activity daily mart từ MinIO.

    Input chính là SparkSession đã cấu hình S3A.
    Output là DataFrame actor activity daily đã build từ Gold modeled.
    """
    return spark.read.parquet(GOLD_ACTOR_ACTIVITY_DAILY_CLICKHOUSE_SOURCE_PATH)


def serialize_clickhouse_value(value: Any) -> Any:
    """Chuẩn hóa giá trị Python thành JSON value ClickHouse đọc được."""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")

    if isinstance(value, date):
        return value.isoformat()

    return value


def build_json_each_row_payload(gold_df: DataFrame) -> str:
    """Chuyển actor activity daily thành JSONEachRow payload.

    Input chính là DataFrame `gold_actor_activity_daily`.
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


def load_gold_actor_activity_daily(gold_df: DataFrame) -> None:
    """Truncate và load lại actor activity daily vào ClickHouse.

    Input chính là DataFrame actor activity daily.
    Output là dữ liệu được ghi vào ClickHouse serving table.
    """
    # Local rebuild truncate trước để chạy lại script không nhân đôi dữ liệu.
    execute_clickhouse(f"TRUNCATE TABLE {GOLD_ACTOR_ACTIVITY_DAILY_TABLE}")

    json_payload = build_json_each_row_payload(gold_df)
    execute_clickhouse(
        f"""
        INSERT INTO {GOLD_ACTOR_ACTIVITY_DAILY_TABLE}
        FORMAT JSONEachRow
        """,
        body=json_payload,
    )


def main() -> None:
    """Load actor activity daily vào ClickHouse và in top rows."""
    spark = create_spark_session("bluesky-load-gold-actor-activity-daily")
    spark.sparkContext.setLogLevel("WARN")

    gold_df = read_gold_actor_activity_daily(spark)
    load_gold_actor_activity_daily(gold_df)

    result = execute_clickhouse(
        f"""
        SELECT
            activity_date,
            actor_did,
            activity_score,
            content_events_created,
            engagements_given,
            received_engagements,
            follows_created
        FROM {GOLD_ACTOR_ACTIVITY_DAILY_TABLE}
        ORDER BY activity_score DESC, activity_date DESC, actor_did
        LIMIT 20
        """
    )
    print(result)


if __name__ == "__main__":
    main()
