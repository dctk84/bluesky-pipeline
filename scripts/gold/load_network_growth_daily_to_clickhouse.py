"""Load Gold network growth daily mart từ MinIO vào ClickHouse."""

from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from bluesky_pipeline.clickhouse_client import execute_clickhouse
from bluesky_pipeline.gold_tables import (
    GOLD_NETWORK_GROWTH_DAILY_CLICKHOUSE_SOURCE_PATH,
    GOLD_NETWORK_GROWTH_DAILY_TABLE,
)
from bluesky_pipeline.spark_session import create_spark_session


CLICKHOUSE_COLUMNS = [
    "activity_date",
    "target_actor_did",
    "follow_count",
    "unfollow_count",
    "net_follow_count",
    "unique_follower_count",
    "first_follow_at",
    "last_follow_at",
]


def read_gold_network_growth_daily(spark: SparkSession) -> DataFrame:
    """Đọc Gold network growth daily mart từ MinIO.

    Input chính là SparkSession đã cấu hình S3A.
    Output là DataFrame network growth daily đã build từ Gold modeled.
    """
    return spark.read.parquet(GOLD_NETWORK_GROWTH_DAILY_CLICKHOUSE_SOURCE_PATH)


def serialize_clickhouse_value(value: Any) -> Any:
    """Chuẩn hóa giá trị Python thành JSON value ClickHouse đọc được."""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")

    if isinstance(value, date):
        return value.isoformat()

    return value


def build_json_each_row_payload(gold_df: DataFrame) -> str:
    """Chuyển network growth daily thành JSONEachRow payload.

    Input chính là DataFrame `gold_network_growth_daily`.
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


def load_gold_network_growth_daily(gold_df: DataFrame) -> None:
    """Truncate và load lại network growth daily vào ClickHouse.

    Input chính là DataFrame network growth daily.
    Output là dữ liệu được ghi vào ClickHouse serving table.
    """
    # Local rebuild truncate trước để chạy lại script không nhân đôi dữ liệu.
    execute_clickhouse(f"TRUNCATE TABLE {GOLD_NETWORK_GROWTH_DAILY_TABLE}")

    json_payload = build_json_each_row_payload(gold_df)
    execute_clickhouse(
        f"""
        INSERT INTO {GOLD_NETWORK_GROWTH_DAILY_TABLE}
        FORMAT JSONEachRow
        """,
        body=json_payload,
    )


def main() -> None:
    """Load network growth daily vào ClickHouse và in top rows."""
    spark = create_spark_session("bluesky-load-gold-network-growth-daily")
    spark.sparkContext.setLogLevel("WARN")

    gold_df = read_gold_network_growth_daily(spark)
    load_gold_network_growth_daily(gold_df)

    result = execute_clickhouse(
        f"""
        SELECT
            activity_date,
            target_actor_did,
            follow_count,
            unfollow_count,
            net_follow_count,
            unique_follower_count
        FROM {GOLD_NETWORK_GROWTH_DAILY_TABLE}
        ORDER BY net_follow_count DESC, follow_count DESC, activity_date DESC
        LIMIT 20
        """
    )
    print(result)


if __name__ == "__main__":
    main()
