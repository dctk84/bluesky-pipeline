"""Load Gold content quality hourly mart từ MinIO vào ClickHouse."""

from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from bluesky_pipeline.clickhouse_client import execute_clickhouse
from bluesky_pipeline.gold_tables import (
    GOLD_CONTENT_QUALITY_HOURLY_CLICKHOUSE_SOURCE_PATH,
    GOLD_CONTENT_QUALITY_HOURLY_TABLE,
)
from bluesky_pipeline.spark_session import create_spark_session


CLICKHOUSE_COLUMNS = [
    "window_start",
    "original_post_create_count",
    "reply_create_count",
    "post_update_count",
    "reply_update_count",
    "post_delete_count",
    "reply_delete_count",
    "total_content_events",
    "reply_ratio",
    "delete_ratio",
    "update_ratio",
    "avg_text_length",
]


def read_gold_content_quality_hourly(spark: SparkSession) -> DataFrame:
    """Đọc Gold content quality hourly mart từ MinIO.

    Input chính là SparkSession đã cấu hình S3A.
    Output là DataFrame content quality hourly đã build từ Gold modeled.
    """
    return spark.read.parquet(GOLD_CONTENT_QUALITY_HOURLY_CLICKHOUSE_SOURCE_PATH)


def serialize_clickhouse_value(value: Any) -> Any:
    """Chuẩn hóa giá trị Python thành JSON value ClickHouse đọc được."""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")

    if isinstance(value, date):
        return value.isoformat()

    return value


def build_json_each_row_payload(gold_df: DataFrame) -> str:
    """Chuyển DataFrame content quality hourly thành JSONEachRow payload.

    Input chính là DataFrame `gold_content_quality_hourly`.
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


def load_gold_content_quality_hourly(gold_df: DataFrame) -> None:
    """Truncate và load lại content quality hourly vào ClickHouse.

    Input chính là DataFrame content quality hourly.
    Output là dữ liệu được ghi vào ClickHouse serving table.
    """
    # Local rebuild truncate trước để chạy lại script không nhân đôi dữ liệu.
    execute_clickhouse(f"TRUNCATE TABLE {GOLD_CONTENT_QUALITY_HOURLY_TABLE}")

    json_payload = build_json_each_row_payload(gold_df)
    execute_clickhouse(
        f"""
        INSERT INTO {GOLD_CONTENT_QUALITY_HOURLY_TABLE}
        FORMAT JSONEachRow
        """,
        body=json_payload,
    )


def main() -> None:
    """Load Gold content quality hourly vào ClickHouse và in rows kiểm chứng."""
    spark = create_spark_session("bluesky-load-gold-content-quality-hourly")
    spark.sparkContext.setLogLevel("WARN")

    gold_df = read_gold_content_quality_hourly(spark)
    load_gold_content_quality_hourly(gold_df)

    result = execute_clickhouse(
        f"""
        SELECT
            window_start,
            original_post_create_count,
            reply_create_count,
            total_content_events,
            reply_ratio,
            delete_ratio,
            update_ratio
        FROM {GOLD_CONTENT_QUALITY_HOURLY_TABLE}
        ORDER BY window_start DESC
        LIMIT 24
        """
    )
    print(result)


if __name__ == "__main__":
    main()
