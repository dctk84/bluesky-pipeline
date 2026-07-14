"""Load Gold post engagement summary từ MinIO vào ClickHouse."""
import json

from pyspark.sql import DataFrame, SparkSession

from bluesky_pipeline.clients.clickhouse import execute_clickhouse
from bluesky_pipeline.schemas.gold_tables import (
    GOLD_POST_ENGAGEMENT_SUMMARY_CLICKHOUSE_SOURCE_PATH,
    GOLD_POST_ENGAGEMENT_SUMMARY_TABLE,
)
from bluesky_pipeline.config.spark import create_spark_session


def read_gold_post_engagement_summary(spark: SparkSession) -> DataFrame:
    """Đọc Gold post engagement summary từ MinIO.

    Input chính là SparkSession đã cấu hình S3A.
    Output là DataFrame chứa summary engagement theo post.
    """
    return spark.read.parquet(GOLD_POST_ENGAGEMENT_SUMMARY_CLICKHOUSE_SOURCE_PATH)


def build_json_each_row_payload(gold_df: DataFrame) -> str:
    """Chuyển Gold DataFrame nhỏ thành JSONEachRow payload để insert ClickHouse.

    Input chính là DataFrame Gold post engagement summary.
    Output là chuỗi JSONEachRow, mỗi dòng là một object JSON.
    """
    rows = gold_df.select(
        "post_uri",
        "post_cid",
        "author_did",
        "post_text",
        "post_created_at",
        "like_count",
        "repost_count",
        "engagement_count",
    ).collect()

    lines = []

    for row in rows:
        # JSONEachRow an toàn hơn TSV khi text có tab, xuống dòng hoặc ký tự đặc biệt.
        lines.append(
            json.dumps(
                {
                    "post_uri": row.post_uri,
                    "post_cid": row.post_cid,
                    "author_did": row.author_did,
                    "post_text": row.post_text,
                    "post_created_at": row.post_created_at,
                    "like_count": row.like_count,
                    "repost_count": row.repost_count,
                    "engagement_count": row.engagement_count,
                },
                ensure_ascii=False,
            )
        )

    return "\n".join(lines) + "\n"


def load_gold_post_engagement_summary(gold_df: DataFrame) -> None:
    """Truncate và load lại Gold post engagement summary vào ClickHouse.

    Input chính là DataFrame Gold post engagement summary.
    Output là dữ liệu được ghi vào ClickHouse serving table.
    """
    # Rebuild bảng serving từ Gold staging để kết quả chạy lại ổn định ở local.
    execute_clickhouse(f"TRUNCATE TABLE {GOLD_POST_ENGAGEMENT_SUMMARY_TABLE}")

    json_payload = build_json_each_row_payload(gold_df)
    execute_clickhouse(
        f"""
        INSERT INTO {GOLD_POST_ENGAGEMENT_SUMMARY_TABLE}
        FORMAT JSONEachRow
        """,
        body=json_payload,
    )


def main() -> None:
    """Load Gold post engagement summary vào ClickHouse và query kiểm chứng."""
    # Tạo SparkSession local có cấu hình đọc MinIO.
    spark = create_spark_session("bluesky-load-gold-post-engagement-summary-clickhouse")
    spark.sparkContext.setLogLevel("WARN")

    # Đọc Gold từ MinIO, load vào ClickHouse và in top rows để kiểm chứng.
    gold_df = read_gold_post_engagement_summary(spark)
    load_gold_post_engagement_summary(gold_df)

    result = execute_clickhouse(
        f"""
        SELECT
            post_uri,
            like_count,
            repost_count,
            engagement_count
        FROM {GOLD_POST_ENGAGEMENT_SUMMARY_TABLE}
        ORDER BY engagement_count DESC, post_uri
        LIMIT 20
        """
    )
    print(result)


if __name__ == "__main__":
    main()
