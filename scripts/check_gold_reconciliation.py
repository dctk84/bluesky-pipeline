"""Reconcile Gold event volume trong ClickHouse với nguồn Silver v1."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit

from bluesky_pipeline.clickhouse_client import execute_clickhouse
from bluesky_pipeline.gold_tables import GOLD_EVENT_VOLUME_TABLE
from bluesky_pipeline.silver_tables import (
    SILVER_DELETED_RECORDS_PATH,
    SILVER_ENGAGEMENTS_PATH,
    SILVER_FOLLOWS_PATH,
    SILVER_POSTS_PATH,
)
from bluesky_pipeline.spark_session import create_spark_session


def read_silver_table(spark: SparkSession, path: str) -> DataFrame:
    """Đọc một bảng Silver từ MinIO.

    Input chính là SparkSession và path của bảng Silver.
    Output là DataFrame tương ứng.
    """
    return spark.read.parquet(path)


def build_expected_counts(spark: SparkSession) -> dict[str, int]:
    """Tính expected event count từ các bảng Silver v1.

    Input chính là SparkSession dùng để đọc Silver.
    Output là dict mapping event_type sang count kỳ vọng.
    """
    posts_count = read_silver_table(spark, SILVER_POSTS_PATH).count()
    follows_count = read_silver_table(spark, SILVER_FOLLOWS_PATH).count()
    deleted_records_count = read_silver_table(
        spark,
        SILVER_DELETED_RECORDS_PATH,
    ).count()

    engagements_df = read_silver_table(spark, SILVER_ENGAGEMENTS_PATH)
    engagement_counts = {
        row.engagement_type: row["count"]
        for row in engagements_df.groupBy("engagement_type").count().collect()
    }

    return {
        "post": posts_count,
        "follow": follows_count,
        "deleted_record": deleted_records_count,
        "like": engagement_counts.get("like", 0),
        "repost": engagement_counts.get("repost", 0),
    }


def read_clickhouse_counts() -> dict[str, int]:
    """Đọc actual event count từ ClickHouse Gold serving table.

    Output là dict mapping event_type sang count hiện có trong ClickHouse.
    """
    result = execute_clickhouse(
        f"""
        SELECT event_type, event_count
        FROM {GOLD_EVENT_VOLUME_TABLE}
        ORDER BY event_type
        """
    )

    counts: dict[str, int] = {}

    for line in result.strip().splitlines():
        event_type, event_count = line.split("\t")
        counts[event_type] = int(event_count)

    return counts


def print_reconciliation(expected: dict[str, int], actual: dict[str, int]) -> None:
    """In kết quả reconciliation giữa Silver expected và ClickHouse actual."""
    all_event_types = sorted(set(expected) | set(actual))
    has_mismatch = False

    print("event_type\texpected\tactual\tstatus")

    for event_type in all_event_types:
        expected_count = expected.get(event_type, 0)
        actual_count = actual.get(event_type, 0)
        status = "OK" if expected_count == actual_count else "MISMATCH"

        if status != "OK":
            has_mismatch = True

        print(f"{event_type}\t{expected_count}\t{actual_count}\t{status}")

    if has_mismatch:
        raise SystemExit("Gold reconciliation failed")

    print("Gold reconciliation passed")


def main() -> None:
    """So sánh Gold ClickHouse với nguồn Silver v1."""
    # Tạo SparkSession local có cấu hình đọc MinIO.
    spark = create_spark_session("bluesky-check-gold-reconciliation")
    spark.sparkContext.setLogLevel("WARN")

    # Tính expected từ Silver và actual từ ClickHouse rồi so sánh.
    expected_counts = build_expected_counts(spark)
    actual_counts = read_clickhouse_counts()
    print_reconciliation(expected_counts, actual_counts)


if __name__ == "__main__":
    main()
