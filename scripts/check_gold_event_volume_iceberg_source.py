"""Reconcile Gold event volume giữa Parquet source và Iceberg source."""

from pyspark.sql import DataFrame, SparkSession

from bluesky_pipeline.gold_tables import (
    GOLD_EVENT_VOLUME_ICEBERG_SOURCE_PATH,
    GOLD_EVENT_VOLUME_PATH,
)
from bluesky_pipeline.spark_session import create_spark_session


def read_gold_table(spark: SparkSession, path: str) -> DataFrame:
    """Đọc một bảng Gold Parquet từ MinIO.

    Input chính là SparkSession và path Gold cần đọc.
    Output là DataFrame Gold tương ứng.
    """
    return spark.read.parquet(path)


def build_counts(gold_df: DataFrame) -> dict[str, int]:
    """Chuyển Gold event volume thành dict để đối chiếu.

    Input chính là DataFrame có event_type và event_count.
    Output là dict mapping event_type sang event_count.
    """
    return {
        row.event_type: int(row.event_count)
        for row in gold_df.select("event_type", "event_count").collect()
    }


def print_reconciliation(expected: dict[str, int], actual: dict[str, int]) -> None:
    """In kết quả reconciliation giữa Gold cũ và Gold từ Iceberg source."""
    has_mismatch = False

    print("event_type\tparquet_source\ticeberg_source\tstatus")

    for event_type in sorted(set(expected) | set(actual)):
        expected_count = expected.get(event_type, 0)
        actual_count = actual.get(event_type, 0)
        status = "OK" if expected_count == actual_count else "MISMATCH"

        if status != "OK":
            has_mismatch = True

        print(f"{event_type}\t{expected_count}\t{actual_count}\t{status}")

    if has_mismatch:
        raise SystemExit("Gold event volume Iceberg source reconciliation failed")

    print("Gold event volume Iceberg source reconciliation passed")


def main() -> None:
    """So sánh Gold event volume từ Parquet Silver và Iceberg Silver."""
    # Tạo SparkSession local có cấu hình đọc MinIO.
    spark = create_spark_session("bluesky-check-gold-event-volume-iceberg-source")
    spark.sparkContext.setLogLevel("WARN")

    parquet_source_df = read_gold_table(spark, GOLD_EVENT_VOLUME_PATH)
    iceberg_source_df = read_gold_table(spark, GOLD_EVENT_VOLUME_ICEBERG_SOURCE_PATH)

    parquet_counts = build_counts(parquet_source_df)
    iceberg_counts = build_counts(iceberg_source_df)

    print_reconciliation(parquet_counts, iceberg_counts)


if __name__ == "__main__":
    main()