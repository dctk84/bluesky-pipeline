"""Load Gold event volume từ MinIO vào ClickHouse."""

from bluesky_pipeline.clients.clickhouse import execute_clickhouse

from pyspark.sql import DataFrame, SparkSession

from bluesky_pipeline.config.spark import create_spark_session

from bluesky_pipeline.schemas.gold_tables import (
    GOLD_EVENT_VOLUME_CLICKHOUSE_SOURCE_PATH,
    GOLD_EVENT_VOLUME_TABLE,
)

def read_gold_event_volume(spark: SparkSession) -> DataFrame:
    """Đọc Gold event volume staging từ MinIO.

    Input chính là SparkSession đã cấu hình S3A.
    Output là DataFrame chứa event_type và event_count.
    """
    return spark.read.parquet(GOLD_EVENT_VOLUME_CLICKHOUSE_SOURCE_PATH)


def build_csv_payload(gold_df: DataFrame) -> str:
    """Chuyển Gold DataFrame nhỏ thành CSV payload để insert ClickHouse.

    Input chính là DataFrame Gold event volume.
    Output là chuỗi CSV không header.
    """
    rows = gold_df.select("event_type", "event_count").collect()

    return "".join(f"{row.event_type},{row.event_count}\n" for row in rows)


def load_gold_event_volume(gold_df: DataFrame) -> None:
    """Truncate và load lại Gold event volume vào ClickHouse.

    Input chính là DataFrame Gold event volume.
    Output là dữ liệu được ghi vào ClickHouse serving table.
    """
    # Rebuild bảng serving từ Gold staging để đảm bảo kết quả idempotent ở local.
    execute_clickhouse(f"TRUNCATE TABLE {GOLD_EVENT_VOLUME_TABLE}")

    csv_payload = build_csv_payload(gold_df)
    execute_clickhouse(
        f"INSERT INTO {GOLD_EVENT_VOLUME_TABLE} (event_type,event_count) FORMAT CSV",
        body=csv_payload,
    )


def main() -> None:
    """Load Gold event volume vào ClickHouse và query kiểm chứng."""
    # Tạo SparkSession local có cấu hình đọc MinIO.
    spark = create_spark_session("bluesky-load-gold-event-volume-clickhouse")
    spark.sparkContext.setLogLevel("WARN")

    # Đọc Gold staging, load vào ClickHouse và in kết quả query.
    gold_df = read_gold_event_volume(spark)
    load_gold_event_volume(gold_df)

    result = execute_clickhouse(
        f"SELECT event_type,event_count FROM {GOLD_EVENT_VOLUME_TABLE} ORDER BY event_type"
    )
    print(result)


if __name__ == "__main__":
    main()
