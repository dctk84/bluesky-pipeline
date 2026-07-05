"""Đọc Gold event volume trên MinIO để kiểm chứng aggregate."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from bluesky_pipeline.gold_tables import GOLD_EVENT_VOLUME_PATH
from bluesky_pipeline.spark_session import create_spark_session


def read_gold_event_volume(spark: SparkSession) -> DataFrame:
    """Đọc Gold event volume từ MinIO.

    Input chính là SparkSession đã cấu hình S3A.
    Output là DataFrame chứa aggregate event volume.
    """
    return spark.read.parquet(GOLD_EVENT_VOLUME_PATH)


def show_gold_event_volume(gold_df: DataFrame) -> None:
    """In aggregate event volume để kiểm chứng kết quả Gold."""
    gold_df.printSchema()
    gold_df.orderBy(col("event_type")).show(truncate=False)


def main() -> None:
    """Đọc Gold event volume và in kết quả kiểm chứng."""
    # Tạo SparkSession local có cấu hình đọc MinIO.
    spark = create_spark_session("bluesky-read-gold-event-volume")
    spark.sparkContext.setLogLevel("WARN")

    # Đọc aggregate Gold và in kết quả.
    gold_df = read_gold_event_volume(spark)
    show_gold_event_volume(gold_df)


if __name__ == "__main__":
    main()
