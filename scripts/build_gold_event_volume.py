"""Tạo Gold event volume aggregate từ các bảng Silver v1 trên MinIO."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, lit

from bluesky_pipeline.gold_tables import GOLD_EVENT_VOLUME_PATH
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


def build_event_volume(spark: SparkSession) -> DataFrame:
    """Tạo aggregate event volume theo event type từ Silver v1.

    Input chính là SparkSession dùng để đọc các bảng Silver.
    Output là DataFrame Gold gồm event_type và event_count.
    """
    posts_df = read_silver_table(spark, SILVER_POSTS_PATH).select(
        lit("post").alias("event_type")
    )

    engagements_df = read_silver_table(spark, SILVER_ENGAGEMENTS_PATH).select(
        col("engagement_type")
    ).select(
        col("engagement_type"),
        col("engagement_type").alias("event_type"),
    ).select(
        col("event_type")
    )

    follows_df = read_silver_table(spark, SILVER_FOLLOWS_PATH).select(
        lit("follow").alias("event_type")
    )

    deleted_records_df = read_silver_table(
        spark,
        SILVER_DELETED_RECORDS_PATH,
    ).select(
        lit("deleted_record").alias("event_type")
    )

    all_events_df = (
        posts_df
        .unionByName(engagements_df)
        .unionByName(follows_df)
        .unionByName(deleted_records_df)
    )

    return all_events_df.groupBy("event_type").agg(
        count(lit(1)).alias("event_count")
    )


def write_gold_event_volume(gold_df: DataFrame) -> None:
    """Ghi Gold event volume xuống MinIO dạng Parquet.

    Input chính là DataFrame Gold event volume.
    Output là dữ liệu Parquet ở path Gold event volume.
    """
    # Ghi overwrite vì đây là aggregate batch local có thể rebuild từ Silver.
    gold_df.write.mode("overwrite").parquet(GOLD_EVENT_VOLUME_PATH)


def main() -> None:
    """Build Gold event volume aggregate và in kết quả kiểm chứng."""
    # Tạo SparkSession local có cấu hình đọc/ghi MinIO.
    spark = create_spark_session("bluesky-build-gold-event-volume")
    spark.sparkContext.setLogLevel("WARN")

    # Build aggregate từ Silver v1 và ghi xuống Gold.
    gold_df = build_event_volume(spark)
    write_gold_event_volume(gold_df)

    gold_df.orderBy("event_type").show(truncate=False)


if __name__ == "__main__":
    main()
