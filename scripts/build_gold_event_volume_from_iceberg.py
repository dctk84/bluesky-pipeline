"""Tạo Gold event volume aggregate từ các bảng Silver Iceberg v1."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, lit

from bluesky_pipeline.gold_tables import GOLD_EVENT_VOLUME_ICEBERG_SOURCE_PATH
from bluesky_pipeline.iceberg_config import (
    ICEBERG_SILVER_TABLES,
    create_iceberg_spark_session,
)


def read_iceberg_silver_table(spark: SparkSession, table_name: str) -> DataFrame:
    """Đọc một bảng Silver Iceberg theo tên bảng.

    Input chính là SparkSession và tên bảng Silver v1.
    Output là DataFrame Iceberg tương ứng.
    """
    return spark.table(ICEBERG_SILVER_TABLES[table_name])


def build_event_volume(spark: SparkSession) -> DataFrame:
    """Tạo aggregate event volume theo event type từ Silver Iceberg v1.

    Input chính là SparkSession dùng để đọc Iceberg tables.
    Output là DataFrame Gold gồm event_type và event_count.
    """
    posts_df = read_iceberg_silver_table(spark, "silver_posts").select(
        lit("post").alias("event_type")
    )

    engagements_df = read_iceberg_silver_table(
        spark,
        "silver_engagements",
    ).select(
        col("engagement_type").alias("event_type")
    )

    follows_df = read_iceberg_silver_table(spark, "silver_follows").select(
        lit("follow").alias("event_type")
    )

    deleted_records_df = read_iceberg_silver_table(
        spark,
        "silver_deleted_records",
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
    """Ghi Gold event volume từ Iceberg source xuống MinIO dạng Parquet.

    Input chính là DataFrame Gold event volume.
    Output là dữ liệu Parquet ở path Gold riêng cho Iceberg source.
    """
    # Ghi path riêng để so sánh với Gold cũ, chưa thay thế luồng Parquet source.
    gold_df.write.mode("overwrite").parquet(GOLD_EVENT_VOLUME_ICEBERG_SOURCE_PATH)


def main() -> None:
    """Build Gold event volume từ Silver Iceberg và in kết quả kiểm chứng."""
    # Tạo SparkSession có Iceberg catalog config.
    spark = create_iceberg_spark_session("bluesky-build-gold-event-volume-iceberg")
    spark.sparkContext.setLogLevel("WARN")

    # Build aggregate từ Silver Iceberg v1 và ghi xuống Gold path song song.
    gold_df = build_event_volume(spark)
    write_gold_event_volume(gold_df)

    gold_df.orderBy("event_type").show(truncate=False)


if __name__ == "__main__":
    main()