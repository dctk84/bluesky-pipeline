"""Tạo Gold post engagement summary từ các bảng Silver Iceberg v1."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, coalesce, lit, sum as spark_sum, when

from bluesky_pipeline.schemas.gold_tables import (
    GOLD_POST_ENGAGEMENT_SUMMARY_PATH,
)
from bluesky_pipeline.config.iceberg import (
    ICEBERG_SILVER_TABLES,
    create_iceberg_spark_session,
)


def read_iceberg_silver_table(spark: SparkSession, table_name: str) -> DataFrame:
    """Đọc một bảng Silver Iceberg theo tên bảng.

    Input chính là SparkSession và tên bảng Silver v1.
    Output là DataFrame Iceberg tương ứng.
    """
    return spark.table(ICEBERG_SILVER_TABLES[table_name])


def build_engagement_counts(silver_engagements_df: DataFrame) -> DataFrame:
    """Tính số like và repost theo từng post được tương tác.

    Input chính là Silver engagements Iceberg.
    Output là DataFrame aggregate theo subject_uri của post.
    """
    # Gom like/repost theo post gốc mà engagement đang trỏ tới.
    return silver_engagements_df.groupBy("subject_uri").agg(
        spark_sum(
            when(col("engagement_type") == "like", lit(1)).otherwise(lit(0))
        ).alias("like_count"),
        spark_sum(
            when(col("engagement_type") == "repost", lit(1)).otherwise(lit(0))
        ).alias("repost_count"),
        count(lit(1)).alias("engagement_count"),
    )


def build_gold_post_engagement_summary(
    silver_posts_df: DataFrame,
    engagement_counts_df: DataFrame,
) -> DataFrame:
    """Kết hợp Silver posts với engagement counts để tạo Gold summary.

    Input gồm Silver posts Iceberg và aggregate engagement theo post.
    Output là bảng Gold có một dòng cho mỗi post.
    """
    # Left join để giữ cả những post chưa có like/repost.
    joined_df = silver_posts_df.join(
        engagement_counts_df,
        silver_posts_df.post_uri == engagement_counts_df.subject_uri,
        "left",
    )

    return joined_df.select(
        col("post_uri"),
        col("cid").alias("post_cid"),
        col("author_did"),
        col("text").alias("post_text"),
        col("record_created_at").alias("post_created_at"),
        coalesce(col("like_count"), lit(0)).alias("like_count"),
        coalesce(col("repost_count"), lit(0)).alias("repost_count"),
        coalesce(col("engagement_count"), lit(0)).alias("engagement_count"),
        col("ingest_date"),
        col("ingest_hour"),
    )


def write_gold_post_engagement_summary(gold_df: DataFrame) -> None:
    """Ghi Gold post engagement summary chính xuống MinIO.

    Input chính là DataFrame Gold đã aggregate.
    Output là dữ liệu Parquet ở path Gold chính dùng để load ClickHouse.
    """
    # Ghi Gold staging chính được rebuild từ Silver Iceberg.
    gold_df.write.mode("overwrite").parquet(GOLD_POST_ENGAGEMENT_SUMMARY_PATH)


def main() -> None:
    """Build Gold post engagement summary từ Silver Iceberg và in count."""
    # Tạo SparkSession có Iceberg catalog config.
    spark = create_iceberg_spark_session(
        "bluesky-build-gold-post-engagement-summary-iceberg"
    )
    spark.sparkContext.setLogLevel("WARN")

    # Đọc Silver Iceberg, aggregate engagement, join với posts rồi ghi Gold.
    silver_posts_df = read_iceberg_silver_table(spark, "silver_posts")
    silver_engagements_df = read_iceberg_silver_table(spark, "silver_engagements")
    engagement_counts_df = build_engagement_counts(silver_engagements_df)
    gold_df = build_gold_post_engagement_summary(
        silver_posts_df,
        engagement_counts_df,
    )

    write_gold_post_engagement_summary(gold_df)

    print(f"gold_post_engagement_summary_count: {gold_df.count()}")
    gold_df.orderBy(col("engagement_count").desc(), col("post_uri")).show(
        20,
        truncate=False,
    )


if __name__ == "__main__":
    main()
