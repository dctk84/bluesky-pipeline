"""Tạo Gold post engagement summary từ các bảng Silver trên MinIO."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, coalesce, lit, sum as spark_sum, when

from bluesky_pipeline.gold_tables import GOLD_POST_ENGAGEMENT_SUMMARY_PATH
from bluesky_pipeline.silver_tables import (
    SILVER_ENGAGEMENTS_PATH,
    SILVER_POSTS_PATH,
)
from bluesky_pipeline.spark_session import create_spark_session


def read_silver_posts(spark: SparkSession) -> DataFrame:
    """Đọc bảng Silver posts từ MinIO.

    Input chính là SparkSession đã cấu hình S3A.
    Output là DataFrame chứa các post đã chuẩn hóa ở Silver.
    """
    return spark.read.parquet(SILVER_POSTS_PATH)


def read_silver_engagements(spark: SparkSession) -> DataFrame:
    """Đọc bảng Silver engagements từ MinIO.

    Input chính là SparkSession đã cấu hình S3A.
    Output là DataFrame chứa like/repost đã chuẩn hóa ở Silver.
    """
    return spark.read.parquet(SILVER_ENGAGEMENTS_PATH)


def build_engagement_counts(silver_engagements_df: DataFrame) -> DataFrame:
    """Tính số like và repost theo từng post được tương tác.

    Input chính là Silver engagements.
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

    Input gồm Silver posts và aggregate engagement theo post.
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
    """Ghi Gold post engagement summary xuống MinIO dạng Parquet.

    Input chính là DataFrame Gold đã aggregate.
    Output là dữ liệu Parquet ở Gold path.
    """
    # Ghi overwrite vì đây là batch build local có thể chạy lại từ Silver.
    gold_df.write.mode("overwrite").parquet(GOLD_POST_ENGAGEMENT_SUMMARY_PATH)


def main() -> None:
    """Build Gold post engagement summary và in output kiểm chứng."""
    # Tạo SparkSession local có cấu hình đọc/ghi MinIO.
    spark = create_spark_session("bluesky-build-gold-post-engagement-summary")
    spark.sparkContext.setLogLevel("WARN")

    # Đọc Silver, aggregate engagement, join với posts rồi ghi Gold.
    silver_posts_df = read_silver_posts(spark)
    silver_engagements_df = read_silver_engagements(spark)
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
