"""Đọc và kiểm tra Gold post engagement summary trên MinIO."""

from pyspark.sql.functions import col

from bluesky_pipeline.gold_tables import GOLD_POST_ENGAGEMENT_SUMMARY_PATH
from bluesky_pipeline.spark_session import create_spark_session


def main() -> None:
    """Đọc Gold post engagement summary và in schema/sample rows."""
    # Tạo SparkSession local có cấu hình đọc MinIO.
    spark = create_spark_session("bluesky-read-gold-post-engagement-summary")
    spark.sparkContext.setLogLevel("WARN")

    # Đọc Gold summary để kiểm tra schema, count và top posts theo engagement.
    gold_df = spark.read.parquet(GOLD_POST_ENGAGEMENT_SUMMARY_PATH)

    print(f"gold_post_engagement_summary_count: {gold_df.count()}")
    gold_df.printSchema()
    gold_df.orderBy(col("engagement_count").desc(), col("post_uri")).show(
        20,
        truncate=False,
    )


if __name__ == "__main__":
    main()