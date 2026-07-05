"""Đọc bảng Silver posts Iceberg để kiểm chứng dữ liệu."""

from bluesky_pipeline.iceberg_config import (
    ICEBERG_SILVER_POSTS_TABLE,
    create_iceberg_spark_session,
)


def main() -> None:
    """Đọc Silver posts Iceberg và in schema/count/sample."""
    # Tạo SparkSession có Iceberg catalog config.
    spark = create_iceberg_spark_session("bluesky-read-iceberg-silver-posts")
    spark.sparkContext.setLogLevel("WARN")

    # Đọc table qua Iceberg catalog để kiểm tra table metadata hoạt động.
    iceberg_posts_df = spark.table(ICEBERG_SILVER_POSTS_TABLE)

    iceberg_posts_df.printSchema()
    print(f"iceberg_silver_posts_count: {iceberg_posts_df.count()}")
    iceberg_posts_df.show(10, truncate=False)


if __name__ == "__main__":
    main()
