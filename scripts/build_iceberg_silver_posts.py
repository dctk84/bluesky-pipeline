"""Build thử bảng Silver posts dạng Iceberg từ Silver Parquet v1."""

from bluesky_pipeline.iceberg_config import (
    ICEBERG_CATALOG_NAME,
    ICEBERG_SILVER_NAMESPACE,
    ICEBERG_SILVER_POSTS_TABLE,
    create_iceberg_spark_session,
)
from bluesky_pipeline.silver_tables import SILVER_POSTS_PATH


def main() -> None:
    """Đọc Silver posts Parquet và ghi thử sang Iceberg table."""
    # Tạo SparkSession có Iceberg catalog config.
    spark = create_iceberg_spark_session("bluesky-build-iceberg-silver-posts")
    spark.sparkContext.setLogLevel("WARN")

    # Đọc bảng Silver posts prototype hiện tại từ Parquet.
    silver_posts_df = spark.read.parquet(SILVER_POSTS_PATH)

    # Tạo namespace Iceberg cho Silver v1 nếu chưa có.
    spark.sql(
        f"CREATE NAMESPACE IF NOT EXISTS "
        f"{ICEBERG_CATALOG_NAME}.{ICEBERG_SILVER_NAMESPACE}"
    )

    # Recreate table để lần thử local có thể chạy lại deterministic.
    spark.sql(f"DROP TABLE IF EXISTS {ICEBERG_SILVER_POSTS_TABLE}")

    # Ghi DataFrame sang Iceberg table bằng DataFrameWriterV2.
    silver_posts_df.writeTo(ICEBERG_SILVER_POSTS_TABLE).using("iceberg").create()

    # Đọc lại qua catalog để xác nhận table usable.
    iceberg_posts_df = spark.table(ICEBERG_SILVER_POSTS_TABLE)

    print(f"iceberg_silver_posts_count: {iceberg_posts_df.count()}")
    iceberg_posts_df.select(
        "post_uri",
        "author_did",
        "operation",
        "record_created_at",
        "text_length",
        "is_reply",
    ).show(10, truncate=False)


if __name__ == "__main__":
    main()
