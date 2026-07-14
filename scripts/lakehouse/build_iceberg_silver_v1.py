"""Build toàn bộ Silver v1 dạng Iceberg trực tiếp từ Bronze."""

from bluesky_pipeline.config.iceberg import (
    ICEBERG_CATALOG_NAME,
    ICEBERG_SILVER_NAMESPACE,
    ICEBERG_SILVER_TABLES,
    create_iceberg_spark_session,
)
from bluesky_pipeline.transforms.silver_transformations import (
    SILVER_TRANSFORMATIONS,
    read_bronze_commit_events,
)


def ensure_silver_namespace(spark) -> None:
    """Tạo namespace Iceberg cho Silver v1 nếu chưa tồn tại.

    Input chính là SparkSession có cấu hình Iceberg catalog.
    Output là namespace sẵn sàng để ghi các bảng Silver Iceberg.
    """
    # Namespace tương đương database logic cho các bảng Silver v1.
    spark.sql(
        f"CREATE NAMESPACE IF NOT EXISTS "
        f"{ICEBERG_CATALOG_NAME}.{ICEBERG_SILVER_NAMESPACE}"
    )


def build_iceberg_table(spark, table_name: str, bronze_df) -> int:
    """Build một bảng Silver Iceberg từ Bronze commit events.

    Input chính là SparkSession, tên bảng Silver v1 và Bronze DataFrame.
    Output là số dòng đọc lại từ Iceberg table sau khi ghi.
    """
    iceberg_table = ICEBERG_SILVER_TABLES[table_name]
    source_df = SILVER_TRANSFORMATIONS[table_name](bronze_df)

    # Ghi overwrite logic bằng cách drop/create để đơn giản trong môi trường local.
    spark.sql(f"DROP TABLE IF EXISTS {iceberg_table}")
    source_df.writeTo(iceberg_table).using("iceberg").create()

    # Đọc lại qua catalog để xác nhận table vừa ghi usable.
    return spark.table(iceberg_table).count()


def main() -> None:
    """Build toàn bộ Silver v1 Iceberg tables và in count kiểm chứng."""
    # Tạo SparkSession có Iceberg catalog config.
    spark = create_iceberg_spark_session("bluesky-build-iceberg-silver-v1")
    spark.sparkContext.setLogLevel("WARN")

    ensure_silver_namespace(spark)
    bronze_df = read_bronze_commit_events(spark).cache()

    # Build từng bảng theo transformation dùng chung để tránh prototype Parquet.
    for table_name in SILVER_TRANSFORMATIONS:
        row_count = build_iceberg_table(spark, table_name, bronze_df)
        print(f"iceberg_{table_name}_count: {row_count}")

    bronze_df.unpersist()


if __name__ == "__main__":
    main()
