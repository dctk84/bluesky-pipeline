"""Build toàn bộ Silver v1 dạng Iceberg từ Silver Parquet prototype."""

from bluesky_pipeline.iceberg_config import (
    ICEBERG_CATALOG_NAME,
    ICEBERG_SILVER_NAMESPACE,
    ICEBERG_SILVER_TABLES,
    create_iceberg_spark_session,
)
from bluesky_pipeline.silver_tables import SILVER_TABLE_PATHS


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


def build_iceberg_table(spark, table_name: str) -> int:
    """Build một bảng Silver Iceberg từ bảng Parquet tương ứng.

    Input chính là SparkSession và tên bảng Silver v1.
    Output là số dòng đọc lại từ Iceberg table sau khi ghi.
    """
    parquet_path = SILVER_TABLE_PATHS[table_name]
    iceberg_table = ICEBERG_SILVER_TABLES[table_name]

    # Đọc nguồn Parquet hiện tại, drop table cũ và ghi lại Iceberg table.
    source_df = spark.read.parquet(parquet_path)
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

    # Build từng bảng theo metadata chung để tránh hard-code path/table lặp lại.
    for table_name in SILVER_TABLE_PATHS:
        row_count = build_iceberg_table(spark, table_name)
        print(f"iceberg_{table_name}_count: {row_count}")


if __name__ == "__main__":
    main()
