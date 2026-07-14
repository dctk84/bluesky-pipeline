"""Build Gold modeled v1 Iceberg tables từ Silver Iceberg."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from bluesky_pipeline.transforms.gold_transformations import GOLD_TRANSFORMATIONS
from bluesky_pipeline.config.iceberg import (
    ICEBERG_CATALOG_NAME,
    ICEBERG_GOLD_NAMESPACE,
    ICEBERG_GOLD_TABLES,
    ICEBERG_SILVER_TABLES,
    create_iceberg_spark_session,
)


def ensure_gold_namespace(spark: SparkSession) -> None:
    """Tạo namespace Iceberg cho Gold modeled v1 nếu chưa tồn tại.

    Input chính là SparkSession có cấu hình Iceberg catalog.
    Output là namespace sẵn sàng để ghi các bảng Gold modeled.
    """
    # Namespace tách Gold modeled khỏi Silver để Trino nhìn schema rõ ràng.
    spark.sql(
        f"CREATE NAMESPACE IF NOT EXISTS "
        f"{ICEBERG_CATALOG_NAME}.{ICEBERG_GOLD_NAMESPACE}"
    )


def validate_gold_contract() -> None:
    """Kiểm tra registry transformation khớp với table contract Gold.

    Input là metadata trong code.
    Output là exception nếu contract và transformation bị lệch.
    """
    missing_tables = set(GOLD_TRANSFORMATIONS) - set(ICEBERG_GOLD_TABLES)
    missing_transforms = set(ICEBERG_GOLD_TABLES) - set(GOLD_TRANSFORMATIONS)

    if missing_tables or missing_transforms:
        raise ValueError(
            "Gold modeled contract mismatch: "
            f"missing_tables={sorted(missing_tables)}, "
            f"missing_transforms={sorted(missing_transforms)}"
        )


def read_silver_tables(spark: SparkSession) -> dict[str, DataFrame]:
    """Đọc toàn bộ Silver Iceberg tables cần cho Gold modeled.

    Input chính là SparkSession có cấu hình Iceberg catalog.
    Output là mapping tên bảng Silver sang DataFrame đã cache.
    """
    # Cache Silver vì nhiều Gold bảng cùng đọc lại posts/follows/deleted records.
    return {
        table_name: spark.table(iceberg_table).cache()
        for table_name, iceberg_table in ICEBERG_SILVER_TABLES.items()
    }


def build_gold_table(
    spark: SparkSession,
    table_name: str,
    silver_tables: dict[str, DataFrame],
) -> int:
    """Build một Gold modeled Iceberg table từ các Silver DataFrame.

    Input chính là SparkSession, tên bảng Gold và mapping Silver DataFrame.
    Output là số dòng đọc lại từ Iceberg table sau khi ghi.
    """
    iceberg_table = ICEBERG_GOLD_TABLES[table_name]
    source_df = GOLD_TRANSFORMATIONS[table_name](silver_tables)

    # Local rebuild dùng drop/create để giữ script dễ hiểu và dễ reset khi model đổi.
    spark.sql(f"DROP TABLE IF EXISTS {iceberg_table}")
    source_df.writeTo(iceberg_table).using("iceberg").create()

    # Đọc lại qua catalog để xác nhận table đã đăng ký metastore và query được.
    return spark.table(iceberg_table).count()


def main() -> None:
    """Build toàn bộ Gold modeled v1 Iceberg tables và in count kiểm chứng."""
    validate_gold_contract()

    spark = create_iceberg_spark_session("bluesky-build-gold-modeled-v1")
    spark.sparkContext.setLogLevel("WARN")

    ensure_gold_namespace(spark)
    silver_tables = read_silver_tables(spark)

    try:
        for table_name in GOLD_TRANSFORMATIONS:
            row_count = build_gold_table(spark, table_name, silver_tables)
            print(f"iceberg_{table_name}_count: {row_count}")
    finally:
        for silver_df in silver_tables.values():
            silver_df.unpersist()


if __name__ == "__main__":
    main()
