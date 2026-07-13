"""Kiểm tra Silver Iceberg đã sẵn sàng cho live Gold incremental."""

from __future__ import annotations

from pyspark.errors import AnalysisException

from bluesky_pipeline.iceberg_config import (
    ICEBERG_SILVER_TABLES,
    create_iceberg_spark_session,
)


def count_table_rows(spark, table_name: str, iceberg_table: str) -> int:
    """Đọc một bảng Silver Iceberg và trả về số dòng hiện có.

    Input chính là SparkSession, tên logic của bảng và full table name trong
    Iceberg catalog. Output là row count để xác nhận bảng tồn tại và đọc được.
    """
    try:
        row = spark.sql(
            f"SELECT COUNT(*) AS row_count FROM {iceberg_table}"
        ).collect()[0]
    except AnalysisException as error:
        raise SystemExit(
            "Silver Iceberg readiness failed: "
            f"table={table_name} iceberg_table={iceberg_table} is not readable"
        ) from error

    return int(row["row_count"] or 0)


def main() -> None:
    """Kiểm tra các bảng Silver Iceberg có thể đọc được trong live mode."""
    # Live mode chỉ cần biết Silver đã tạo table và có dữ liệu đầu vào tối thiểu.
    spark = create_iceberg_spark_session("bluesky-check-silver-iceberg-readiness")
    spark.sparkContext.setLogLevel("WARN")

    total_rows = 0

    print("Silver Iceberg readiness")
    print("table\trow_count\tstatus")

    for table_name, iceberg_table in ICEBERG_SILVER_TABLES.items():
        row_count = count_table_rows(spark, table_name, iceberg_table)
        total_rows += row_count
        print(f"{table_name}\t{row_count}\tREADABLE")

    if total_rows == 0:
        raise SystemExit(
            "Silver Iceberg readiness failed: all Silver tables are empty"
        )

    print("\nSilver Iceberg readiness passed")


if __name__ == "__main__":
    main()
