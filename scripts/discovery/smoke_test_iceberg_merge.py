"""Smoke test Iceberg MERGE INTO bằng Spark catalog local."""

from __future__ import annotations

from pyspark.sql import SparkSession

from bluesky_pipeline.iceberg_config import (
    ICEBERG_CATALOG_NAME,
    ICEBERG_GOLD_NAMESPACE,
    create_iceberg_spark_session,
)


MERGE_TEST_TABLE = (
    f"{ICEBERG_CATALOG_NAME}.{ICEBERG_GOLD_NAMESPACE}._merge_smoke_test"
)


def ensure_gold_namespace(spark: SparkSession) -> None:
    """Tạo Gold namespace nếu chưa tồn tại.

    Input chính là SparkSession có cấu hình Iceberg catalog.
    Output là namespace Gold sẵn sàng cho bảng smoke test.
    """
    spark.sql(
        f"CREATE NAMESPACE IF NOT EXISTS "
        f"{ICEBERG_CATALOG_NAME}.{ICEBERG_GOLD_NAMESPACE}"
    )


def create_test_table(spark: SparkSession) -> None:
    """Tạo bảng Iceberg test nhỏ để kiểm tra MERGE INTO.

    Input chính là SparkSession.
    Output là bảng test có 2 dòng ban đầu.
    """
    spark.sql(f"DROP TABLE IF EXISTS {MERGE_TEST_TABLE}")
    spark.sql(
        f"""
        CREATE TABLE {MERGE_TEST_TABLE}
        (
            id STRING,
            value STRING,
            updated_at TIMESTAMP
        )
        USING iceberg
        TBLPROPERTIES ('format-version' = '2')
        """
    )
    spark.sql(
        f"""
        INSERT INTO {MERGE_TEST_TABLE} VALUES
            ('a', 'old-a', TIMESTAMP '2026-07-12 10:00:00'),
            ('b', 'old-b', TIMESTAMP '2026-07-12 10:00:00')
        """
    )


def create_source_view(spark: SparkSession) -> None:
    """Tạo source view gồm một row update và một row insert.

    Input chính là SparkSession.
    Output là temp view `merge_smoke_source`.
    """
    source_df = spark.sql(
        """
        SELECT 'a' AS id, 'new-a' AS value, TIMESTAMP '2026-07-12 11:00:00' AS updated_at
        UNION ALL
        SELECT 'c' AS id, 'new-c' AS value, TIMESTAMP '2026-07-12 11:00:00' AS updated_at
        """
    )
    source_df.createOrReplaceTempView("merge_smoke_source")


def run_merge(spark: SparkSession) -> None:
    """Chạy MERGE INTO để update row cũ và insert row mới.

    Input chính là SparkSession và bảng/view đã chuẩn bị.
    Output là bảng test được merge.
    """
    spark.sql(
        f"""
        MERGE INTO {MERGE_TEST_TABLE} AS target
        USING merge_smoke_source AS source
        ON target.id = source.id
        WHEN MATCHED THEN UPDATE SET
            value = source.value,
            updated_at = source.updated_at
        WHEN NOT MATCHED THEN INSERT (id, value, updated_at)
        VALUES (source.id, source.value, source.updated_at)
        """
    )


def verify_result(spark: SparkSession) -> None:
    """Kiểm tra kết quả sau MERGE INTO.

    Input chính là SparkSession.
    Output là exception nếu kết quả merge không đúng.
    """
    result_rows = spark.sql(
        f"""
        SELECT id, value
        FROM {MERGE_TEST_TABLE}
        ORDER BY id
        """
    ).collect()
    actual = [(row["id"], row["value"]) for row in result_rows]
    expected = [
        ("a", "new-a"),
        ("b", "old-b"),
        ("c", "new-c"),
    ]

    print("merge_smoke_result:")
    for row in actual:
        print(f"- id={row[0]} value={row[1]}")

    if actual != expected:
        raise RuntimeError(f"Iceberg MERGE smoke test mismatch: {actual}")


def cleanup_test_table(spark: SparkSession) -> None:
    """Xóa bảng smoke test sau khi kiểm chứng xong."""
    spark.sql(f"DROP TABLE IF EXISTS {MERGE_TEST_TABLE}")


def main() -> None:
    """Chạy smoke test Iceberg MERGE INTO end-to-end."""
    spark = create_iceberg_spark_session("bluesky-smoke-test-iceberg-merge")
    spark.sparkContext.setLogLevel("WARN")

    try:
        ensure_gold_namespace(spark)
        create_test_table(spark)
        create_source_view(spark)
        run_merge(spark)
        verify_result(spark)
        print("Iceberg MERGE smoke test passed")
    finally:
        cleanup_test_table(spark)
        spark.stop()


if __name__ == "__main__":
    main()
