"""Smoke test Spark Iceberg table trên MinIO."""

from bluesky_pipeline.config.iceberg import (
    ICEBERG_CATALOG_NAME,
    create_iceberg_spark_session,
)


ICEBERG_NAMESPACE = "smoke"
ICEBERG_TABLE = f"{ICEBERG_CATALOG_NAME}.{ICEBERG_NAMESPACE}.iceberg_smoke_events"


def main() -> None:
    """Tạo, ghi và đọc thử một Iceberg table nhỏ trên MinIO."""
    # Tạo SparkSession có Iceberg runtime package và catalog config.
    spark = create_iceberg_spark_session("bluesky-smoke-test-iceberg-minio")
    spark.sparkContext.setLogLevel("WARN")

    # Tạo namespace tương đương database trong Iceberg catalog.
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ICEBERG_CATALOG_NAME}.{ICEBERG_NAMESPACE}")

    # Recreate table để smoke test chạy lại được trong môi trường local.
    spark.sql(f"DROP TABLE IF EXISTS {ICEBERG_TABLE}")
    spark.sql(
        f"""
        CREATE TABLE {ICEBERG_TABLE}
        (
            id BIGINT,
            event_type STRING,
            event_count BIGINT,
            created_at TIMESTAMP
        )
        USING iceberg
        """
    )

    # Ghi vài dòng sample để kiểm tra Iceberg write path.
    spark.sql(
        f"""
        INSERT INTO {ICEBERG_TABLE} VALUES
        (1, 'post', 119, TIMESTAMP '2026-01-01 00:00:00'),
        (2, 'like', 837, TIMESTAMP '2026-01-01 00:01:00'),
        (3, 'repost', 144, TIMESTAMP '2026-01-01 00:02:00')
        """
    )

    # Đọc lại table bằng catalog name để kiểm tra Iceberg read path.
    result_df = spark.sql(
        f"""
        SELECT id, event_type, event_count, created_at
        FROM {ICEBERG_TABLE}
        ORDER BY id
        """
    )

    print(f"iceberg_smoke_count: {result_df.count()}")
    result_df.show(truncate=False)


if __name__ == "__main__":
    main()