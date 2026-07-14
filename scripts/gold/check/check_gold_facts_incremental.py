"""Checkpoint cho các Gold fact tables trong incremental refresh."""

from __future__ import annotations

from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import count as spark_count, countDistinct

from bluesky_pipeline.config.iceberg import (
    ICEBERG_GOLD_TABLES,
    create_iceberg_spark_session,
)


GOLD_FACT_KEY_COLUMNS = {
    "gold_fact_content_events": "content_event_id",
    "gold_fact_engagement_events": "engagement_event_id",
    "gold_fact_network_events": "network_event_id",
}


def read_gold_fact_table(spark: SparkSession, table_name: str) -> DataFrame:
    """Đọc một Gold fact Iceberg table từ catalog.

    Input chính là SparkSession và tên logical table trong contract.
    Output là DataFrame của bảng Gold fact tương ứng.
    """
    iceberg_table = ICEBERG_GOLD_TABLES[table_name]

    try:
        return spark.table(iceberg_table)
    except AnalysisException as error:
        raise RuntimeError(f"Gold fact table not found: {iceberg_table}") from error


def build_key_metrics(table_df: DataFrame, key_column: str) -> dict[str, int]:
    """Tính row count, non-null key count và distinct key count.

    Input là DataFrame Gold fact và tên cột key.
    Output là dict metric dùng để kiểm tra key uniqueness.
    """
    metrics_row = table_df.agg(
        spark_count("*").alias("row_count"),
        spark_count(key_column).alias("non_null_key_count"),
        countDistinct(key_column).alias("distinct_key_count"),
    ).collect()[0]

    return {
        metric_name: int(metric_value or 0)
        for metric_name, metric_value in metrics_row.asDict().items()
    }


def print_key_check(
    table_name: str,
    key_column: str,
    metrics: dict[str, int],
) -> bool:
    """In kết quả key check cho một Gold fact table.

    Input là tên bảng, key column và metrics đã tính.
    Output là True nếu bảng có mismatch.
    """
    row_count = metrics["row_count"]
    non_null_key_count = metrics["non_null_key_count"]
    distinct_key_count = metrics["distinct_key_count"]
    status = (
        "OK"
        if row_count == non_null_key_count == distinct_key_count
        else "MISMATCH"
    )

    print(
        f"{table_name}\tkey={key_column}\t"
        f"rows={row_count}\tnon_null={non_null_key_count}\t"
        f"distinct={distinct_key_count}\t{status}"
    )

    return status != "OK"


def check_gold_fact_keys(spark: SparkSession) -> None:
    """Kiểm tra key uniqueness cho toàn bộ Gold fact tables.

    Input chính là SparkSession đọc được Iceberg catalog.
    Output là exception nếu có bảng bị null key hoặc duplicate key.
    """
    print("=== gold_fact_key_checks ===")
    has_mismatch = False

    for table_name, key_column in GOLD_FACT_KEY_COLUMNS.items():
        table_df = read_gold_fact_table(spark, table_name)
        metrics = build_key_metrics(table_df, key_column)
        has_mismatch = (
            print_key_check(table_name, key_column, metrics) or has_mismatch
        )

    if has_mismatch:
        raise SystemExit("Gold fact incremental key check failed")


def main() -> None:
    """Chạy checkpoint Gold fact incremental refresh."""
    spark = create_iceberg_spark_session("bluesky-check-gold-facts-incremental")
    spark.sparkContext.setLogLevel("WARN")

    try:
        check_gold_fact_keys(spark)
    finally:
        spark.stop()

    print("Gold fact incremental check passed")


if __name__ == "__main__":
    main()
