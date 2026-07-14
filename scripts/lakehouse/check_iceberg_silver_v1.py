"""Reconcile toàn bộ Silver v1 Iceberg với expected data từ Bronze."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, sum as spark_sum, when

from bluesky_pipeline.config.iceberg import (
    ICEBERG_SILVER_TABLES,
    create_iceberg_spark_session,
)
from bluesky_pipeline.transforms.silver_transformations import (
    SILVER_TRANSFORMATIONS,
    read_bronze_commit_events,
)


def read_iceberg_table(spark, table_name: str) -> DataFrame:
    """Đọc một bảng Silver Iceberg v1 từ catalog.

    Input chính là SparkSession và tên bảng Silver.
    Output là DataFrame Iceberg của bảng tương ứng.
    """
    return spark.table(ICEBERG_SILVER_TABLES[table_name])


def build_metrics(table_name: str, table_df: DataFrame) -> dict[str, int]:
    """Tính các metric reconciliation cho từng bảng Silver.

    Input chính là tên bảng và DataFrame cần kiểm tra.
    Output là dict các metric có ý nghĩa với bảng đó.
    """
    aggregations = [count("*").alias("row_count")]

    if table_name == "silver_posts":
        aggregations.extend(
            [
                spark_sum("text_length").alias("text_length_sum"),
                spark_sum(col("is_reply").cast("int")).alias("reply_count"),
            ]
        )

    if table_name == "silver_engagements":
        aggregations.extend(
            [
                spark_sum(
                    when(col("engagement_type") == "like", 1).otherwise(0)
                ).alias("like_count"),
                spark_sum(
                    when(col("engagement_type") == "repost", 1).otherwise(0)
                ).alias("repost_count"),
                spark_sum(
                    when(col("subject_uri").isNotNull(), 1).otherwise(0)
                ).alias("subject_uri_count"),
            ]
        )

    if table_name == "silver_follows":
        aggregations.append(
            spark_sum(
                when(col("target_actor_did").isNotNull(), 1).otherwise(0)
            ).alias("target_actor_did_count")
        )

    if table_name == "silver_deleted_records":
        aggregations.append(
            spark_sum(
                when(col("record_uri").isNotNull(), 1).otherwise(0)
            ).alias("record_uri_count")
        )

    metrics_row = table_df.agg(*aggregations).collect()[0]

    return {
        metric_name: int(metric_value or 0)
        for metric_name, metric_value in metrics_row.asDict().items()
    }


def reconcile_table(
    table_name: str,
    expected_metrics: dict[str, int],
    iceberg_metrics: dict[str, int],
) -> bool:
    """In kết quả reconciliation cho một bảng Silver.

    Input là tên bảng, metrics expected từ Bronze và metrics từ Iceberg.
    Output là True nếu bảng có mismatch.
    """
    has_mismatch = False

    print(f"\n=== {table_name} ===")
    print("metric\texpected\ticeberg\tstatus")

    for metric in sorted(set(expected_metrics) | set(iceberg_metrics)):
        expected_value = expected_metrics.get(metric, 0)
        iceberg_value = iceberg_metrics.get(metric, 0)
        status = "OK" if expected_value == iceberg_value else "MISMATCH"

        if status != "OK":
            has_mismatch = True

        print(f"{metric}\t{expected_value}\t{iceberg_value}\t{status}")

    return has_mismatch


def main() -> None:
    """So sánh toàn bộ Silver v1 Iceberg với expected data từ Bronze."""
    # Dùng Iceberg SparkSession để đọc Bronze và Iceberg catalog.
    spark = create_iceberg_spark_session("bluesky-check-iceberg-silver-v1")
    spark.sparkContext.setLogLevel("WARN")

    has_mismatch = False
    bronze_df = read_bronze_commit_events(spark).cache()

    # Reconcile từng bảng theo transformation dùng chung từ Bronze.
    for table_name, build_expected_df in SILVER_TRANSFORMATIONS.items():
        expected_df = build_expected_df(bronze_df)
        iceberg_df = read_iceberg_table(spark, table_name)

        expected_metrics = build_metrics(table_name, expected_df)
        iceberg_metrics = build_metrics(table_name, iceberg_df)

        has_mismatch = (
            reconcile_table(table_name, expected_metrics, iceberg_metrics)
            or has_mismatch
        )

    bronze_df.unpersist()

    if has_mismatch:
        raise SystemExit("Iceberg Silver v1 reconciliation failed")

    print("\nIceberg Silver v1 reconciliation passed")


if __name__ == "__main__":
    main()
