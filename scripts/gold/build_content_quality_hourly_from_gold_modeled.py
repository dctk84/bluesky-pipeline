"""Build Gold content quality hourly mart từ Gold modeled Iceberg."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from bluesky_pipeline.gold_analytics_transformations import (
    build_gold_content_quality_hourly,
)
from bluesky_pipeline.gold_tables import GOLD_CONTENT_QUALITY_HOURLY_PATH
from bluesky_pipeline.iceberg_config import (
    ICEBERG_GOLD_TABLES,
    create_iceberg_spark_session,
)


def read_gold_modeled_table(spark: SparkSession, table_name: str) -> DataFrame:
    """Đọc một bảng Gold modeled Iceberg theo table contract.

    Input chính là SparkSession và tên bảng Gold modeled.
    Output là DataFrame Iceberg tương ứng.
    """
    return spark.table(ICEBERG_GOLD_TABLES[table_name])


def write_gold_content_quality_hourly(gold_df: DataFrame) -> None:
    """Ghi content quality hourly mart xuống MinIO staging path.

    Input chính là DataFrame `gold_content_quality_hourly`.
    Output là dữ liệu Parquet dùng để load vào ClickHouse.
    """
    # Staging path này có thể rebuild từ Gold modeled nên local dùng overwrite.
    gold_df.write.mode("overwrite").parquet(GOLD_CONTENT_QUALITY_HOURLY_PATH)


def main() -> None:
    """Build Gold content quality hourly từ Gold modeled và in rows kiểm chứng."""
    spark = create_iceberg_spark_session(
        "bluesky-build-gold-content-quality-hourly"
    )
    spark.sparkContext.setLogLevel("WARN")

    gold_fact_content_events_df = read_gold_modeled_table(
        spark,
        "gold_fact_content_events",
    )
    gold_dim_posts_df = read_gold_modeled_table(spark, "gold_dim_posts")

    gold_df = build_gold_content_quality_hourly(
        gold_fact_content_events_df,
        gold_dim_posts_df,
    )

    write_gold_content_quality_hourly(gold_df)

    print(f"gold_content_quality_hourly_count: {gold_df.count()}")
    gold_df.orderBy(col("window_start").desc()).show(24, truncate=False)


if __name__ == "__main__":
    main()
