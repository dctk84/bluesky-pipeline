"""Build Gold network growth daily mart từ Gold modeled Iceberg."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from bluesky_pipeline.gold_analytics_transformations import (
    build_gold_network_growth_daily,
)
from bluesky_pipeline.gold_tables import GOLD_NETWORK_GROWTH_DAILY_PATH
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


def write_gold_network_growth_daily(gold_df: DataFrame) -> None:
    """Ghi network growth daily mart xuống MinIO staging path.

    Input chính là DataFrame `gold_network_growth_daily`.
    Output là dữ liệu Parquet dùng để load vào ClickHouse.
    """
    # Staging path này có thể rebuild từ Gold modeled nên local dùng overwrite.
    gold_df.write.mode("overwrite").parquet(GOLD_NETWORK_GROWTH_DAILY_PATH)


def main() -> None:
    """Build network growth daily từ Gold modeled và in top rows."""
    spark = create_iceberg_spark_session("bluesky-build-gold-network-growth-daily")
    spark.sparkContext.setLogLevel("WARN")

    gold_fact_network_events_df = read_gold_modeled_table(
        spark,
        "gold_fact_network_events",
    )

    gold_df = build_gold_network_growth_daily(gold_fact_network_events_df)
    write_gold_network_growth_daily(gold_df)

    print(f"gold_network_growth_daily_count: {gold_df.count()}")
    gold_df.orderBy(
        col("net_follow_count").desc(),
        col("follow_count").desc(),
        col("activity_date").desc(),
        col("target_actor_did").asc_nulls_last(),
    ).show(20, truncate=False)


if __name__ == "__main__":
    main()
