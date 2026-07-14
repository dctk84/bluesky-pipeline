"""Build Gold actor activity daily mart từ Gold modeled Iceberg."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from bluesky_pipeline.transforms.gold_analytics_transformations import (
    build_gold_actor_activity_daily,
)
from bluesky_pipeline.schemas.gold_tables import GOLD_ACTOR_ACTIVITY_DAILY_PATH
from bluesky_pipeline.config.iceberg import (
    ICEBERG_GOLD_TABLES,
    create_iceberg_spark_session,
)


def read_gold_modeled_table(spark: SparkSession, table_name: str) -> DataFrame:
    """Đọc một bảng Gold modeled Iceberg theo table contract.

    Input chính là SparkSession và tên bảng Gold modeled.
    Output là DataFrame Iceberg tương ứng.
    """
    return spark.table(ICEBERG_GOLD_TABLES[table_name])


def write_gold_actor_activity_daily(gold_df: DataFrame) -> None:
    """Ghi actor activity daily mart xuống MinIO staging path.

    Input chính là DataFrame `gold_actor_activity_daily`.
    Output là dữ liệu Parquet dùng để load vào ClickHouse.
    """
    # Staging path này có thể rebuild từ Gold modeled nên local dùng overwrite.
    gold_df.write.mode("overwrite").parquet(GOLD_ACTOR_ACTIVITY_DAILY_PATH)


def main() -> None:
    """Build actor activity daily từ Gold modeled và in top rows."""
    spark = create_iceberg_spark_session("bluesky-build-gold-actor-activity-daily")
    spark.sparkContext.setLogLevel("WARN")

    gold_dim_actors_df = read_gold_modeled_table(spark, "gold_dim_actors")
    gold_dim_posts_df = read_gold_modeled_table(spark, "gold_dim_posts")
    gold_fact_content_events_df = read_gold_modeled_table(
        spark,
        "gold_fact_content_events",
    )
    gold_fact_engagement_events_df = read_gold_modeled_table(
        spark,
        "gold_fact_engagement_events",
    )
    gold_fact_network_events_df = read_gold_modeled_table(
        spark,
        "gold_fact_network_events",
    )

    gold_df = build_gold_actor_activity_daily(
        gold_dim_actors_df,
        gold_dim_posts_df,
        gold_fact_content_events_df,
        gold_fact_engagement_events_df,
        gold_fact_network_events_df,
    )

    write_gold_actor_activity_daily(gold_df)

    print(f"gold_actor_activity_daily_count: {gold_df.count()}")
    gold_df.orderBy(
        col("activity_score").desc(),
        col("activity_date").desc(),
        col("actor_did"),
    ).show(20, truncate=False)


if __name__ == "__main__":
    main()
