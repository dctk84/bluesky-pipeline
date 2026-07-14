"""Build Gold thread conversation summary mart từ Gold modeled Iceberg."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from bluesky_pipeline.transforms.gold_analytics_transformations import (
    build_gold_thread_conversation_summary,
)
from bluesky_pipeline.schemas.gold_tables import GOLD_THREAD_CONVERSATION_SUMMARY_PATH
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


def write_gold_thread_conversation_summary(gold_df: DataFrame) -> None:
    """Ghi thread conversation summary mart xuống MinIO staging path.

    Input chính là DataFrame `gold_thread_conversation_summary`.
    Output là dữ liệu Parquet dùng để load vào ClickHouse.
    """
    # Staging path này có thể rebuild từ Gold modeled nên local dùng overwrite.
    gold_df.write.mode("overwrite").parquet(GOLD_THREAD_CONVERSATION_SUMMARY_PATH)


def main() -> None:
    """Build thread conversation summary từ Gold modeled và in top rows."""
    spark = create_iceberg_spark_session(
        "bluesky-build-gold-thread-conversation-summary"
    )
    spark.sparkContext.setLogLevel("WARN")

    gold_dim_posts_df = read_gold_modeled_table(spark, "gold_dim_posts")
    gold_fact_content_events_df = read_gold_modeled_table(
        spark,
        "gold_fact_content_events",
    )

    gold_df = build_gold_thread_conversation_summary(
        gold_dim_posts_df,
        gold_fact_content_events_df,
    )

    write_gold_thread_conversation_summary(gold_df)

    print(f"gold_thread_conversation_summary_count: {gold_df.count()}")
    gold_df.orderBy(
        col("reply_count").desc(),
        col("reply_author_count").desc(),
        col("reply_root_uri"),
    ).show(20, truncate=False)


if __name__ == "__main__":
    main()
