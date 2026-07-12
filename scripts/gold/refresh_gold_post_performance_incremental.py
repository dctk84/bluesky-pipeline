"""Refresh incremental Gold post performance mart vào ClickHouse."""

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from bluesky_pipeline.clickhouse_client import execute_clickhouse
from bluesky_pipeline.gold_analytics_transformations import build_gold_post_performance
from bluesky_pipeline.gold_tables import (
    GOLD_POST_PERFORMANCE_TABLE,
    build_gold_post_performance_ddl,
)
from bluesky_pipeline.iceberg_config import create_iceberg_spark_session
from bluesky_pipeline.incremental_refresh import (
    build_refresh_window,
    read_last_successful_run_at,
    utc_now,
    write_last_successful_run_at,
)
from scripts.gold.incremental_serving_utils import (
    build_expected_metrics,
    build_single_key_predicate,
    chunked,
    delete_clickhouse_by_predicates,
    filter_by_received_at,
    print_reconciliation,
    read_clickhouse_metrics_for_predicates,
    read_gold_modeled_table,
)
from scripts.gold.load_post_performance_to_clickhouse import (
    build_json_each_row_payload,
)


DEFAULT_STATE_PATH = Path("data/state/gold_post_performance_incremental_refresh.json")
COUNT_METRICS = [
    "like_count",
    "repost_count",
    "engagement_count",
    "engagement_score",
]


def parse_args() -> argparse.Namespace:
    """Đọc CLI flags cho post performance incremental refresh."""
    parser = argparse.ArgumentParser(
        description="Refresh Gold post performance incrementally."
    )
    parser.add_argument(
        "--ignore-state",
        action="store_true",
        help="Bỏ qua local state và chạy như initial refresh.",
    )
    return parser.parse_args()


def build_affected_post_keys(
    incremental_content_fact_df: DataFrame,
    incremental_engagement_fact_df: DataFrame,
) -> DataFrame:
    """Xác định các post_uri bị ảnh hưởng bởi content hoặc engagement mới.

    Input là incremental content fact và engagement fact theo `received_at`.
    Output là DataFrame một cột `post_uri` đã distinct.
    """
    content_post_keys_df = incremental_content_fact_df.select("post_uri")
    engagement_post_keys_df = incremental_engagement_fact_df.select(
        col("target_post_uri").alias("post_uri")
    )

    return (
        content_post_keys_df.unionByName(engagement_post_keys_df)
        .filter(col("post_uri").isNotNull())
        .dropDuplicates(["post_uri"])
    )


def collect_post_keys(affected_post_keys_df: DataFrame) -> list[str]:
    """Collect affected post_uri values về driver để build ClickHouse predicate."""
    return [
        row["post_uri"]
        for row in affected_post_keys_df.orderBy("post_uri").collect()
    ]


def build_predicates(post_keys: list[str]) -> list[str]:
    """Build ClickHouse predicates theo chunk post_uri."""
    return [
        build_single_key_predicate("post_uri", key_chunk)
        for key_chunk in chunked(post_keys)
    ]


def replace_clickhouse_rows(affected_mart_df: DataFrame, predicates: list[str]) -> None:
    """Replace affected post performance rows trong ClickHouse.

    Input là affected mart rows và predicates theo post_uri.
    Output là ClickHouse table đã delete affected keys rồi insert rows mới.
    """
    execute_clickhouse(build_gold_post_performance_ddl())
    delete_clickhouse_by_predicates(GOLD_POST_PERFORMANCE_TABLE, predicates)

    if affected_mart_df.count() == 0:
        return

    execute_clickhouse(
        f"""
        INSERT INTO {GOLD_POST_PERFORMANCE_TABLE}
        FORMAT JSONEachRow
        """,
        body=build_json_each_row_payload(affected_mart_df),
    )


def main() -> None:
    """Refresh incremental post performance mart từ Gold modeled.

    Input chính là local state, Gold modeled Iceberg và ClickHouse.
    Output là affected post count, replace count và reconciliation result.
    """
    args = parse_args()
    last_successful_run_at = (
        None
        if args.ignore_state
        else read_last_successful_run_at(DEFAULT_STATE_PATH)
    )
    refresh_to = utc_now()
    refresh_window = build_refresh_window(last_successful_run_at, refresh_to)

    print("gold_post_performance_incremental_refresh")
    print(f"state_path: {DEFAULT_STATE_PATH}")
    print(f"last_successful_run_at: {last_successful_run_at}")
    print(f"refresh_from: {refresh_window.refresh_from.isoformat()}")
    print(f"refresh_to: {refresh_window.refresh_to.isoformat()}")
    print(f"lookback_hours: {refresh_window.lookback_hours}")

    spark = create_iceberg_spark_session("bluesky-refresh-gold-post-performance")
    spark.sparkContext.setLogLevel("WARN")

    try:
        gold_dim_posts_df = read_gold_modeled_table(spark, "gold_dim_posts")
        gold_fact_content_events_df = read_gold_modeled_table(
            spark,
            "gold_fact_content_events",
        )
        gold_fact_engagement_events_df = read_gold_modeled_table(
            spark,
            "gold_fact_engagement_events",
        )
        incremental_content_fact_df = filter_by_received_at(
            gold_fact_content_events_df,
            refresh_window,
        ).cache()
        incremental_engagement_fact_df = filter_by_received_at(
            gold_fact_engagement_events_df,
            refresh_window,
        ).cache()

        print(
            "gold_fact_content_events_incremental_rows: "
            f"{incremental_content_fact_df.count()}"
        )
        print(
            "gold_fact_engagement_events_incremental_rows: "
            f"{incremental_engagement_fact_df.count()}"
        )

        affected_post_keys_df = build_affected_post_keys(
            incremental_content_fact_df,
            incremental_engagement_fact_df,
        ).cache()
        affected_key_count = affected_post_keys_df.count()
        print(f"affected_post_uri_count: {affected_key_count}")
        affected_post_keys_df.orderBy("post_uri").show(24, truncate=False)

        if affected_key_count == 0:
            write_last_successful_run_at(DEFAULT_STATE_PATH, refresh_to)
            print("affected_rows_replaced: 0")
            print("state_updated: true")
            return

        affected_post_keys = collect_post_keys(affected_post_keys_df)
        predicates = build_predicates(affected_post_keys)
        full_mart_df = build_gold_post_performance(
            gold_dim_posts_df,
            gold_fact_engagement_events_df,
        )
        affected_mart_df = full_mart_df.join(
            affected_post_keys_df,
            "post_uri",
            "inner",
        ).cache()

        try:
            affected_rows = affected_mart_df.count()
            print(f"affected_rows_recomputed: {affected_rows}")
            affected_mart_df.orderBy(
                col("engagement_score").desc(),
                col("engagement_count").desc(),
                col("post_uri"),
            ).show(24, truncate=False)

            replace_clickhouse_rows(affected_mart_df, predicates)
            print(f"affected_rows_replaced: {affected_rows}")

            expected_metrics = build_expected_metrics(affected_mart_df, COUNT_METRICS)
            actual_metrics = read_clickhouse_metrics_for_predicates(
                GOLD_POST_PERFORMANCE_TABLE,
                COUNT_METRICS,
                predicates,
            )
            print_reconciliation(expected_metrics, actual_metrics)
            print("Gold post performance incremental check passed")

            write_last_successful_run_at(DEFAULT_STATE_PATH, refresh_to)
            print("state_updated: true")
        finally:
            affected_mart_df.unpersist()

    finally:
        for table_df in [
            locals().get("incremental_content_fact_df"),
            locals().get("incremental_engagement_fact_df"),
            locals().get("affected_post_keys_df"),
        ]:
            if table_df is not None:
                table_df.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
