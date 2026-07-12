"""Refresh incremental Gold actor activity daily mart vào ClickHouse."""

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_date

from bluesky_pipeline.clickhouse_client import execute_clickhouse
from bluesky_pipeline.gold_analytics_transformations import build_gold_actor_activity_daily
from bluesky_pipeline.gold_tables import (
    GOLD_ACTOR_ACTIVITY_DAILY_TABLE,
    build_gold_actor_activity_daily_ddl,
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
    build_tuple_key_predicate,
    chunked,
    delete_clickhouse_by_predicates,
    filter_by_received_at,
    print_reconciliation,
    read_clickhouse_metrics_for_predicates,
    read_gold_modeled_table,
)
from scripts.gold.load_actor_activity_daily_to_clickhouse import (
    build_json_each_row_payload,
)


DEFAULT_STATE_PATH = Path(
    "data/state/gold_actor_activity_daily_incremental_refresh.json"
)
COUNT_METRICS = [
    "original_posts_created",
    "replies_created",
    "posts_updated",
    "posts_deleted",
    "likes_given",
    "reposts_given",
    "follows_created",
    "follows_deleted",
    "engagements_given",
    "content_events_created",
    "received_likes",
    "received_reposts",
    "received_engagements",
    "unique_posts_engaged",
    "unique_actors_followed",
    "activity_score",
]


def parse_args() -> argparse.Namespace:
    """Đọc CLI flags cho actor activity daily incremental refresh."""
    parser = argparse.ArgumentParser(
        description="Refresh Gold actor activity daily incrementally."
    )
    parser.add_argument(
        "--ignore-state",
        action="store_true",
        help="Bỏ qua local state và chạy như initial refresh.",
    )
    return parser.parse_args()


def build_received_engagement_keys(
    engagement_fact_df: DataFrame,
    gold_dim_posts_df: DataFrame,
) -> DataFrame:
    """Tính affected actor/date cho engagement mà actor nhận được.

    Input là engagement fact rows và Gold dim posts.
    Output là DataFrame `activity_date, actor_did` của post authors nhận engagement.
    """
    post_authors_df = gold_dim_posts_df.select(
        col("post_uri").alias("target_post_uri"),
        col("author_did").alias("received_actor_did"),
    ).filter(col("received_actor_did").isNotNull())

    return (
        engagement_fact_df.join(post_authors_df, "target_post_uri", "inner")
        .select(
            to_date(col("event_time")).alias("activity_date"),
            col("received_actor_did").alias("actor_did"),
        )
        .filter(col("activity_date").isNotNull() & col("actor_did").isNotNull())
    )


def build_changed_post_received_engagement_keys(
    incremental_content_fact_df: DataFrame,
    gold_fact_engagement_events_df: DataFrame,
    gold_dim_posts_df: DataFrame,
) -> DataFrame:
    """Tính affected received-engagement keys khi post dimension thay đổi.

    Input là content fact mới, full engagement fact và Gold dim posts.
    Output là actor/date keys của engagement lịch sử trỏ tới post vừa đổi state.
    """
    changed_post_keys_df = incremental_content_fact_df.select(
        col("post_uri").alias("target_post_uri")
    ).filter(col("target_post_uri").isNotNull()).dropDuplicates(["target_post_uri"])

    affected_engagements_df = gold_fact_engagement_events_df.join(
        changed_post_keys_df,
        "target_post_uri",
        "inner",
    )

    return build_received_engagement_keys(affected_engagements_df, gold_dim_posts_df)


def build_affected_actor_activity_keys(
    incremental_content_fact_df: DataFrame,
    incremental_engagement_fact_df: DataFrame,
    incremental_network_fact_df: DataFrame,
    gold_fact_engagement_events_df: DataFrame,
    gold_dim_posts_df: DataFrame,
) -> DataFrame:
    """Xác định affected `activity_date + actor_did` cho actor daily mart.

    Input là các incremental facts, full engagement fact và dim posts.
    Output là DataFrame key đã distinct.
    """
    content_keys_df = incremental_content_fact_df.select(
        to_date(col("event_time")).alias("activity_date"),
        col("author_did").alias("actor_did"),
    )
    engagement_given_keys_df = incremental_engagement_fact_df.select(
        to_date(col("event_time")).alias("activity_date"),
        col("actor_did"),
    )
    network_keys_df = incremental_network_fact_df.select(
        to_date(col("event_time")).alias("activity_date"),
        col("actor_did"),
    )
    received_engagement_keys_df = build_received_engagement_keys(
        incremental_engagement_fact_df,
        gold_dim_posts_df,
    )
    changed_post_received_keys_df = build_changed_post_received_engagement_keys(
        incremental_content_fact_df,
        gold_fact_engagement_events_df,
        gold_dim_posts_df,
    )

    return (
        content_keys_df.unionByName(engagement_given_keys_df)
        .unionByName(network_keys_df)
        .unionByName(received_engagement_keys_df)
        .unionByName(changed_post_received_keys_df)
        .filter(col("activity_date").isNotNull() & col("actor_did").isNotNull())
        .dropDuplicates(["activity_date", "actor_did"])
    )


def collect_actor_activity_keys(affected_keys_df: DataFrame) -> list[tuple[object, str]]:
    """Collect affected actor/date keys về driver."""
    return [
        (row["activity_date"], row["actor_did"])
        for row in affected_keys_df.orderBy("activity_date", "actor_did").collect()
    ]


def build_predicates(keys: list[tuple[object, str]]) -> list[str]:
    """Build ClickHouse predicates theo chunk `activity_date + actor_did`."""
    return [
        build_tuple_key_predicate(("activity_date", "actor_did"), key_chunk)
        for key_chunk in chunked(keys)
    ]


def replace_clickhouse_rows(affected_mart_df: DataFrame, predicates: list[str]) -> None:
    """Replace affected actor activity daily rows trong ClickHouse."""
    execute_clickhouse(build_gold_actor_activity_daily_ddl())
    delete_clickhouse_by_predicates(GOLD_ACTOR_ACTIVITY_DAILY_TABLE, predicates)

    if affected_mart_df.count() == 0:
        return

    execute_clickhouse(
        f"""
        INSERT INTO {GOLD_ACTOR_ACTIVITY_DAILY_TABLE}
        FORMAT JSONEachRow
        """,
        body=build_json_each_row_payload(affected_mart_df),
    )


def main() -> None:
    """Refresh incremental actor activity daily từ Gold modeled."""
    args = parse_args()
    last_successful_run_at = (
        None
        if args.ignore_state
        else read_last_successful_run_at(DEFAULT_STATE_PATH)
    )
    refresh_to = utc_now()
    refresh_window = build_refresh_window(last_successful_run_at, refresh_to)

    print("gold_actor_activity_daily_incremental_refresh")
    print(f"state_path: {DEFAULT_STATE_PATH}")
    print(f"last_successful_run_at: {last_successful_run_at}")
    print(f"refresh_from: {refresh_window.refresh_from.isoformat()}")
    print(f"refresh_to: {refresh_window.refresh_to.isoformat()}")
    print(f"lookback_hours: {refresh_window.lookback_hours}")

    spark = create_iceberg_spark_session(
        "bluesky-refresh-gold-actor-activity-daily"
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
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
        incremental_content_fact_df = filter_by_received_at(
            gold_fact_content_events_df,
            refresh_window,
        ).cache()
        incremental_engagement_fact_df = filter_by_received_at(
            gold_fact_engagement_events_df,
            refresh_window,
        ).cache()
        incremental_network_fact_df = filter_by_received_at(
            gold_fact_network_events_df,
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
        print(
            "gold_fact_network_events_incremental_rows: "
            f"{incremental_network_fact_df.count()}"
        )

        affected_keys_df = build_affected_actor_activity_keys(
            incremental_content_fact_df,
            incremental_engagement_fact_df,
            incremental_network_fact_df,
            gold_fact_engagement_events_df,
            gold_dim_posts_df,
        ).cache()
        affected_key_count = affected_keys_df.count()
        print(f"affected_actor_activity_key_count: {affected_key_count}")
        affected_keys_df.orderBy("activity_date", "actor_did").show(
            24,
            truncate=False,
        )

        if affected_key_count == 0:
            write_last_successful_run_at(DEFAULT_STATE_PATH, refresh_to)
            print("affected_rows_replaced: 0")
            print("state_updated: true")
            return

        keys = collect_actor_activity_keys(affected_keys_df)
        predicates = build_predicates(keys)
        full_mart_df = build_gold_actor_activity_daily(
            gold_dim_actors_df,
            gold_dim_posts_df,
            gold_fact_content_events_df,
            gold_fact_engagement_events_df,
            gold_fact_network_events_df,
        )
        affected_mart_df = full_mart_df.join(
            affected_keys_df,
            ["activity_date", "actor_did"],
            "inner",
        ).cache()

        try:
            affected_rows = affected_mart_df.count()
            print(f"affected_rows_recomputed: {affected_rows}")
            affected_mart_df.orderBy(
                col("activity_score").desc(),
                col("activity_date").desc(),
                col("actor_did"),
            ).show(24, truncate=False)

            replace_clickhouse_rows(affected_mart_df, predicates)
            print(f"affected_rows_replaced: {affected_rows}")

            expected_metrics = build_expected_metrics(affected_mart_df, COUNT_METRICS)
            actual_metrics = read_clickhouse_metrics_for_predicates(
                GOLD_ACTOR_ACTIVITY_DAILY_TABLE,
                COUNT_METRICS,
                predicates,
            )
            print_reconciliation(expected_metrics, actual_metrics)
            print("Gold actor activity daily incremental check passed")

            write_last_successful_run_at(DEFAULT_STATE_PATH, refresh_to)
            print("state_updated: true")
        finally:
            affected_mart_df.unpersist()

    finally:
        for table_df in [
            locals().get("incremental_content_fact_df"),
            locals().get("incremental_engagement_fact_df"),
            locals().get("incremental_network_fact_df"),
            locals().get("affected_keys_df"),
        ]:
            if table_df is not None:
                table_df.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
