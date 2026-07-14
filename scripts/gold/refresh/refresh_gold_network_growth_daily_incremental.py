"""Refresh incremental Gold network growth daily mart vào ClickHouse."""

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql.functions import coalesce, col, lit, to_date

from bluesky_pipeline.clients.clickhouse import execute_clickhouse
from bluesky_pipeline.transforms.gold_analytics_transformations import build_gold_network_growth_daily
from bluesky_pipeline.schemas.gold_tables import (
    GOLD_NETWORK_GROWTH_DAILY_TABLE,
    build_gold_network_growth_daily_ddl,
)
from bluesky_pipeline.config.iceberg import create_iceberg_spark_session
from bluesky_pipeline.state.incremental_refresh import (
    build_refresh_window,
    read_last_successful_run_at,
    utc_now,
    write_last_successful_run_at,
)
from scripts.gold.common.incremental_serving_utils import (
    build_expected_metrics,
    build_tuple_key_predicate,
    chunked,
    delete_clickhouse_by_predicates,
    filter_by_received_at,
    print_reconciliation,
    read_clickhouse_metrics_for_predicates,
    read_gold_modeled_table,
)
from scripts.gold.load.load_network_growth_daily_to_clickhouse import (
    build_json_each_row_payload,
)


DEFAULT_STATE_PATH = Path(
    "data/state/gold_network_growth_daily_incremental_refresh.json"
)
COUNT_METRICS = [
    "follow_count",
    "unfollow_count",
    "net_follow_count",
    "unique_follower_count",
]
NULL_TARGET_ACTOR_KEY = ""


def parse_args() -> argparse.Namespace:
    """Đọc CLI flags cho network growth daily incremental refresh."""
    parser = argparse.ArgumentParser(
        description="Refresh Gold network growth daily incrementally."
    )
    parser.add_argument(
        "--ignore-state",
        action="store_true",
        help="Bỏ qua local state và chạy như initial refresh.",
    )
    return parser.parse_args()


def add_target_actor_key(table_df: DataFrame) -> DataFrame:
    """Thêm key phụ để join/predicate ổn định với target_actor_did NULL.

    Input là DataFrame có `target_actor_did`.
    Output là DataFrame có thêm `_target_actor_key`.
    """
    return table_df.withColumn(
        "_target_actor_key",
        coalesce(col("target_actor_did"), lit(NULL_TARGET_ACTOR_KEY)),
    )


def build_affected_network_keys(incremental_network_fact_df: DataFrame) -> DataFrame:
    """Xác định affected `activity_date + target_actor_did`.

    Input là incremental network fact theo `received_at`.
    Output là DataFrame key đã distinct, có `_target_actor_key` để xử lý NULL.
    """
    return (
        add_target_actor_key(
            incremental_network_fact_df.select(
                to_date(col("event_time")).alias("activity_date"),
                col("target_actor_did"),
            )
        )
        .filter(col("activity_date").isNotNull())
        .dropDuplicates(["activity_date", "_target_actor_key"])
    )


def collect_network_keys(affected_keys_df: DataFrame) -> list[tuple[object, str]]:
    """Collect affected network daily keys về driver."""
    return [
        (row["activity_date"], row["_target_actor_key"])
        for row in affected_keys_df.orderBy(
            "activity_date",
            "_target_actor_key",
        ).collect()
    ]


def build_predicates(keys: list[tuple[object, str]]) -> list[str]:
    """Build ClickHouse predicates theo chunk `activity_date + target_actor_did`."""
    return [
        build_tuple_key_predicate(
            ("activity_date", "ifNull(target_actor_did, '')"),
            key_chunk,
        )
        for key_chunk in chunked(keys)
    ]


def replace_clickhouse_rows(affected_mart_df: DataFrame, predicates: list[str]) -> None:
    """Replace affected network growth daily rows trong ClickHouse."""
    execute_clickhouse(build_gold_network_growth_daily_ddl())
    delete_clickhouse_by_predicates(GOLD_NETWORK_GROWTH_DAILY_TABLE, predicates)

    if affected_mart_df.count() == 0:
        return

    execute_clickhouse(
        f"""
        INSERT INTO {GOLD_NETWORK_GROWTH_DAILY_TABLE}
        FORMAT JSONEachRow
        """,
        body=build_json_each_row_payload(affected_mart_df),
    )


def main() -> None:
    """Refresh incremental network growth daily từ Gold modeled."""
    args = parse_args()
    last_successful_run_at = (
        None
        if args.ignore_state
        else read_last_successful_run_at(DEFAULT_STATE_PATH)
    )
    refresh_to = utc_now()
    refresh_window = build_refresh_window(last_successful_run_at, refresh_to)

    print("gold_network_growth_daily_incremental_refresh")
    print(f"state_path: {DEFAULT_STATE_PATH}")
    print(f"last_successful_run_at: {last_successful_run_at}")
    print(f"refresh_from: {refresh_window.refresh_from.isoformat()}")
    print(f"refresh_to: {refresh_window.refresh_to.isoformat()}")
    print(f"lookback_hours: {refresh_window.lookback_hours}")

    spark = create_iceberg_spark_session(
        "bluesky-refresh-gold-network-growth-daily"
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        gold_fact_network_events_df = read_gold_modeled_table(
            spark,
            "gold_fact_network_events",
        )
        incremental_network_fact_df = filter_by_received_at(
            gold_fact_network_events_df,
            refresh_window,
        ).cache()

        print(
            "gold_fact_network_events_incremental_rows: "
            f"{incremental_network_fact_df.count()}"
        )

        affected_keys_df = build_affected_network_keys(
            incremental_network_fact_df
        ).cache()
        affected_key_count = affected_keys_df.count()
        print(f"affected_network_growth_key_count: {affected_key_count}")
        affected_keys_df.orderBy("activity_date", "_target_actor_key").show(
            24,
            truncate=False,
        )

        if affected_key_count == 0:
            write_last_successful_run_at(DEFAULT_STATE_PATH, refresh_to)
            print("affected_rows_replaced: 0")
            print("state_updated: true")
            return

        keys = collect_network_keys(affected_keys_df)
        predicates = build_predicates(keys)
        full_mart_df = add_target_actor_key(
            build_gold_network_growth_daily(gold_fact_network_events_df)
        )
        affected_mart_df = (
            full_mart_df.join(
                affected_keys_df.select("activity_date", "_target_actor_key"),
                ["activity_date", "_target_actor_key"],
                "inner",
            )
            .drop("_target_actor_key")
            .cache()
        )

        try:
            affected_rows = affected_mart_df.count()
            print(f"affected_rows_recomputed: {affected_rows}")
            affected_mart_df.orderBy(
                col("net_follow_count").desc(),
                col("follow_count").desc(),
                col("activity_date").desc(),
                col("target_actor_did").asc_nulls_last(),
            ).show(24, truncate=False)

            replace_clickhouse_rows(affected_mart_df, predicates)
            print(f"affected_rows_replaced: {affected_rows}")

            expected_metrics = build_expected_metrics(affected_mart_df, COUNT_METRICS)
            actual_metrics = read_clickhouse_metrics_for_predicates(
                GOLD_NETWORK_GROWTH_DAILY_TABLE,
                COUNT_METRICS,
                predicates,
            )
            print_reconciliation(expected_metrics, actual_metrics)
            print("Gold network growth daily incremental check passed")

            write_last_successful_run_at(DEFAULT_STATE_PATH, refresh_to)
            print("state_updated: true")
        finally:
            affected_mart_df.unpersist()

    finally:
        for table_df in [
            locals().get("incremental_network_fact_df"),
            locals().get("affected_keys_df"),
        ]:
            if table_df is not None:
                table_df.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
