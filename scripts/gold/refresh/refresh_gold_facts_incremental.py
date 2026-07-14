"""Refresh incremental các Gold fact tables từ Silver Iceberg."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    count as spark_count,
    countDistinct,
    lit,
    to_timestamp,
)

from bluesky_pipeline.transforms.gold_transformations import (
    build_gold_fact_content_events,
    build_gold_fact_engagement_events,
    build_gold_fact_network_events,
)
from bluesky_pipeline.config.iceberg import (
    ICEBERG_CATALOG_NAME,
    ICEBERG_GOLD_NAMESPACE,
    ICEBERG_GOLD_TABLES,
    ICEBERG_SILVER_TABLES,
    create_iceberg_spark_session,
)
from bluesky_pipeline.state.incremental_refresh import (
    RefreshWindow,
    build_refresh_window,
    read_last_successful_run_at,
    utc_now,
    write_last_successful_run_at,
)
from scripts.gold.check.check_gold_facts_incremental import check_gold_fact_keys


DEFAULT_STATE_PATH = Path("data/state/gold_facts_incremental_refresh.json")
SILVER_FACT_SOURCE_TABLES = [
    "silver_posts",
    "silver_engagements",
    "silver_follows",
    "silver_deleted_records",
]
GOLD_FACT_SPECS = [
    (
        "gold_fact_content_events",
        build_gold_fact_content_events,
        "content_event_id",
    ),
    (
        "gold_fact_engagement_events",
        build_gold_fact_engagement_events,
        "engagement_event_id",
    ),
    (
        "gold_fact_network_events",
        build_gold_fact_network_events,
        "network_event_id",
    ),
]


def _format_timestamp_for_spark(value: datetime) -> str:
    """Format datetime UTC thành chuỗi ISO có timezone để Spark parse ổn định.

    Input là datetime timezone-aware.
    Output là chuỗi ISO-8601 kết thúc bằng `Z`, cùng semantics với `received_at`.
    """
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def filter_by_received_at(
    table_df: DataFrame,
    refresh_window: RefreshWindow,
) -> DataFrame:
    """Lọc Silver rows theo refresh window dựa trên `received_at`.

    Input là Silver DataFrame và refresh window.
    Output là DataFrame chỉ gồm rows nằm trong phạm vi cần xử lý.
    """
    received_at_ts = to_timestamp(col("received_at"))
    refresh_to_ts = to_timestamp(
        lit(_format_timestamp_for_spark(refresh_window.refresh_to))
    )
    upper_bounded_df = table_df.filter(received_at_ts < refresh_to_ts)

    # Initial refresh dùng datetime.min làm marker logic. Spark không parse ổn
    # định năm 0001, nên không đưa marker này vào filter timestamp vật lý.
    if refresh_window.refresh_from == datetime.min.replace(tzinfo=timezone.utc):
        return upper_bounded_df

    refresh_from_ts = to_timestamp(
        lit(_format_timestamp_for_spark(refresh_window.refresh_from))
    )

    return upper_bounded_df.filter(received_at_ts >= refresh_from_ts)


def parse_args() -> argparse.Namespace:
    """Đọc CLI flags cho Gold fact incremental refresh.

    Output là các option điều khiển cách tính refresh window.
    """
    parser = argparse.ArgumentParser(
        description="Refresh Gold fact tables incrementally from Silver Iceberg."
    )
    parser.add_argument(
        "--ignore-state",
        action="store_true",
        help="Bỏ qua local state và chạy như initial refresh.",
    )
    return parser.parse_args()


def print_gold_fact_key_metrics(
    table_name: str,
    table_df: DataFrame,
    key_column: str,
) -> None:
    """In row count và distinct key count cho một Gold fact DataFrame.

    Input là tên fact table, DataFrame đã build và tên cột key.
    Output là các metric giúp kiểm tra key uniqueness trước khi ghi Iceberg.
    """
    metrics = table_df.agg(
        spark_count("*").alias("row_count"),
        spark_count(key_column).alias("non_null_key_count"),
        countDistinct(key_column).alias("distinct_key_count"),
    ).collect()[0]
    row_count = int(metrics["row_count"] or 0)
    non_null_key_count = int(metrics["non_null_key_count"] or 0)
    distinct_key_count = int(metrics["distinct_key_count"] or 0)
    status = (
        "OK"
        if row_count == non_null_key_count == distinct_key_count
        else "MISMATCH"
    )

    print(f"{table_name}_incremental_rows: {row_count}")
    print(f"{table_name}_incremental_non_null_keys: {non_null_key_count}")
    print(f"{table_name}_incremental_distinct_keys: {distinct_key_count}")
    print(f"{table_name}_incremental_key_status: {status}")

    if status != "OK":
        raise RuntimeError(f"{table_name} incremental key check failed")


def ensure_gold_namespace(spark) -> None:
    """Tạo namespace Gold Iceberg nếu chưa tồn tại.

    Input chính là SparkSession có cấu hình Iceberg catalog.
    Output là namespace `gold_v1` sẵn sàng để tạo bảng Gold fact.
    """
    spark.sql(
        f"CREATE NAMESPACE IF NOT EXISTS "
        f"{ICEBERG_CATALOG_NAME}.{ICEBERG_GOLD_NAMESPACE}"
    )


def iceberg_table_exists(spark, iceberg_table: str) -> bool:
    """Kiểm tra một bảng Iceberg đã tồn tại trong catalog hay chưa.

    Input là SparkSession và tên bảng Iceberg đầy đủ.
    Output là True nếu bảng đọc được qua catalog.
    """
    try:
        spark.table(iceberg_table).limit(0).count()
        return True
    except AnalysisException:
        return False


def ensure_gold_fact_table(
    spark,
    table_name: str,
    source_df: DataFrame,
) -> str:
    """Tạo Gold fact table rỗng nếu bảng chưa tồn tại.

    Input là tên logical table và DataFrame nguồn có schema cần ghi.
    Output là tên bảng Iceberg đầy đủ trong catalog.
    """
    iceberg_table = ICEBERG_GOLD_TABLES[table_name]

    if not iceberg_table_exists(spark, iceberg_table):
        source_df.limit(0).writeTo(iceberg_table).using("iceberg").create()
        print(f"created_gold_fact_table: {iceberg_table}")

    return iceberg_table


def append_new_fact_rows(
    spark,
    table_name: str,
    candidate_df: DataFrame,
    key_column: str,
) -> int:
    """Append các Gold fact rows chưa tồn tại theo event id.

    Input là candidate fact DataFrame trong refresh window và key column.
    Output là số rows mới đã append vào bảng Gold fact Iceberg.
    """
    print_gold_fact_key_metrics(table_name, candidate_df, key_column)

    iceberg_table = ensure_gold_fact_table(spark, table_name, candidate_df)
    existing_df = spark.table(iceberg_table)
    existing_keys_df = existing_df.select(key_column).dropDuplicates([key_column])

    overlap_rows = candidate_df.join(existing_keys_df, key_column, "inner").count()
    new_rows_df = candidate_df.join(existing_keys_df, key_column, "left_anti").cache()
    new_rows = new_rows_df.count()

    print(f"{table_name}_existing_overlap_rows: {overlap_rows}")
    print(f"{table_name}_new_rows_to_append: {new_rows}")

    try:
        if new_rows > 0:
            new_rows_df.writeTo(iceberg_table).append()
            print(f"{table_name}_appended_rows: {new_rows}")
        else:
            print(f"{table_name}_appended_rows: 0")

    finally:
        new_rows_df.unpersist()

    if new_rows == 0:
        # Bảng không đổi, nên bỏ qua full-table key check để giảm tải local.
        print(f"{table_name}_iceberg_key_check_skipped: no_new_rows")
        return new_rows

    final_df = spark.table(iceberg_table)
    print_gold_fact_key_metrics(
        f"{table_name}_iceberg",
        final_df,
        key_column,
    )
    return new_rows


def main() -> None:
    """Refresh incremental Gold fact rows nằm trong refresh window.

    Input chính là local state file và các bảng Silver Iceberg.
    Output là row count Silver, key metrics, số rows overlap và số rows append
    của từng Gold fact table.
    """
    args = parse_args()
    last_successful_run_at = (
        None
        if args.ignore_state
        else read_last_successful_run_at(DEFAULT_STATE_PATH)
    )
    refresh_to = utc_now()
    refresh_window = build_refresh_window(
        last_successful_run_at=last_successful_run_at,
        refresh_to=refresh_to,
    )

    print("gold_facts_incremental_refresh")
    print(f"state_path: {DEFAULT_STATE_PATH}")
    print(f"last_successful_run_at: {last_successful_run_at}")
    print(f"refresh_from: {refresh_window.refresh_from.isoformat()}")
    print(f"refresh_to: {refresh_window.refresh_to.isoformat()}")
    print(f"lookback_hours: {refresh_window.lookback_hours}")

    spark = create_iceberg_spark_session("bluesky-refresh-gold-facts-incremental")
    spark.sparkContext.setLogLevel("WARN")

    try:
        ensure_gold_namespace(spark)
        incremental_silver_tables = {}
        incremental_silver_row_counts = {}

        for table_name in SILVER_FACT_SOURCE_TABLES:
            table_df = spark.table(ICEBERG_SILVER_TABLES[table_name])
            incremental_df = filter_by_received_at(table_df, refresh_window)
            incremental_silver_tables[table_name] = incremental_df.cache()
            incremental_silver_row_counts[table_name] = incremental_silver_tables[
                table_name
            ].count()
            print(
                f"{table_name}_incremental_rows: "
                f"{incremental_silver_row_counts[table_name]}"
            )

        if sum(incremental_silver_row_counts.values()) == 0:
            print("gold_facts_new_rows_to_append_total: 0")
            print("gold_facts_has_changes: false")
            print("gold_fact_key_checks_skipped: empty_refresh_window")
            write_last_successful_run_at(DEFAULT_STATE_PATH, refresh_to)
            print("state_updated: true")
            return

        total_new_rows_to_append = 0

        for table_name, build_fact_df, key_column in GOLD_FACT_SPECS:
            fact_df = build_fact_df(incremental_silver_tables).cache()

            try:
                total_new_rows_to_append += append_new_fact_rows(
                    spark,
                    table_name,
                    fact_df,
                    key_column,
                )
            finally:
                fact_df.unpersist()

        print(f"gold_facts_new_rows_to_append_total: {total_new_rows_to_append}")
        print(
            "gold_facts_has_changes: "
            f"{str(total_new_rows_to_append > 0).lower()}"
        )

        if total_new_rows_to_append > 0:
            check_gold_fact_keys(spark)
        else:
            print("gold_fact_key_checks_skipped: no_new_rows")

        write_last_successful_run_at(DEFAULT_STATE_PATH, refresh_to)
        print("state_updated: true")

    finally:
        for table_df in locals().get("incremental_silver_tables", {}).values():
            table_df.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
