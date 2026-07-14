"""Refresh incremental Gold content quality hourly mart vào ClickHouse."""

from __future__ import annotations

import argparse
import time
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, date_trunc, lit, sum as spark_sum, to_timestamp

from bluesky_pipeline.clients.clickhouse import execute_clickhouse
from bluesky_pipeline.transforms.gold_analytics_transformations import (
    build_gold_content_quality_hourly,
)
from bluesky_pipeline.schemas.gold_tables import (
    GOLD_CONTENT_QUALITY_HOURLY_TABLE,
    build_gold_content_quality_hourly_ddl,
)
from bluesky_pipeline.config.iceberg import (
    ICEBERG_GOLD_TABLES,
    create_iceberg_spark_session,
)
from bluesky_pipeline.state.incremental_refresh import (
    RefreshWindow,
    build_refresh_window,
    read_last_successful_run_at,
    utc_now,
    write_last_successful_run_at,
)
from scripts.gold.load.load_content_quality_hourly_to_clickhouse import (
    build_json_each_row_payload,
)


DEFAULT_STATE_PATH = Path(
    "data/state/gold_content_quality_hourly_incremental_refresh.json"
)
COUNT_METRICS = [
    "original_post_create_count",
    "reply_create_count",
    "post_update_count",
    "reply_update_count",
    "post_delete_count",
    "reply_delete_count",
    "total_content_events",
]


def _format_timestamp_for_spark(value: datetime) -> str:
    """Format datetime UTC thành chuỗi ISO có timezone để Spark parse ổn định.

    Input là datetime timezone-aware.
    Output là chuỗi ISO-8601 kết thúc bằng `Z`, cùng semantics với `received_at`.
    """
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _format_datetime_for_clickhouse(value: datetime) -> str:
    """Format Python datetime thành literal DateTime cho ClickHouse.

    Input là datetime từ Spark collect.
    Output là chuỗi `yyyy-MM-dd HH:mm:ss` dùng trong `toDateTime`.
    """
    return value.strftime("%Y-%m-%d %H:%M:%S")


def parse_args() -> argparse.Namespace:
    """Đọc CLI flags cho content quality hourly incremental refresh."""
    parser = argparse.ArgumentParser(
        description="Refresh Gold content quality hourly incrementally."
    )
    parser.add_argument(
        "--ignore-state",
        action="store_true",
        help="Bỏ qua local state và chạy như initial refresh.",
    )
    return parser.parse_args()


def filter_by_received_at(
    table_df: DataFrame,
    refresh_window: RefreshWindow,
) -> DataFrame:
    """Lọc Gold fact rows theo refresh window dựa trên `received_at`.

    Input là Gold fact DataFrame và refresh window.
    Output là DataFrame chỉ gồm fact rows được load/observe trong window.
    """
    received_at_ts = to_timestamp(col("received_at"))
    refresh_to_ts = to_timestamp(
        lit(_format_timestamp_for_spark(refresh_window.refresh_to))
    )
    upper_bounded_df = table_df.filter(received_at_ts < refresh_to_ts)

    # Initial refresh không đưa datetime.min vào Spark filter để tránh timestamp
    # năm 0001 bị parse không nhất quán giữa các engine.
    if refresh_window.refresh_from == datetime.min.replace(tzinfo=timezone.utc):
        return upper_bounded_df

    refresh_from_ts = to_timestamp(
        lit(_format_timestamp_for_spark(refresh_window.refresh_from))
    )

    return upper_bounded_df.filter(received_at_ts >= refresh_from_ts)


def read_gold_modeled_table(spark: SparkSession, table_name: str) -> DataFrame:
    """Đọc một Gold modeled Iceberg table theo table contract.

    Input chính là SparkSession và tên logical table trong contract.
    Output là DataFrame Iceberg tương ứng.
    """
    return spark.table(ICEBERG_GOLD_TABLES[table_name])


def build_affected_windows(incremental_content_fact_df: DataFrame) -> DataFrame:
    """Xác định các hourly windows bị ảnh hưởng bởi fact rows mới.

    Input là content fact rows nằm trong refresh window theo `received_at`.
    Output là DataFrame một cột `window_start` theo event time hour.
    """
    return (
        incremental_content_fact_df.withColumn(
            "window_start",
            date_trunc("hour", col("event_time")),
        )
        .select("window_start")
        .filter(col("window_start").isNotNull())
        .dropDuplicates(["window_start"])
    )


def collect_window_values(affected_windows_df: DataFrame) -> list[datetime]:
    """Collect affected windows về driver để build ClickHouse predicate.

    Input là DataFrame một cột `window_start`.
    Output là list datetime đã sort tăng dần.
    """
    return [
        row["window_start"]
        for row in affected_windows_df.orderBy("window_start").collect()
    ]


def build_incremental_mart_rows(
    gold_fact_content_events_df: DataFrame,
    gold_dim_posts_df: DataFrame,
    affected_windows_df: DataFrame,
) -> DataFrame:
    """Recompute content quality hourly cho các affected windows.

    Input là full Gold modeled source of truth và danh sách affected windows.
    Output là mart rows cần replace trong ClickHouse.
    """
    full_mart_df = build_gold_content_quality_hourly(
        gold_fact_content_events_df,
        gold_dim_posts_df,
    )

    return full_mart_df.join(affected_windows_df, "window_start", "inner")


def split_clickhouse_table(table_name: str) -> tuple[str, str]:
    """Tách tên bảng ClickHouse dạng `database.table`.

    Input là tên bảng đầy đủ.
    Output là tuple `(database, table)`.
    """
    database_name, raw_table_name = table_name.split(".", maxsplit=1)
    return database_name, raw_table_name


def build_window_predicate(window_values: list[datetime]) -> str:
    """Build predicate ClickHouse cho danh sách affected windows.

    Input là list datetime affected.
    Output là biểu thức SQL dùng trong WHERE.
    """
    literals = ", ".join(
        f"toDateTime('{_format_datetime_for_clickhouse(window_value)}')"
        for window_value in window_values
    )
    return f"window_start IN ({literals})"


def wait_for_clickhouse_mutations(
    table_name: str,
    timeout_seconds: int = 60,
    poll_seconds: float = 1.0,
) -> None:
    """Chờ ClickHouse mutation của bảng hoàn tất.

    Input là tên bảng ClickHouse và timeout.
    Output là exception nếu mutation không hoàn tất trong timeout.
    """
    database_name, raw_table_name = split_clickhouse_table(table_name)
    deadline = time.monotonic() + timeout_seconds

    while time.monotonic() < deadline:
        result = execute_clickhouse(
            f"""
            SELECT count()
            FROM system.mutations
            WHERE database = '{database_name}'
              AND table = '{raw_table_name}'
              AND is_done = 0
            """
        )
        pending_mutations = int(result.strip() or "0")

        if pending_mutations == 0:
            return

        time.sleep(poll_seconds)

    raise TimeoutError(
        f"ClickHouse mutations did not finish for {table_name} "
        f"within {timeout_seconds} seconds"
    )


def replace_clickhouse_windows(
    affected_mart_df: DataFrame,
    window_values: list[datetime],
) -> None:
    """Replace affected hourly rows trong ClickHouse.

    Input là affected mart DataFrame và affected window values.
    Output là ClickHouse table được delete theo window rồi insert version mới.
    """
    execute_clickhouse(build_gold_content_quality_hourly_ddl())
    window_predicate = build_window_predicate(window_values)

    # MergeTree không có transaction delete+insert trong local setup, nên mutation
    # phải hoàn tất trước khi insert lại để tránh dashboard đọc trùng dữ liệu.
    execute_clickhouse(
        f"""
        ALTER TABLE {GOLD_CONTENT_QUALITY_HOURLY_TABLE}
        DELETE WHERE {window_predicate}
        """
    )
    wait_for_clickhouse_mutations(GOLD_CONTENT_QUALITY_HOURLY_TABLE)

    json_payload = build_json_each_row_payload(affected_mart_df)
    execute_clickhouse(
        f"""
        INSERT INTO {GOLD_CONTENT_QUALITY_HOURLY_TABLE}
        FORMAT JSONEachRow
        """,
        body=json_payload,
    )


def build_expected_metrics(affected_mart_df: DataFrame) -> dict[str, int]:
    """Tính metrics kỳ vọng từ affected mart DataFrame.

    Input là affected mart rows vừa recompute từ Gold modeled.
    Output là dict chứa row count và tổng các count metrics.
    """
    metrics_row = affected_mart_df.agg(
        *[spark_sum(metric).alias(metric) for metric in COUNT_METRICS]
    ).collect()[0]
    metrics = {"row_count": affected_mart_df.count()}

    for metric in COUNT_METRICS:
        metrics[metric] = int(metrics_row[metric] or 0)

    return metrics


def read_clickhouse_metrics(window_values: list[datetime]) -> dict[str, int]:
    """Đọc metrics thực tế từ ClickHouse cho affected windows.

    Input là list affected window values.
    Output là dict chứa row count và tổng các count metrics trong ClickHouse.
    """
    metric_selects = ",\n".join(
        f"sum({metric}) AS {metric}" for metric in COUNT_METRICS
    )
    result = execute_clickhouse(
        f"""
        SELECT
            count() AS row_count,
            {metric_selects}
        FROM {GOLD_CONTENT_QUALITY_HOURLY_TABLE}
        WHERE {build_window_predicate(window_values)}
        """
    )

    values = result.strip().split("\t")
    metric_names = ["row_count", *COUNT_METRICS]

    return {
        metric_name: int(metric_value if metric_value != "\\N" else 0)
        for metric_name, metric_value in zip(metric_names, values, strict=True)
    }


def print_reconciliation(expected: dict[str, int], actual: dict[str, int]) -> None:
    """In reconciliation giữa expected rows và ClickHouse affected windows."""
    has_mismatch = False

    print("metric\texpected\tactual\tstatus")

    for metric in sorted(set(expected) | set(actual)):
        expected_value = expected.get(metric, 0)
        actual_value = actual.get(metric, 0)
        status = "OK" if expected_value == actual_value else "MISMATCH"

        if status != "OK":
            has_mismatch = True

        print(f"{metric}\t{expected_value}\t{actual_value}\t{status}")

    if has_mismatch:
        raise SystemExit("Gold content quality hourly incremental check failed")

    print("Gold content quality hourly incremental check passed")


def main() -> None:
    """Refresh incremental content quality hourly mart từ Gold modeled.

    Input chính là local state file, Gold modeled Iceberg và ClickHouse.
    Output là affected windows, số rows replace và reconciliation result.
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

    print("gold_content_quality_hourly_incremental_refresh")
    print(f"state_path: {DEFAULT_STATE_PATH}")
    print(f"last_successful_run_at: {last_successful_run_at}")
    print(f"refresh_from: {refresh_window.refresh_from.isoformat()}")
    print(f"refresh_to: {refresh_window.refresh_to.isoformat()}")
    print(f"lookback_hours: {refresh_window.lookback_hours}")

    spark = create_iceberg_spark_session(
        "bluesky-refresh-gold-content-quality-hourly-incremental"
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        gold_fact_content_events_df = read_gold_modeled_table(
            spark,
            "gold_fact_content_events",
        )
        gold_dim_posts_df = read_gold_modeled_table(spark, "gold_dim_posts")
        incremental_content_fact_df = filter_by_received_at(
            gold_fact_content_events_df,
            refresh_window,
        ).cache()

        incremental_fact_rows = incremental_content_fact_df.count()
        print(f"gold_fact_content_events_incremental_rows: {incremental_fact_rows}")

        affected_windows_df = build_affected_windows(incremental_content_fact_df).cache()
        affected_window_count = affected_windows_df.count()
        print(f"affected_window_count: {affected_window_count}")
        affected_windows_df.orderBy("window_start").show(24, truncate=False)

        if affected_window_count == 0:
            write_last_successful_run_at(DEFAULT_STATE_PATH, refresh_to)
            print("affected_rows_replaced: 0")
            print("state_updated: true")
            return

        window_values = collect_window_values(affected_windows_df)
        affected_mart_df = build_incremental_mart_rows(
            gold_fact_content_events_df,
            gold_dim_posts_df,
            affected_windows_df,
        ).cache()

        try:
            affected_rows = affected_mart_df.count()
            print(f"affected_rows_recomputed: {affected_rows}")
            affected_mart_df.orderBy(col("window_start").desc()).show(
                24,
                truncate=False,
            )

            replace_clickhouse_windows(affected_mart_df, window_values)
            print(f"affected_rows_replaced: {affected_rows}")

            expected_metrics = build_expected_metrics(affected_mart_df)
            actual_metrics = read_clickhouse_metrics(window_values)
            print_reconciliation(expected_metrics, actual_metrics)

            write_last_successful_run_at(DEFAULT_STATE_PATH, refresh_to)
            print("state_updated: true")

        finally:
            affected_mart_df.unpersist()

    finally:
        for table_df in [
            locals().get("incremental_content_fact_df"),
            locals().get("affected_windows_df"),
        ]:
            if table_df is not None:
                table_df.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
