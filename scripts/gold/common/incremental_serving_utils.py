"""Helper dùng chung cho Gold serving mart incremental refresh."""

from __future__ import annotations

import time
from collections.abc import Iterable, Sequence
from datetime import date, datetime, timezone
from typing import Any
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, sum as spark_sum, to_timestamp

from bluesky_pipeline.clients.clickhouse import (
    CLICKHOUSE_URL,
    build_auth_header,
    execute_clickhouse,
)
from bluesky_pipeline.config.iceberg import ICEBERG_GOLD_TABLES
from bluesky_pipeline.state.incremental_refresh import RefreshWindow


def format_timestamp_for_spark(value: datetime) -> str:
    """Format datetime UTC thành chuỗi ISO có timezone để Spark parse ổn định.

    Input là datetime timezone-aware.
    Output là chuỗi ISO-8601 kết thúc bằng `Z`, cùng semantics với `received_at`.
    """
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def filter_by_received_at(
    table_df: DataFrame,
    refresh_window: RefreshWindow,
) -> DataFrame:
    """Lọc Gold fact rows theo refresh window dựa trên `received_at`.

    Input là Gold fact DataFrame và refresh window.
    Output là DataFrame chỉ gồm rows được observe/load trong window.
    """
    received_at_ts = to_timestamp(col("received_at"))
    refresh_to_ts = to_timestamp(
        lit(format_timestamp_for_spark(refresh_window.refresh_to))
    )
    upper_bounded_df = table_df.filter(received_at_ts < refresh_to_ts)

    # Initial refresh dùng datetime.min làm marker control-plane. Không đưa marker
    # này vào Spark timestamp filter vì năm 0001 có thể parse không ổn định.
    if refresh_window.refresh_from == datetime.min.replace(tzinfo=timezone.utc):
        return upper_bounded_df

    refresh_from_ts = to_timestamp(
        lit(format_timestamp_for_spark(refresh_window.refresh_from))
    )
    return upper_bounded_df.filter(received_at_ts >= refresh_from_ts)


def read_gold_modeled_table(spark: SparkSession, table_name: str) -> DataFrame:
    """Đọc một Gold modeled Iceberg table theo table contract.

    Input chính là SparkSession và logical table name.
    Output là DataFrame Iceberg tương ứng.
    """
    return spark.table(ICEBERG_GOLD_TABLES[table_name])


def split_clickhouse_table(table_name: str) -> tuple[str, str]:
    """Tách tên bảng ClickHouse dạng `database.table`.

    Input là tên bảng đầy đủ.
    Output là tuple `(database, table)`.
    """
    database_name, raw_table_name = table_name.split(".", maxsplit=1)
    return database_name, raw_table_name


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


def execute_clickhouse_sql(query: str) -> str:
    """Gửi SQL tới ClickHouse bằng HTTP body thay vì query string.

    Input là câu SQL có thể dài.
    Output là response text từ ClickHouse.
    """
    request = Request(
        CLICKHOUSE_URL,
        data=query.encode("utf-8"),
        method="POST",
    )
    request.add_header("Authorization", build_auth_header())

    try:
        with urlopen(request) as response:
            return response.read().decode("utf-8")
    except HTTPError as error:
        error_body = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            "ClickHouse SQL failed\n"
            f"status_code: {error.code}\n"
            f"reason: {error.reason}\n"
            f"query:\n{query.strip()}\n"
            f"response:\n{error_body.strip()}"
        ) from error


def chunked(values: Sequence[Any], chunk_size: int = 500) -> Iterable[Sequence[Any]]:
    """Chia list giá trị thành các chunk nhỏ để tránh SQL quá dài.

    Input là sequence giá trị và kích thước chunk.
    Output là iterator các sequence con.
    """
    for offset in range(0, len(values), chunk_size):
        yield values[offset : offset + chunk_size]


def clickhouse_literal(value: Any) -> str:
    """Chuyển Python value thành ClickHouse SQL literal an toàn cho local script.

    Input là giá trị scalar.
    Output là literal dùng trong predicate SQL.
    """
    if value is None:
        return "NULL"

    if isinstance(value, datetime):
        return f"toDateTime('{value.strftime('%Y-%m-%d %H:%M:%S')}')"

    if isinstance(value, date):
        return f"toDate('{value.isoformat()}')"

    if isinstance(value, bool):
        return "1" if value else "0"

    if isinstance(value, int | float):
        return str(value)

    escaped_value = str(value).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped_value}'"


def build_single_key_predicate(column_name: str, values: Sequence[Any]) -> str:
    """Build predicate ClickHouse dạng `column IN (...)`.

    Input là tên cột và danh sách giá trị non-null.
    Output là biểu thức SQL dùng trong WHERE.
    """
    literals = ", ".join(clickhouse_literal(value) for value in values)
    return f"{column_name} IN ({literals})"


def build_tuple_key_predicate(
    column_names: Sequence[str],
    values: Sequence[Sequence[Any]],
) -> str:
    """Build predicate ClickHouse dạng tuple IN.

    Input là danh sách cột và danh sách tuple giá trị.
    Output là biểu thức SQL dùng trong WHERE.
    """
    column_expr = ", ".join(column_names)
    tuple_literals = ", ".join(
        "(" + ", ".join(clickhouse_literal(value) for value in row) + ")"
        for row in values
    )
    return f"({column_expr}) IN ({tuple_literals})"


def delete_clickhouse_by_predicates(
    table_name: str,
    predicates: Sequence[str],
) -> None:
    """Xóa dữ liệu ClickHouse theo nhiều predicates rồi chờ mutation hoàn tất.

    Input là tên bảng và list predicate WHERE.
    Output là bảng ClickHouse đã xóa xong các phạm vi cần replace.
    """
    if not predicates:
        return

    for predicate in predicates:
        execute_clickhouse_sql(
            f"""
            ALTER TABLE {table_name}
            DELETE WHERE {predicate}
            """
        )

    wait_for_clickhouse_mutations(table_name)


def build_expected_metrics(
    mart_df: DataFrame,
    count_metrics: Sequence[str],
) -> dict[str, int]:
    """Tính row count và tổng các count metrics từ affected mart DataFrame.

    Input là affected mart DataFrame và danh sách metric cần sum.
    Output là dict metric kỳ vọng để reconcile.
    """
    metrics_row = mart_df.agg(
        *[spark_sum(metric).alias(metric) for metric in count_metrics]
    ).collect()[0]
    metrics = {"row_count": mart_df.count()}

    for metric in count_metrics:
        metrics[metric] = int(metrics_row[metric] or 0)

    return metrics


def parse_clickhouse_int(metric_value: str) -> int:
    """Parse số nguyên từ ClickHouse, coi NULL aggregate là 0."""
    return int(metric_value if metric_value != "\\N" else 0)


def read_clickhouse_metrics(
    table_name: str,
    count_metrics: Sequence[str],
    predicate: str,
) -> dict[str, int]:
    """Đọc row count và tổng metrics từ ClickHouse theo affected predicate.

    Input là tên bảng, danh sách metric và predicate WHERE.
    Output là dict actual metrics.
    """
    metric_selects = ",\n".join(
        f"sum({metric}) AS {metric}" for metric in count_metrics
    )
    result = execute_clickhouse_sql(
        f"""
        SELECT
            count() AS row_count,
            {metric_selects}
        FROM {table_name}
        WHERE {predicate}
        """
    )
    values = result.strip().split("\t")
    metric_names = ["row_count", *count_metrics]

    return {
        metric_name: parse_clickhouse_int(metric_value)
        for metric_name, metric_value in zip(metric_names, values, strict=True)
    }


def read_clickhouse_metrics_for_predicates(
    table_name: str,
    count_metrics: Sequence[str],
    predicates: Sequence[str],
) -> dict[str, int]:
    """Đọc và cộng actual metrics từ nhiều affected predicates.

    Input là tên bảng, danh sách metric và nhiều predicate không overlap.
    Output là dict actual metrics cộng gộp.
    """
    totals = {"row_count": 0, **{metric: 0 for metric in count_metrics}}

    for predicate in predicates:
        metrics = read_clickhouse_metrics(table_name, count_metrics, predicate)

        for metric_name, metric_value in metrics.items():
            totals[metric_name] += metric_value

    return totals


def print_reconciliation(expected: dict[str, int], actual: dict[str, int]) -> None:
    """In reconciliation giữa expected và actual metrics."""
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
        raise SystemExit("Gold incremental serving reconciliation failed")
