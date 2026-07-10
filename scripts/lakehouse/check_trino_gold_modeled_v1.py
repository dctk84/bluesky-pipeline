"""Kiểm tra Trino query được các bảng Gold modeled Iceberg v1."""

from __future__ import annotations

import csv
import os
import subprocess

from bluesky_pipeline.iceberg_config import (
    ICEBERG_CATALOG_NAME,
    ICEBERG_GOLD_NAMESPACE,
    ICEBERG_GOLD_TABLES,
)


TRINO_SERVICE_NAME = os.getenv("TRINO_SERVICE_NAME", "trino")

GOLD_KEY_COLUMNS = {
    "gold_dim_actors": "actor_did",
    "gold_dim_posts": "post_uri",
    "gold_fact_content_events": "content_event_id",
    "gold_fact_engagement_events": "engagement_event_id",
    "gold_fact_network_events": "network_event_id",
}


def run_trino_query(query: str) -> str:
    """Chạy một câu SQL bằng Trino CLI trong Docker Compose.

    Input chính là câu SQL cần chạy.
    Output là stdout của Trino CLI nếu query thành công.
    """
    command = [
        "docker",
        "compose",
        "exec",
        "-T",
        TRINO_SERVICE_NAME,
        "trino",
        "--execute",
        query,
    ]

    result = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        raise RuntimeError(
            "Trino query failed\n"
            f"query:\n{query}\n"
            f"stdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )

    return result.stdout.strip()


def parse_trino_csv_row(output: str) -> list[str]:
    """Parse một dòng CSV do Trino CLI trả về.

    Input chính là stdout của Trino CLI ở dạng CSV.
    Output là list giá trị đã bỏ quote CSV.
    """
    rows = [row for row in csv.reader(output.splitlines()) if row]

    if len(rows) != 1:
        raise ValueError(f"Expected one Trino CSV row, got {len(rows)} rows: {output}")

    return rows[0]


def check_catalog_visibility() -> None:
    """In schema/table list để xác nhận Trino nhìn thấy Gold namespace."""
    # Metadata query giúp bắt lỗi catalog/metastore trước khi đọc data files.
    schemas = run_trino_query(f"SHOW SCHEMAS FROM {ICEBERG_CATALOG_NAME}")
    tables = run_trino_query(
        f"SHOW TABLES FROM {ICEBERG_CATALOG_NAME}.{ICEBERG_GOLD_NAMESPACE}"
    )

    print("=== trino_schemas ===")
    print(schemas)
    print("\n=== trino_gold_tables ===")
    print(tables)


def check_gold_counts() -> None:
    """Đếm row từng bảng Gold modeled qua Trino để kiểm chứng read path."""
    print("\n=== trino_gold_counts ===")

    for table_name in ICEBERG_GOLD_TABLES:
        query = (
            "SELECT count(*) AS row_count "
            f"FROM {ICEBERG_CATALOG_NAME}.{ICEBERG_GOLD_NAMESPACE}.{table_name}"
        )
        output = run_trino_query(query)
        print(f"{table_name}\t{output}")


def check_gold_keys() -> None:
    """Kiểm tra key null và duplicate cho các bảng Gold modeled.

    Input là table/key contract nội bộ.
    Output là exception nếu có key null hoặc duplicate.
    """
    print("\n=== trino_gold_key_checks ===")

    has_mismatch = False

    for table_name, key_column in GOLD_KEY_COLUMNS.items():
        query = (
            "SELECT "
            "count(*) AS row_count, "
            f"count({key_column}) AS non_null_key_count, "
            f"count(DISTINCT {key_column}) AS distinct_key_count "
            f"FROM {ICEBERG_CATALOG_NAME}.{ICEBERG_GOLD_NAMESPACE}.{table_name}"
        )
        output = run_trino_query(query)
        # Trino CLI mặc định quote CSV values, ví dụ "123"; dùng csv.reader để parse.
        values = [int(value) for value in parse_trino_csv_row(output)]
        row_count, non_null_key_count, distinct_key_count = values

        status = (
            "OK"
            if row_count == non_null_key_count == distinct_key_count
            else "MISMATCH"
        )

        if status != "OK":
            has_mismatch = True

        print(
            f"{table_name}\tkey={key_column}\t"
            f"rows={row_count}\tnon_null={non_null_key_count}\t"
            f"distinct={distinct_key_count}\t{status}"
        )

    if has_mismatch:
        raise SystemExit("Trino Gold modeled v1 key check failed")


def main() -> None:
    """Chạy checkpoint Trino cho Gold modeled Iceberg v1."""
    check_catalog_visibility()
    check_gold_counts()
    check_gold_keys()
    print("\nTrino Gold modeled v1 check passed")


if __name__ == "__main__":
    main()
