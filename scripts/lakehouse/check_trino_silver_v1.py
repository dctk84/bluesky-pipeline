"""Kiểm tra Trino query được các bảng Silver Iceberg v1."""

from __future__ import annotations

import os
import subprocess

from bluesky_pipeline.config.iceberg import (
    ICEBERG_CATALOG_NAME,
    ICEBERG_SILVER_NAMESPACE,
    ICEBERG_SILVER_TABLES,
)


TRINO_SERVICE_NAME = os.getenv("TRINO_SERVICE_NAME", "trino")


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


def check_catalog_visibility() -> None:
    """In schema/table list để xác nhận Trino nhìn thấy Iceberg catalog."""
    # Các query này kiểm tra metadata path trước khi đụng tới data files.
    schemas = run_trino_query(f"SHOW SCHEMAS FROM {ICEBERG_CATALOG_NAME}")
    tables = run_trino_query(
        f"SHOW TABLES FROM {ICEBERG_CATALOG_NAME}.{ICEBERG_SILVER_NAMESPACE}"
    )

    print("=== trino_schemas ===")
    print(schemas)
    print("\n=== trino_silver_tables ===")
    print(tables)


def check_silver_counts() -> None:
    """Đếm row từng bảng Silver qua Trino để kiểm chứng read path."""
    print("\n=== trino_silver_counts ===")

    for table_name in ICEBERG_SILVER_TABLES:
        query = (
            "SELECT count(*) AS row_count "
            f"FROM {ICEBERG_CATALOG_NAME}.{ICEBERG_SILVER_NAMESPACE}.{table_name}"
        )
        output = run_trino_query(query)
        print(f"{table_name}\t{output}")


def main() -> None:
    """Chạy checkpoint Trino cho Silver Iceberg v1."""
    check_catalog_visibility()
    check_silver_counts()
    print("\nTrino Silver v1 check passed")


if __name__ == "__main__":
    main()
