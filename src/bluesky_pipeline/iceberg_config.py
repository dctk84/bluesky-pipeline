"""Cấu hình Spark Iceberg catalog dùng cho MinIO local."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


ICEBERG_SPARK_PACKAGE = os.getenv(
    "ICEBERG_SPARK_PACKAGE",
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.1",
)
ICEBERG_CATALOG_NAME = os.getenv("ICEBERG_CATALOG_NAME", "lakehouse")
ICEBERG_WAREHOUSE_PATH = os.getenv(
    "ICEBERG_WAREHOUSE_PATH",
    "s3a://bluesky-lake/iceberg/warehouse",
)
ICEBERG_SILVER_NAMESPACE = "silver_v1"
ICEBERG_SILVER_POSTS_TABLE = (
    f"{ICEBERG_CATALOG_NAME}.{ICEBERG_SILVER_NAMESPACE}.silver_posts"
)

ICEBERG_SILVER_ENGAGEMENTS_TABLE = (
    f"{ICEBERG_CATALOG_NAME}.{ICEBERG_SILVER_NAMESPACE}.silver_engagements"
)
ICEBERG_SILVER_FOLLOWS_TABLE = (
    f"{ICEBERG_CATALOG_NAME}.{ICEBERG_SILVER_NAMESPACE}.silver_follows"
)
ICEBERG_SILVER_DELETED_RECORDS_TABLE = (
    f"{ICEBERG_CATALOG_NAME}.{ICEBERG_SILVER_NAMESPACE}.silver_deleted_records"
)

ICEBERG_SILVER_TABLES = {
    "silver_posts": ICEBERG_SILVER_POSTS_TABLE,
    "silver_engagements": ICEBERG_SILVER_ENGAGEMENTS_TABLE,
    "silver_follows": ICEBERG_SILVER_FOLLOWS_TABLE,
    "silver_deleted_records": ICEBERG_SILVER_DELETED_RECORDS_TABLE,
}

ICEBERG_SILVER_STREAM_CHECKPOINT_LOCATION = (
    "s3a://bluesky-lake/checkpoints/silver_iceberg_v1_stream"
)


def create_iceberg_spark_session(app_name: str) -> SparkSession:
    """Tạo SparkSession có cấu hình Iceberg Hadoop catalog trên MinIO.

    Input chính là tên Spark app.
    Output là SparkSession có thể tạo, ghi và đọc Iceberg tables.
    """
    # Hadoop catalog dùng warehouse path trên MinIO, chưa cần Hive/REST catalog.
    iceberg_configs = {
        "spark.sql.extensions": (
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        ),
        f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}": (
            "org.apache.iceberg.spark.SparkCatalog"
        ),
        f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}.type": "hadoop",
        f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}.warehouse": (
            ICEBERG_WAREHOUSE_PATH
        ),
    }

    from bluesky_pipeline.spark_session import create_spark_session

    return create_spark_session(
        app_name,
        extra_packages=[ICEBERG_SPARK_PACKAGE],
        extra_configs=iceberg_configs,
    )
