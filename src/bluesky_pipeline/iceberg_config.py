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
ICEBERG_CATALOG_TYPE = os.getenv("ICEBERG_CATALOG_TYPE", "hive")
ICEBERG_HIVE_METASTORE_URI = os.getenv(
    "ICEBERG_HIVE_METASTORE_URI",
    "thrift://localhost:9083",
)
ICEBERG_WAREHOUSE_PATH = os.getenv(
    "ICEBERG_WAREHOUSE_PATH",
    "s3://bluesky-lake/iceberg/warehouse",
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

ICEBERG_GOLD_NAMESPACE = "gold_v1"
ICEBERG_GOLD_DIM_ACTORS_TABLE = (
    f"{ICEBERG_CATALOG_NAME}.{ICEBERG_GOLD_NAMESPACE}.gold_dim_actors"
)
ICEBERG_GOLD_DIM_POSTS_TABLE = (
    f"{ICEBERG_CATALOG_NAME}.{ICEBERG_GOLD_NAMESPACE}.gold_dim_posts"
)
ICEBERG_GOLD_FACT_CONTENT_EVENTS_TABLE = (
    f"{ICEBERG_CATALOG_NAME}.{ICEBERG_GOLD_NAMESPACE}.gold_fact_content_events"
)
ICEBERG_GOLD_FACT_ENGAGEMENT_EVENTS_TABLE = (
    f"{ICEBERG_CATALOG_NAME}.{ICEBERG_GOLD_NAMESPACE}.gold_fact_engagement_events"
)
ICEBERG_GOLD_FACT_NETWORK_EVENTS_TABLE = (
    f"{ICEBERG_CATALOG_NAME}.{ICEBERG_GOLD_NAMESPACE}.gold_fact_network_events"
)

ICEBERG_GOLD_TABLES = {
    "gold_dim_actors": ICEBERG_GOLD_DIM_ACTORS_TABLE,
    "gold_dim_posts": ICEBERG_GOLD_DIM_POSTS_TABLE,
    "gold_fact_content_events": ICEBERG_GOLD_FACT_CONTENT_EVENTS_TABLE,
    "gold_fact_engagement_events": ICEBERG_GOLD_FACT_ENGAGEMENT_EVENTS_TABLE,
    "gold_fact_network_events": ICEBERG_GOLD_FACT_NETWORK_EVENTS_TABLE,
}

ICEBERG_SILVER_STREAM_CHECKPOINT_LOCATION = (
    "s3a://bluesky-lake/checkpoints/silver_iceberg_v1_stream"
)


def create_iceberg_spark_session(app_name: str) -> SparkSession:
    """Tạo SparkSession có cấu hình Iceberg catalog trên MinIO.

    Input chính là tên Spark app.
    Output là SparkSession có thể tạo, ghi và đọc Iceberg tables.
    """
    # Hive catalog giúp Spark và Trino nhìn chung Iceberg table metadata.
    iceberg_configs = {
        "spark.sql.extensions": (
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        ),
        f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}": (
            "org.apache.iceberg.spark.SparkCatalog"
        ),
        f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}.type": ICEBERG_CATALOG_TYPE,
        f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}.warehouse": (
            ICEBERG_WAREHOUSE_PATH
        ),
    }

    if ICEBERG_CATALOG_TYPE == "hive":
        iceberg_configs[f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}.uri"] = (
            ICEBERG_HIVE_METASTORE_URI
        )

    from bluesky_pipeline.spark_session import create_spark_session

    return create_spark_session(
        app_name,
        extra_packages=[ICEBERG_SPARK_PACKAGE],
        extra_configs=iceberg_configs,
    )
