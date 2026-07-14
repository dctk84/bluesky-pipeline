"""Dọn dữ liệu đã ingest trong môi trường local.

Script này chỉ phục vụ môi trường học/local demo. Mặc định script chạy ở chế độ
dry-run để in ra các bảng/path/topic sẽ bị dọn, muốn xóa thật phải truyền
--confirm-delete.
"""

from __future__ import annotations

import argparse
import time
from dataclasses import dataclass
from pathlib import Path

from bluesky_pipeline.schemas.bronze_tables import (
    BRONZE_ACCOUNT_CHECKPOINT_LOCATION,
    BRONZE_COMMIT_CHECKPOINT_LOCATION,
    BRONZE_EVENT_PATHS,
    BRONZE_IDENTITY_CHECKPOINT_LOCATION,
)
from bluesky_pipeline.clients.clickhouse import execute_clickhouse
from bluesky_pipeline.schemas.gold_tables import (
    GOLD_ACTOR_ACTIVITY_DAILY_PATH,
    GOLD_ACTOR_ACTIVITY_DAILY_TABLE,
    GOLD_CONTENT_QUALITY_HOURLY_PATH,
    GOLD_CONTENT_QUALITY_HOURLY_TABLE,
    GOLD_CONTENT_ACTIVITY_1M_STREAM_TABLE,
    GOLD_ENGAGEMENT_1M_STREAM_TABLE,
    GOLD_EVENT_VOLUME_1M_STREAM_TABLE,
    GOLD_EVENT_VOLUME_PATH,
    GOLD_EVENT_VOLUME_TABLE,
    GOLD_NETWORK_GROWTH_DAILY_PATH,
    GOLD_NETWORK_GROWTH_DAILY_TABLE,
    GOLD_NETWORK_ACTIVITY_1M_STREAM_TABLE,
    GOLD_POST_ENGAGEMENT_SUMMARY_PATH,
    GOLD_POST_ENGAGEMENT_SUMMARY_TABLE,
    GOLD_POST_PERFORMANCE_PATH,
    GOLD_POST_PERFORMANCE_TABLE,
    GOLD_REALTIME_METRICS_1M_STREAM_CHECKPOINT_LOCATION,
    GOLD_REALTIME_STREAM_BATCHES_TABLE,
    GOLD_THREAD_CONVERSATION_SUMMARY_PATH,
    GOLD_THREAD_CONVERSATION_SUMMARY_TABLE,
)
from bluesky_pipeline.config.iceberg import (
    ICEBERG_CATALOG_NAME,
    ICEBERG_GOLD_NAMESPACE,
    ICEBERG_GOLD_TABLES,
    ICEBERG_SILVER_NAMESPACE,
    ICEBERG_SILVER_STREAM_CHECKPOINT_LOCATION,
    ICEBERG_SILVER_TABLES,
    ICEBERG_WAREHOUSE_PATH,
    create_iceberg_spark_session,
)
from bluesky_pipeline.config.kafka import KAFKA_BOOTSTRAP_SERVERS, KAFKA_RAW_EVENTS_TOPIC


PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOCAL_INCREMENTAL_STATE_PATTERNS = [
    "data/state/*.json",
]


@dataclass(frozen=True)
class CleanupPlan:
    """Danh sách dữ liệu pipeline sẽ được dọn."""

    minio_paths: list[str]
    clickhouse_tables: list[str]
    kafka_topics: list[str]
    iceberg_tables: list[str]
    iceberg_namespaces: list[str]
    local_state_paths: list[str]


def build_cleanup_plan(include_kafka: bool) -> CleanupPlan:
    """Tạo kế hoạch cleanup từ các contract path/table/topic dùng chung."""
    bronze_paths = list(BRONZE_EVENT_PATHS.values())
    checkpoint_paths = [
        BRONZE_COMMIT_CHECKPOINT_LOCATION,
        BRONZE_IDENTITY_CHECKPOINT_LOCATION,
        BRONZE_ACCOUNT_CHECKPOINT_LOCATION,
        ICEBERG_SILVER_STREAM_CHECKPOINT_LOCATION,
        GOLD_REALTIME_METRICS_1M_STREAM_CHECKPOINT_LOCATION,
    ]
    gold_staging_paths = [
        GOLD_EVENT_VOLUME_PATH,
        GOLD_POST_ENGAGEMENT_SUMMARY_PATH,
        GOLD_POST_PERFORMANCE_PATH,
        GOLD_CONTENT_QUALITY_HOURLY_PATH,
        GOLD_THREAD_CONVERSATION_SUMMARY_PATH,
        GOLD_ACTOR_ACTIVITY_DAILY_PATH,
        GOLD_NETWORK_GROWTH_DAILY_PATH,
    ]

    minio_paths = [
        *bronze_paths,
        ICEBERG_WAREHOUSE_PATH,
        *gold_staging_paths,
        *checkpoint_paths,
    ]

    clickhouse_tables = [
        GOLD_EVENT_VOLUME_TABLE,
        GOLD_POST_ENGAGEMENT_SUMMARY_TABLE,
        GOLD_POST_PERFORMANCE_TABLE,
        GOLD_CONTENT_QUALITY_HOURLY_TABLE,
        GOLD_THREAD_CONVERSATION_SUMMARY_TABLE,
        GOLD_ACTOR_ACTIVITY_DAILY_TABLE,
        GOLD_NETWORK_GROWTH_DAILY_TABLE,
        GOLD_EVENT_VOLUME_1M_STREAM_TABLE,
        GOLD_CONTENT_ACTIVITY_1M_STREAM_TABLE,
        GOLD_ENGAGEMENT_1M_STREAM_TABLE,
        GOLD_NETWORK_ACTIVITY_1M_STREAM_TABLE,
        GOLD_REALTIME_STREAM_BATCHES_TABLE,
    ]

    kafka_topics = [KAFKA_RAW_EVENTS_TOPIC] if include_kafka else []
    iceberg_tables = [
        *ICEBERG_GOLD_TABLES.values(),
        *ICEBERG_SILVER_TABLES.values(),
    ]
    iceberg_namespaces = [
        f"{ICEBERG_CATALOG_NAME}.{ICEBERG_GOLD_NAMESPACE}",
        f"{ICEBERG_CATALOG_NAME}.{ICEBERG_SILVER_NAMESPACE}",
    ]

    return CleanupPlan(
        minio_paths=minio_paths,
        clickhouse_tables=clickhouse_tables,
        kafka_topics=kafka_topics,
        iceberg_tables=iceberg_tables,
        iceberg_namespaces=iceberg_namespaces,
        local_state_paths=LOCAL_INCREMENTAL_STATE_PATTERNS,
    )


def print_plan(plan: CleanupPlan, dry_run: bool) -> None:
    """In kế hoạch cleanup để người chạy xác nhận trước khi xóa thật."""
    mode = "dry_run" if dry_run else "delete"
    print(f"cleanup_mode: {mode}")

    print("\nminio_paths:")
    for path in plan.minio_paths:
        delete_path = normalize_minio_path_for_hadoop(path)
        if delete_path == path:
            print(f"- {path}")
        else:
            print(f"- {path} delete_as={delete_path}")

    print("\nclickhouse_tables:")
    for table in plan.clickhouse_tables:
        print(f"- {table}")

    print("\nkafka_topics:")
    if plan.kafka_topics:
        for topic in plan.kafka_topics:
            print(f"- {topic}")
    else:
        print("- skipped")

    print("\niceberg_tables:")
    for table in plan.iceberg_tables:
        print(f"- {table}")

    print("\niceberg_namespaces:")
    for namespace in plan.iceberg_namespaces:
        print(f"- {namespace}")

    print("\nlocal_state_paths:")
    for path in plan.local_state_paths:
        print(f"- {path}")


def normalize_minio_path_for_hadoop(path: str) -> str:
    """Chuẩn hóa scheme S3 để Hadoop FileSystem xóa được object trên MinIO.

    Input là path contract có thể dùng `s3://` hoặc `s3a://`.
    Output là path dùng `s3a://`, phù hợp với cấu hình Spark/Hadoop local.
    """
    if path.startswith("s3://"):
        return path.replace("s3://", "s3a://", 1)

    return path


def drop_iceberg_catalog_objects(tables: list[str], namespaces: list[str]) -> None:
    """Drop metadata Iceberg trong Hive Metastore trước khi xóa data files.

    Input là danh sách bảng và namespace Iceberg từ contract dùng chung.
    Output là metastore không còn trỏ tới các bảng Silver/Gold cũ.
    """
    spark = create_iceberg_spark_session("bluesky-cleanup-iceberg-catalog")
    spark.sparkContext.setLogLevel("WARN")

    try:
        for table in tables:
            spark.sql(f"DROP TABLE IF EXISTS {table}")
            print(f"dropped_iceberg_table: {table}")

        for namespace in namespaces:
            # Không dùng CASCADE để tránh âm thầm bỏ sót table mới chưa có trong plan.
            spark.sql(f"DROP NAMESPACE IF EXISTS {namespace}")
            print(f"dropped_iceberg_namespace: {namespace}")

    finally:
        spark.stop()


def delete_minio_paths(paths: list[str]) -> None:
    """Xóa các prefix trên MinIO thông qua Hadoop FileSystem của Spark."""
    from bluesky_pipeline.config.spark import create_spark_session

    spark = create_spark_session("bluesky-cleanup-ingested-data")
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()

    try:
        for path in paths:
            delete_path = normalize_minio_path_for_hadoop(path)
            hadoop_path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(delete_path)
            filesystem = hadoop_path.getFileSystem(hadoop_conf)

            # exists() giúp log rõ path không có dữ liệu thay vì coi là lỗi.
            if filesystem.exists(hadoop_path):
                filesystem.delete(hadoop_path, True)
                if delete_path == path:
                    print(f"deleted_minio_path: {path}")
                else:
                    print(f"deleted_minio_path: {path} delete_as={delete_path}")
            else:
                if delete_path == path:
                    print(f"missing_minio_path: {path}")
                else:
                    print(f"missing_minio_path: {path} delete_as={delete_path}")

    finally:
        spark.stop()


def truncate_clickhouse_tables(tables: list[str]) -> None:
    """Xóa dữ liệu trong các bảng ClickHouse nhưng giữ lại schema."""
    for table in tables:
        execute_clickhouse(f"TRUNCATE TABLE IF EXISTS {table}")
        print(f"truncated_clickhouse_table: {table}")


def get_topic_shape(admin_client: AdminClient, topic: str) -> tuple[int, int] | None:
    """Đọc số partition/replication hiện tại để recreate topic sau khi xóa."""
    metadata = admin_client.list_topics(timeout=10)
    topic_metadata = metadata.topics.get(topic)

    if topic_metadata is None or topic_metadata.error is not None:
        return None

    partitions = len(topic_metadata.partitions)
    replication_factor = min(
        len(partition.replicas) for partition in topic_metadata.partitions.values()
    )
    return partitions, replication_factor


def wait_for_topic_deletion(admin_client: AdminClient, topic: str) -> None:
    """Chờ Kafka hoàn tất xóa topic trước khi tạo lại cùng tên."""
    for _ in range(30):
        metadata = admin_client.list_topics(timeout=10)
        topic_metadata = metadata.topics.get(topic)

        if topic_metadata is None or topic_metadata.error is not None:
            return

        time.sleep(1)

    raise RuntimeError(f"Timed out waiting for Kafka topic deletion: {topic}")


def purge_kafka_topics(topics: list[str]) -> None:
    """Xóa rồi tạo lại Kafka topic để loại bỏ raw event còn trong log."""
    from confluent_kafka.admin import AdminClient, NewTopic

    admin_client = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    for topic in topics:
        topic_shape = get_topic_shape(admin_client, topic)

        if topic_shape is None:
            print(f"missing_kafka_topic: {topic}")
            continue

        partitions, replication_factor = topic_shape
        delete_results = admin_client.delete_topics([topic], operation_timeout=30)
        delete_results[topic].result()
        wait_for_topic_deletion(admin_client, topic)

        # Recreate topic để các producer/consumer vẫn dùng đúng contract topic cũ.
        create_results = admin_client.create_topics(
            [
                NewTopic(
                    topic,
                    num_partitions=partitions,
                    replication_factor=replication_factor,
                )
            ],
            operation_timeout=30,
        )
        create_results[topic].result()
        print(
            "purged_kafka_topic: "
            f"{topic} partitions={partitions} replication_factor={replication_factor}"
        )


def delete_local_state_paths(paths: list[str]) -> None:
    """Xóa local incremental state để lần chạy sạch không dùng marker cũ.

    Input là danh sách path hoặc glob pattern tương đối từ project root.
    Output là các state file cũ bị xóa nếu tồn tại.
    """
    for path in paths:
        matches = (
            sorted(PROJECT_ROOT.glob(path))
            if "*" in path
            else [PROJECT_ROOT / path]
        )

        if not matches:
            print(f"missing_local_state_path: {path}")
            continue

        for matched_path in matches:
            if matched_path.is_file():
                matched_path.unlink()
                print(
                    "deleted_local_state_path: "
                    f"{matched_path.relative_to(PROJECT_ROOT)}"
                )
            else:
                print(
                    "skipped_local_state_path: "
                    f"{matched_path.relative_to(PROJECT_ROOT)}"
                )


def parse_args() -> argparse.Namespace:
    """Đọc CLI flags cho cleanup script."""
    parser = argparse.ArgumentParser(
        description="Cleanup local pipeline data from Kafka, MinIO and ClickHouse."
    )
    parser.add_argument(
        "--confirm-delete",
        action="store_true",
        help="Xóa dữ liệu thật. Nếu không truyền flag này, script chỉ dry-run.",
    )
    parser.add_argument(
        "--include-kafka-topic",
        action="store_true",
        help=(
            "Xóa rồi tạo lại Kafka raw topic để purge event log. "
            "Chỉ chạy khi đã dừng live pipeline."
        ),
    )
    return parser.parse_args()


def main() -> None:
    """Chạy cleanup dữ liệu ingest local theo kế hoạch đã in ra."""
    args = parse_args()
    dry_run = not args.confirm_delete
    plan = build_cleanup_plan(include_kafka=args.include_kafka_topic)

    print_plan(plan, dry_run=dry_run)

    if dry_run:
        print("\nresult: dry_run_only")
        print("next_command: add --confirm-delete to delete these data paths/tables")
        return

    print("\ncleanup_started")

    # Nên dừng live pipeline trước để không có writer đang ghi vào path/table/topic.
    if plan.kafka_topics:
        purge_kafka_topics(plan.kafka_topics)

    drop_iceberg_catalog_objects(plan.iceberg_tables, plan.iceberg_namespaces)
    delete_minio_paths(plan.minio_paths)
    truncate_clickhouse_tables(plan.clickhouse_tables)
    delete_local_state_paths(plan.local_state_paths)

    print("cleanup_finished")


if __name__ == "__main__":
    main()
