"""Dọn dữ liệu đã ingest trong môi trường local.

Script này chỉ phục vụ môi trường học/local demo. Mặc định script chạy ở chế độ
dry-run để in ra các bảng/path/topic sẽ bị dọn, muốn xóa thật phải truyền
--confirm-delete.
"""

from __future__ import annotations

import argparse
import time
from dataclasses import dataclass

from bluesky_pipeline.bronze_tables import (
    BRONZE_ACCOUNT_CHECKPOINT_LOCATION,
    BRONZE_COMMIT_CHECKPOINT_LOCATION,
    BRONZE_EVENT_PATHS,
    BRONZE_IDENTITY_CHECKPOINT_LOCATION,
)
from bluesky_pipeline.clickhouse_client import execute_clickhouse
from bluesky_pipeline.gold_tables import (
    GOLD_CONTENT_ACTIVITY_1M_STREAM_TABLE,
    GOLD_ENGAGEMENT_1M_STREAM_TABLE,
    GOLD_EVENT_VOLUME_1M_STREAM_TABLE,
    GOLD_EVENT_VOLUME_PATH,
    GOLD_EVENT_VOLUME_TABLE,
    GOLD_NETWORK_ACTIVITY_1M_STREAM_TABLE,
    GOLD_POST_ENGAGEMENT_SUMMARY_PATH,
    GOLD_POST_ENGAGEMENT_SUMMARY_TABLE,
    GOLD_REALTIME_METRICS_1M_STREAM_CHECKPOINT_LOCATION,
    GOLD_REALTIME_STREAM_BATCHES_TABLE,
)
from bluesky_pipeline.iceberg_config import (
    ICEBERG_SILVER_STREAM_CHECKPOINT_LOCATION,
    ICEBERG_WAREHOUSE_PATH,
)
from bluesky_pipeline.kafka_config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_RAW_EVENTS_TOPIC


@dataclass(frozen=True)
class CleanupPlan:
    """Danh sách dữ liệu pipeline sẽ được dọn."""

    minio_paths: list[str]
    clickhouse_tables: list[str]
    kafka_topics: list[str]


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
        GOLD_EVENT_VOLUME_1M_STREAM_TABLE,
        GOLD_CONTENT_ACTIVITY_1M_STREAM_TABLE,
        GOLD_ENGAGEMENT_1M_STREAM_TABLE,
        GOLD_NETWORK_ACTIVITY_1M_STREAM_TABLE,
        GOLD_REALTIME_STREAM_BATCHES_TABLE,
    ]

    kafka_topics = [KAFKA_RAW_EVENTS_TOPIC] if include_kafka else []

    return CleanupPlan(
        minio_paths=minio_paths,
        clickhouse_tables=clickhouse_tables,
        kafka_topics=kafka_topics,
    )


def print_plan(plan: CleanupPlan, dry_run: bool) -> None:
    """In kế hoạch cleanup để người chạy xác nhận trước khi xóa thật."""
    mode = "dry_run" if dry_run else "delete"
    print(f"cleanup_mode: {mode}")

    print("\nminio_paths:")
    for path in plan.minio_paths:
        print(f"- {path}")

    print("\nclickhouse_tables:")
    for table in plan.clickhouse_tables:
        print(f"- {table}")

    print("\nkafka_topics:")
    if plan.kafka_topics:
        for topic in plan.kafka_topics:
            print(f"- {topic}")
    else:
        print("- skipped")


def delete_minio_paths(paths: list[str]) -> None:
    """Xóa các prefix trên MinIO thông qua Hadoop FileSystem của Spark."""
    from bluesky_pipeline.spark_session import create_spark_session

    spark = create_spark_session("bluesky-cleanup-ingested-data")
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()

    try:
        for path in paths:
            hadoop_path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(path)
            filesystem = hadoop_path.getFileSystem(hadoop_conf)

            # exists() giúp log rõ path không có dữ liệu thay vì coi là lỗi.
            if filesystem.exists(hadoop_path):
                filesystem.delete(hadoop_path, True)
                print(f"deleted_minio_path: {path}")
            else:
                print(f"missing_minio_path: {path}")

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

    delete_minio_paths(plan.minio_paths)
    truncate_clickhouse_tables(plan.clickhouse_tables)

    print("cleanup_finished")


if __name__ == "__main__":
    main()
