# Script inventory

Tài liệu này mô tả vai trò của các script trong thư mục `scripts/`.

Mục tiêu là giúp người đọc biết script nào là entrypoint chính, script nào là
bước con để debug, và script nào chỉ giữ lại để phục vụ học tập/discovery.

## 1. Main entrypoints

Các script này là lệnh nên ưu tiên chạy khi demo hoặc kiểm tra project.

- `scripts/historical/run_lakehouse_path.py`: chạy toàn bộ historical/lakehouse
  path, gồm build Silver Iceberg, check Silver, refresh Gold serving và check
  Gold serving.
- `scripts/historical/check_lakehouse_path.py`: kiểm tra historical/lakehouse path
  hiện có mà không build hoặc refresh lại dữ liệu.
- `scripts/realtime/stream_metrics_to_clickhouse.py`: chạy realtime fast path từ
  Kafka qua Spark Structured Streaming vào ClickHouse realtime marts.
- `scripts/realtime/check_clickhouse_metrics.py`: kiểm tra các realtime marts và
  batch health trong ClickHouse.

## 2. Historical/lakehouse sub-steps

Các script này là bước con của historical/lakehouse path. Chạy riêng khi cần
debug một tầng cụ thể.

- `scripts/historical/build_iceberg_silver_v1.py`: build Silver Iceberg v1 trực tiếp từ
  Bronze commit events.
- `scripts/historical/check_iceberg_silver_v1.py`: reconcile Silver Iceberg v1 với expected
  metrics tính lại từ Bronze transformation.
- `scripts/gold/refresh_serving_from_iceberg.py`: build Gold aggregates từ Silver
  Iceberg, load vào ClickHouse và chạy Gold serving checkpoint.
- `scripts/gold/check_serving_v1.py`: checkpoint tổng hợp cho Gold serving v1.

## 3. Gold serving sub-steps

Các script này phục vụ build, load và reconcile từng Gold mart.

- `scripts/gold/build_event_volume_from_iceberg.py`: build Gold event volume từ
  Silver Iceberg.
- `scripts/gold/build_post_engagement_summary_from_iceberg.py`: build Gold post
  engagement summary từ Silver Iceberg.
- `scripts/gold/load_event_volume_to_clickhouse.py`: load Gold event volume vào
  ClickHouse.
- `scripts/gold/load_post_engagement_summary_to_clickhouse.py`: load Gold post
  engagement summary vào ClickHouse.
- `scripts/gold/check_event_volume_reconciliation.py`: reconcile Gold event volume trong
  ClickHouse với Silver Iceberg.
- `scripts/gold/check_post_engagement_reconciliation.py`: reconcile Gold post
  engagement summary giữa Gold staging và ClickHouse.
- `scripts/platform/create_clickhouse_gold_tables.py`: tạo database và các bảng serving
  trong ClickHouse.

## 4. Ingestion và Bronze

Các script này phục vụ đưa dữ liệu vào Kafka hoặc ghi Bronze.

- `scripts/ingestion/spark_read_kafka_raw.py`: đọc Kafka raw topic bằng Spark Structured
  Streaming và ghi Bronze Parquet trên MinIO.
- `scripts/ingestion/publish_sample_batch_to_kafka.py`: publish một batch sample events vào
  Kafka để kiểm thử local.
- `scripts/ingestion/publish_sample_to_kafka.py`: publish một sample event vào Kafka để
  kiểm thử nhanh.

## 5. Discovery/debug giữ lại cho học tập

Các script này không nằm trên main path hiện tại, nhưng vẫn hữu ích để giải thích
quá trình khám phá dữ liệu và kiểm chứng từng lớp.

- `scripts/discovery/jetstream_probe.py`: quan sát raw event từ Bluesky Jetstream.
- `scripts/discovery/analyze_sample.py`: phân tích sample raw event đã lưu local.
- `scripts/discovery/normalize_sample.py`: kiểm chứng logic normalize sample trước khi đưa
  vào pipeline chính.
- `scripts/discovery/profile_bronze_commit_events.py`: profile Bronze commit events để thiết
  kế Silver schema.
- `scripts/discovery/read_bronze_parquet.py`: đọc Bronze Parquet để kiểm tra dữ liệu raw đã
  ghi trên MinIO.
- `scripts/discovery/smoke_test_iceberg_minio.py`: smoke test Iceberg catalog và MinIO
  trước khi build Silver Iceberg thật.
