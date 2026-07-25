# Script inventory

Tài liệu này mô tả vai trò của các script trong thư mục `scripts/`.

Mục tiêu là giúp người đọc biết script nào là entrypoint chính, script nào là
bước con để debug, và script nào là utility phục vụ discovery hoặc kiểm chứng
thiết kế.

## 1. Main entrypoints

Các script này là lệnh nên ưu tiên chạy khi demo hoặc kiểm tra project.

- `scripts/lakehouse/run_lakehouse_path.py`: chạy toàn bộ lakehouse
  path, gồm build Silver Iceberg, check Silver, build/check Gold modeled, refresh
  Gold serving và check Gold serving.
- `scripts/lakehouse/run_lakehouse_path_incremental.py`: chạy phase Gold
  incremental sau khi live pipeline đã ghi dữ liệu vào Silver Iceberg; đây là
  entrypoint chính cho demo Gold dashboard trên máy local.
- `scripts/gold/refresh/refresh_gold_incremental.py`: orchestrator chính của
  Gold incremental, chạy facts, dimensions, Gold modeled check và các serving
  marts theo đúng thứ tự phụ thuộc.
- `scripts/lakehouse/check_lakehouse_path.py`: kiểm tra lakehouse path
  hiện có mà không build hoặc refresh lại dữ liệu.
- `scripts/realtime/stream_metrics_to_clickhouse.py`: chạy realtime fast path từ
  Kafka qua Spark Structured Streaming vào ClickHouse realtime marts.
- `scripts/realtime/check_clickhouse_metrics.py`: kiểm tra các realtime marts và
  batch health trong ClickHouse.
- `scripts/e2e/run_live_pipeline.py`: chạy live pipeline gồm ingestion gateway,
  Bronze writer, realtime metrics stream và Bronze-to-Silver streaming job.

Trong môi trường local, không có entrypoint gộp hot path và Gold path chạy song
song. Demo chuẩn của project là chạy `scripts/e2e/run_live_pipeline.py`, dừng
pipeline khi đã ingest đủ dữ liệu, rồi chạy
`scripts/lakehouse/run_lakehouse_path_incremental.py` để refresh Gold modeled và
Gold serving marts.

## 2. Lakehouse sub-steps

Các script này là bước con của lakehouse path. Chạy riêng khi cần debug một tầng
cụ thể.

- `scripts/lakehouse/build_iceberg_silver_v1.py`: build Silver Iceberg v1 trực tiếp từ
  Bronze commit events.
- `scripts/lakehouse/stream_silver_from_bronze.py`: stream Bronze commit events
  sang Silver Iceberg trong live pipeline local.
- `scripts/lakehouse/check_iceberg_silver_v1.py`: reconcile Silver Iceberg v1 với expected
  metrics tính lại từ Bronze transformation.
- `scripts/lakehouse/check_silver_iceberg_readiness.py`: kiểm tra các bảng
  Silver Iceberg tồn tại, đọc được và có dữ liệu tối thiểu trong live mode.
- `scripts/lakehouse/check_trino_silver_v1.py`: kiểm tra Trino query được
  namespace và các bảng Silver Iceberg v1.
- `scripts/lakehouse/build_gold_modeled_v1.py`: build các bảng Gold modeled v1
  trên Iceberg từ Silver Iceberg.
- `scripts/lakehouse/check_trino_gold_modeled_v1.py`: kiểm tra Trino query được
  namespace và các bảng Gold modeled Iceberg v1.
- `scripts/gold/refresh/refresh_serving_from_iceberg.py`: build Gold aggregates từ Silver
  hoặc Gold modeled Iceberg, load vào ClickHouse và chạy Gold serving checkpoint.
- `scripts/gold/refresh/refresh_gold_facts_incremental.py`: refresh incremental
  các Gold fact tables từ Silver Iceberg.
- `scripts/gold/refresh/refresh_gold_dimensions_incremental.py`: refresh
  incremental các Gold dimension tables bằng affected keys và Iceberg merge.
- `scripts/gold/check/check_gold_facts_incremental.py`: kiểm tra key quality cho
  các Gold fact tables sau full build hoặc incremental refresh.
- `scripts/gold/check/check_gold_dimensions_incremental.py`: kiểm tra key quality
  cho các Gold dimension tables sau full build hoặc incremental refresh.
- `scripts/gold/check/check_serving_v1.py`: checkpoint tổng hợp cho Gold serving v1.

## 3. Gold aggregate/serving sub-steps

Các script này phục vụ build, load và reconcile từng aggregate/serving mart. Đây
là phần ClickHouse serving hiện tại, chưa phải toàn bộ Gold modeled layer.

Thư mục `scripts/gold/` được tách theo vai trò:

- `build/`: build mart/staging output từ Silver hoặc Gold modeled.
- `load/`: load mart output vào ClickHouse.
- `check/`: reconciliation, key check và serving validation.
- `refresh/`: incremental refresh orchestration.
- `common/`: helper dùng chung cho các incremental serving jobs.

- `scripts/gold/build/build_event_volume_from_iceberg.py`: build Gold event volume từ
  Silver Iceberg.
- `scripts/gold/build/build_post_engagement_summary_from_iceberg.py`: build legacy Gold
  post engagement summary từ Silver Iceberg.
- `scripts/gold/build/build_post_performance_from_gold_modeled.py`: build Gold post
  performance analytics mart từ Gold modeled Iceberg.
- `scripts/gold/build/build_content_quality_hourly_from_gold_modeled.py`: build Gold
  content quality hourly analytics mart từ Gold modeled Iceberg.
- `scripts/gold/build/build_thread_conversation_summary_from_gold_modeled.py`: build
  Gold thread conversation summary analytics mart từ Gold modeled Iceberg.
- `scripts/gold/build/build_actor_activity_daily_from_gold_modeled.py`: build Gold
  actor activity daily analytics mart từ Gold modeled Iceberg.
- `scripts/gold/build/build_network_growth_daily_from_gold_modeled.py`: build Gold
  network growth daily analytics mart từ Gold modeled Iceberg.
- `scripts/gold/load/load_event_volume_to_clickhouse.py`: load Gold event volume vào
  ClickHouse.
- `scripts/gold/load/load_post_engagement_summary_to_clickhouse.py`: load Gold post
  engagement summary vào ClickHouse.
- `scripts/gold/load/load_post_performance_to_clickhouse.py`: load Gold post
  performance analytics mart vào ClickHouse.
- `scripts/gold/load/load_content_quality_hourly_to_clickhouse.py`: load Gold content
  quality hourly analytics mart vào ClickHouse.
- `scripts/gold/load/load_thread_conversation_summary_to_clickhouse.py`: load Gold
  thread conversation summary analytics mart vào ClickHouse.
- `scripts/gold/load/load_actor_activity_daily_to_clickhouse.py`: load Gold actor
  activity daily analytics mart vào ClickHouse.
- `scripts/gold/load/load_network_growth_daily_to_clickhouse.py`: load Gold network
  growth daily analytics mart vào ClickHouse.
- `scripts/gold/check/check_event_volume_reconciliation.py`: reconcile Gold event volume trong
  ClickHouse với Silver Iceberg.
- `scripts/gold/check/check_post_engagement_reconciliation.py`: reconcile Gold post
  engagement summary giữa Gold staging và ClickHouse.
- `scripts/gold/check/check_post_performance_reconciliation.py`: reconcile Gold post
  performance giữa Gold staging và ClickHouse.
- `scripts/gold/check/check_content_quality_hourly_reconciliation.py`: reconcile Gold
  content quality hourly giữa Gold staging và ClickHouse.
- `scripts/gold/check/check_thread_conversation_summary_reconciliation.py`: reconcile
  Gold thread conversation summary giữa Gold staging và ClickHouse.
- `scripts/gold/check/check_actor_activity_daily_reconciliation.py`: reconcile Gold
  actor activity daily giữa Gold staging và ClickHouse.
- `scripts/gold/check/check_network_growth_daily_reconciliation.py`: reconcile Gold
  network growth daily giữa Gold staging và ClickHouse.
- `scripts/gold/refresh/refresh_gold_content_quality_hourly_incremental.py`: refresh
  incremental mart content quality theo affected hourly windows.
- `scripts/gold/refresh/refresh_gold_post_performance_incremental.py`: refresh
  incremental mart post performance theo affected `post_uri`.
- `scripts/gold/refresh/refresh_gold_thread_conversation_summary_incremental.py`: refresh
  incremental mart thread summary theo affected `reply_root_uri`.
- `scripts/gold/refresh/refresh_gold_actor_activity_daily_incremental.py`: refresh
  incremental mart actor activity theo affected `activity_date + actor_did`.
- `scripts/gold/refresh/refresh_gold_network_growth_daily_incremental.py`: refresh
  incremental mart network growth theo affected `activity_date + target_actor_did`.
- `scripts/gold/common/incremental_serving_utils.py`: helper dùng chung cho
  ClickHouse delete/insert, mutation wait và reconciliation theo affected scope.
- `scripts/platform/create_clickhouse_gold_tables.py`: tạo database và các bảng serving
  trong ClickHouse.
- `scripts/platform/cleanup_ingested_data.py`: dọn dữ liệu ingest local trên
  Kafka, MinIO, Iceberg catalog, ClickHouse và local incremental state bằng cơ
  chế dry-run trước khi xóa thật.

## 4. Ingestion và Bronze

Các script này phục vụ đưa dữ liệu vào Kafka hoặc ghi Bronze.

- `scripts/ingestion/spark_read_kafka_raw.py`: đọc Kafka raw topic bằng Spark Structured
  Streaming và ghi Bronze Parquet trên MinIO.
- `scripts/ingestion/publish_sample_batch_to_kafka.py`: publish một batch sample events vào
  Kafka để kiểm thử local.
- `scripts/ingestion/publish_sample_to_kafka.py`: publish một sample event vào Kafka để
  kiểm thử nhanh.

## 5. Discovery/debug utilities

Các script này không nằm trên main path hiện tại, nhưng vẫn hữu ích để tái hiện
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
- `scripts/discovery/smoke_test_iceberg_merge.py`: smoke test Iceberg `MERGE INTO`
  trước khi dùng row-level merge cho Gold dimension incremental.
