# Distributed Bluesky Streaming Analytics Lakehouse

Project xây dựng một nền tảng xử lý dữ liệu streaming phân tán từ Bluesky
Jetstream.

Tài liệu kỹ thuật chi tiết:

- [Tổng quan dự án](docs/tong-quan-du-an.md)
- [Hướng dẫn làm việc với Codex](docs/huong-dan-lam-viec-voi-codex.md)

Project hiện được triển khai theo từng milestone nhỏ.

## Trạng thái hiện tại

Project hiện đã có một luồng local end-to-end:

```text
Jetstream
→ Kafka
→ Spark Bronze trên MinIO
→ Silver Iceberg v1 trên MinIO
→ Gold aggregates build từ Silver Iceberg
→ ClickHouse Gold serving tables
→ Reconciliation check
```

Lưu ý:

- Bronze hiện là partitioned Parquet trên MinIO.
- Silver Parquet v1 vẫn còn là prototype/rebuild source trung gian.
- Silver Iceberg v1 hiện đã có đủ 4 bảng chính trên MinIO.
- Gold aggregates được rebuild từ Silver Iceberg trước khi load vào ClickHouse.
- Gold serving layer hiện có 2 bảng ClickHouse:
  - `bluesky.gold_event_volume_by_type`
  - `bluesky.gold_post_engagement_summary`

## Chạy local

Khởi động hạ tầng:

```bash
docker compose up -d kafka minio clickhouse
```

Kiểm tra ClickHouse:

```bash
curl 'http://default:clickhouse@localhost:8123/?query=SELECT%201'
```

Một số biến môi trường có thể override khi chạy local:

```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=bluesky.raw.events.v2
SPARK_KAFKA_CONNECTOR_PACKAGE=org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1
MAX_EVENTS=300
CLICKHOUSE_URL=http://localhost:8123
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=clickhouse
```

Chạy Spark Bronze writer:

```bash
PYTHONPATH=src python scripts/spark_read_kafka_raw.py
```

Ở terminal khác, chạy ingestion gateway để publish live events vào Kafka:

```bash
PYTHONPATH=src MAX_EVENTS=300 python -m bluesky_pipeline.ingestion_gateway
```

Build Silver v1:

```bash
PYTHONPATH=src python scripts/build_silver_posts.py
PYTHONPATH=src python scripts/build_silver_engagements.py
PYTHONPATH=src python scripts/build_silver_follows.py
PYTHONPATH=src python scripts/build_silver_deleted_records.py
```

Kiểm tra Silver Parquet v1 và build Silver Iceberg v1:

```bash
PYTHONPATH=src python scripts/check_silver_v1.py
PYTHONPATH=src python scripts/build_iceberg_silver_v1.py
PYTHONPATH=src python scripts/check_iceberg_silver_v1.py
```

Tạo ClickHouse Gold tables:

```bash
PYTHONPATH=src python scripts/create_clickhouse_gold_tables.py
```

Build Gold aggregates từ Silver Iceberg và load ClickHouse:

```bash
PYTHONPATH=src python scripts/build_gold_event_volume_from_iceberg.py
PYTHONPATH=src python scripts/build_gold_post_engagement_summary_from_iceberg.py
PYTHONPATH=src python scripts/load_gold_event_volume_to_clickhouse.py
PYTHONPATH=src python scripts/load_gold_post_engagement_summary_to_clickhouse.py
```

Refresh Gold serving từ Silver Iceberg bằng một entrypoint tổng hợp:

```bash
PYTHONPATH=src:. python scripts/refresh_gold_serving_from_iceberg.py
```

Kiểm tra ClickHouse Gold và reconciliation:

```bash
PYTHONPATH=src python scripts/check_clickhouse_gold_event_volume.py
PYTHONPATH=src python scripts/check_clickhouse_gold_post_engagement_summary.py
PYTHONPATH=src python scripts/check_gold_reconciliation.py
PYTHONPATH=src python scripts/check_gold_post_engagement_reconciliation.py
```

Chạy checkpoint tổng hợp cho Gold serving v1:

```bash
PYTHONPATH=src:. python scripts/check_gold_serving_v1.py
```

## Chạy streaming end-to-end tới Grafana

Tạo ClickHouse tables nếu chưa có:

```bash
PYTHONPATH=src python scripts/create_clickhouse_gold_tables.py
```

Terminal 1, chạy Spark streaming job ghi event volume realtime vào ClickHouse:

```bash
PYTHONPATH=src python scripts/stream_event_volume_to_clickhouse.py
```

Terminal 2, publish live events vào Kafka:

```bash
PYTHONPATH=src MAX_EVENTS=300 python -m bluesky_pipeline.ingestion_gateway
```

Kiểm tra bảng realtime bằng CLI:

```bash
PYTHONPATH=src python scripts/check_clickhouse_stream_event_volume.py
```

Query dùng cho Grafana time series:

```sql
SELECT
    window_start AS time,
    event_type AS metric,
    sum(event_count) AS event_count
FROM bluesky.gold_event_volume_1m_stream
WHERE $__timeFilter(window_start)
GROUP BY
    time,
    metric
ORDER BY
    time ASC,
    metric ASC
```

Nếu query có `$__timeFilter` không trả dữ liệu, kiểm tra trước bằng query không có
time filter hoặc chỉnh time picker để bao đúng khoảng `window_start` đang có trong
ClickHouse.
