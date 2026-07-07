# Distributed Bluesky Streaming Analytics Lakehouse

Project xây dựng một nền tảng xử lý dữ liệu streaming phân tán từ Bluesky
Jetstream.

Tài liệu kỹ thuật chi tiết:

- [Tổng quan dự án](docs/tong-quan-du-an.md)
- [Hướng dẫn làm việc với Codex](docs/huong-dan-lam-viec-voi-codex.md)
- [Script inventory](docs/script-inventory.md)

Project hiện được triển khai theo từng milestone nhỏ và có hai path phục vụ
dashboard:

- Historical/lakehouse path: ưu tiên source of truth, rebuild và reconciliation.
- Realtime fast path: ưu tiên freshness cho dashboard gần thời gian thực.

## Trạng thái hiện tại

Project hiện có historical/lakehouse path local end-to-end:

```text
Jetstream
→ Kafka
→ Spark Bronze trên MinIO
→ Silver Iceberg v1 trên MinIO
→ Gold aggregates build từ Silver Iceberg
→ ClickHouse Gold serving tables
→ Reconciliation check
```

Project cũng có realtime fast path:

```text
Kafka
→ Spark Structured Streaming
→ ClickHouse realtime marts
→ Grafana
```

Lưu ý:

- Bronze hiện là partitioned Parquet trên MinIO.
- Silver Iceberg v1 hiện là source of truth cho 4 bảng chính trên MinIO.
- Silver được build trực tiếp từ Bronze qua transformation module dùng chung.
- Historical Gold aggregates được rebuild từ Silver Iceberg trước khi load vào
  ClickHouse.
- Gold serving layer hiện có 2 bảng ClickHouse:
  - `bluesky.gold_event_volume_by_type`
  - `bluesky.gold_post_engagement_summary`
- Realtime serving layer có các bảng mart theo phút và bảng stream batch health.

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

Tạo ClickHouse Gold/realtime tables nếu chưa có:

```bash
PYTHONPATH=src python scripts/create_clickhouse_gold_tables.py
```

## Historical/lakehouse path

Chạy toàn bộ historical path:

```bash
PYTHONPATH=src:. python scripts/run_historical_lakehouse_path.py
```

Lệnh này thực hiện theo thứ tự:

- Build Silver Iceberg v1 trực tiếp từ Bronze.
- Check Silver Iceberg v1.
- Refresh Gold serving từ Silver Iceberg.
- Check Gold serving v1.

Kiểm tra historical path hiện có mà không build/refresh lại dữ liệu:

```bash
PYTHONPATH=src:. python scripts/check_historical_lakehouse_path.py
```

Các script con vẫn có thể chạy riêng khi cần debug từng tầng:

```bash
PYTHONPATH=src python scripts/build_iceberg_silver_v1.py
PYTHONPATH=src python scripts/check_iceberg_silver_v1.py
PYTHONPATH=src:. python scripts/refresh_gold_serving_from_iceberg.py
PYTHONPATH=src python scripts/check_gold_reconciliation.py
PYTHONPATH=src python scripts/check_gold_post_engagement_reconciliation.py
PYTHONPATH=src:. python scripts/check_gold_serving_v1.py
```

## Realtime fast path

Terminal 1, chạy Spark streaming job ghi realtime metrics vào ClickHouse:

```bash
PYTHONPATH=src python scripts/stream_realtime_metrics_to_clickhouse.py
```

Terminal 2, publish live events vào Kafka:

```bash
PYTHONPATH=src MAX_EVENTS=300 python -m bluesky_pipeline.ingestion_gateway
```

Kiểm tra bảng realtime bằng CLI:

```bash
PYTHONPATH=src python scripts/check_clickhouse_realtime_metrics.py
```

Ví dụ query dùng cho Grafana time series:

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
