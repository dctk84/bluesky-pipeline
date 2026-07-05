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
→ Silver Parquet v1 trên MinIO
→ Gold event volume prototype trên MinIO
→ ClickHouse Gold serving table
→ Reconciliation check
```

Lưu ý:

- Bronze hiện là partitioned Parquet trên MinIO.
- Silver v1 hiện là Parquet prototype trên MinIO, chưa phải Iceberg.
- Gold prototype trên MinIO dùng để kiểm chứng aggregate.
- Gold serving layer chính thức hiện bắt đầu bằng ClickHouse table
  `bluesky.gold_event_volume_by_type`.

## Chạy local

Khởi động hạ tầng:

```bash
docker compose up -d kafka minio clickhouse
```

Kiểm tra ClickHouse:

```bash
curl 'http://default:clickhouse@localhost:8123/?query=SELECT%201'
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

Kiểm tra Silver v1:

```bash
PYTHONPATH=src python scripts/check_silver_v1.py
```

Build và load Gold event volume:

```bash
PYTHONPATH=src python scripts/build_gold_event_volume.py
PYTHONPATH=src python scripts/load_gold_event_volume_to_clickhouse.py
```

Kiểm tra ClickHouse Gold và reconciliation:

```bash
PYTHONPATH=src python scripts/check_clickhouse_gold_event_volume.py
PYTHONPATH=src python scripts/check_gold_reconciliation.py
```
