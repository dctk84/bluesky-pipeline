# Distributed Bluesky Streaming Analytics Lakehouse

Nền tảng xử lý dữ liệu streaming từ Bluesky Jetstream, sử dụng Kafka, Spark
Structured Streaming, MinIO, Apache Iceberg, Trino, ClickHouse và Grafana để mô
phỏng một streaming analytics lakehouse chạy trên môi trường local.

## Overview

Project thu thập dữ liệu sự kiện công khai từ Bluesky Jetstream, đưa vào Kafka,
xử lý bằng Spark, lưu lịch sử trên data lake/lakehouse và phục vụ dashboard qua
ClickHouse/Grafana.

Kiến trúc hiện tại có hai serving path:

- **Realtime fast path**: ưu tiên freshness cho dashboard vận hành gần thời gian
  thực.
- **Lakehouse path**: ưu tiên source of truth, rebuildability, reconciliation và
  analytics sâu hơn qua Bronze/Silver/Gold.

Tài liệu kỹ thuật chi tiết:

- [Tổng quan dự án](docs/tong-quan-du-an.md)
- [Silver schema v1](docs/silver-schema-v1.md)
- [Gold data model v1](docs/gold-data-model-v1.md)
- [Gold analytics metrics v1](docs/gold-analytics-metrics-v1.md)
- [Gold incremental refresh design](docs/gold-incremental-refresh-design.md)
- [Jetstream schema notes](docs/jetstream-schema-notes.md)
- [Script inventory](docs/script-inventory.md)

## Project Goals

- Ingest sự kiện Bluesky Jetstream liên tục qua WebSocket.
- Buffer và replay dữ liệu bằng Kafka.
- Xử lý streaming bằng Spark Structured Streaming.
- Lưu raw history trên MinIO dưới dạng Bronze Parquet.
- Chuẩn hóa dữ liệu vào Silver Iceberg.
- Build Gold modeled fact/dimension tables và Gold analytics serving marts.
- Query lakehouse bằng Trino.
- Serve dashboard low-latency bằng ClickHouse và Grafana.
- Duy trì checkpoint, reconciliation và cleanup workflow rõ ràng cho local run.

## Architecture

![Distributed Bluesky Streaming Analytics Lakehouse](docs/assets/bluesky-pipeline-architecture.png)

Kiến trúc được triển khai trong Docker Compose local runtime:

```text
Bluesky Jetstream
  -> Python Ingestion Gateway
  -> Apache Kafka
  -> Apache Spark
      ├-> Realtime Fast Path -> ClickHouse -> Grafana
      └-> Lakehouse Path -> MinIO/Iceberg -> Trino + ClickHouse -> Grafana
```

Airflow được đặt trong roadmap để orchestration các workflow hữu hạn như Gold
refresh, data quality, compaction, snapshot expiration, backfill và rebuild.

## Data Flow

### Realtime Fast Path

```text
Kafka
  -> Spark Structured Streaming
  -> ClickHouse realtime marts
  -> Grafana Realtime Dashboard
```

Fast path phục vụ các metric vận hành theo phút như event volume, content
activity, engagement activity, network activity, freshness và Spark batch health.

### Lakehouse Path

```text
Kafka
  -> Spark Bronze writer
  -> Bronze Parquet on MinIO
  -> Silver Iceberg
  -> Gold modeled Iceberg
  -> Gold analytics serving marts
  -> ClickHouse
  -> Grafana Gold Analytics Dashboard
```

Lakehouse path giữ source of truth cho analytics. Silver/Gold Iceberg có thể query
bằng Trino, còn ClickHouse giữ các serving marts để dashboard truy vấn nhanh.

## Technology Stack

| Layer | Technologies |
| --- | --- |
| Ingestion | Python, asyncio, WebSocket, confluent-kafka |
| Streaming broker | Apache Kafka |
| Compute | Apache Spark, PySpark, Spark Structured Streaming |
| Object storage | MinIO, S3-compatible API |
| Table format | Apache Iceberg |
| Query engine | Trino, Hive Metastore |
| Serving database | ClickHouse |
| Dashboard | Grafana, ClickHouse datasource |
| Local runtime | Docker Compose, WSL/Linux |
| Testing | pytest |

## Repository Structure

```text
bluesky-pipeline/
├── src/
│   └── bluesky_pipeline/
│       ├── clients/      # Helper giao tiếp service bên ngoài, ví dụ ClickHouse
│       ├── config/       # Cấu hình runtime cho Kafka, Spark và Iceberg
│       ├── schemas/      # Schema, table name, path contract và DDL dùng chung
│       ├── state/        # Contract/state cho incremental refresh
│       ├── transforms/   # Logic biến đổi Bronze, Silver, Gold và event envelope
│       └── ingestion_gateway.py
├── scripts/
│   ├── discovery/        # Script probe nguồn và phân tích schema
│   ├── ingestion/        # Script publish sample và ghi Bronze
│   ├── e2e/              # Runner live pipeline cho môi trường local
│   ├── lakehouse/        # Script build/check Silver, Gold modeled và lakehouse
│   ├── gold/
│   │   ├── build/        # Build Gold serving marts
│   │   ├── load/         # Load marts vào ClickHouse
│   │   ├── check/        # Reconciliation và validation
│   │   ├── refresh/      # Orchestration cho Gold incremental refresh
│   │   └── common/       # Helper dùng chung cho Gold scripts
│   ├── realtime/         # Realtime fast-path metrics vào ClickHouse
│   └── platform/         # Cleanup và setup object local
├── config/
│   ├── hive/             # Cấu hình Hive Metastore
│   └── trino/            # Cấu hình Trino catalog
├── docs/
│   └── assets/           # Tài liệu thiết kế và hình ảnh dashboard/architecture
├── tests/                # Unit tests
├── docker-compose.yml    # Local runtime services
├── requirements.txt      # Python dependencies
└── README.md
```

## Data Model

### Bronze

Bronze lưu raw events dạng partitioned Parquet trên MinIO:

- `bluesky_commit_events`
- `bluesky_identity_events`
- `bluesky_account_events`

Bronze giữ payload gần nguồn để audit, replay và reprocess.

### Silver

Silver Iceberg v1 chuẩn hóa commit events thành các bảng event-level:

- `lakehouse.silver_v1.silver_posts`
- `lakehouse.silver_v1.silver_engagements`
- `lakehouse.silver_v1.silver_follows`
- `lakehouse.silver_v1.silver_deleted_records`

### Gold Modeled

Gold modeled Iceberg v1 tạo các bảng fact/dimension:

- `lakehouse.gold_v1.gold_dim_actors`
- `lakehouse.gold_v1.gold_dim_posts`
- `lakehouse.gold_v1.gold_fact_content_events`
- `lakehouse.gold_v1.gold_fact_engagement_events`
- `lakehouse.gold_v1.gold_fact_network_events`

### Gold Serving Marts

ClickHouse giữ các serving marts cho dashboard:

- `bluesky.gold_post_performance`
- `bluesky.gold_content_quality_hourly`
- `bluesky.gold_thread_conversation_summary`
- `bluesky.gold_actor_activity_daily`
- `bluesky.gold_network_growth_daily`
- Realtime marts theo phút cho hot path.

## Key Engineering Features

- WebSocket ingestion gateway với reconnect, bounded retry và graceful shutdown.
- Kafka raw topic với partitioning, offsets, retention và replay.
- Spark Structured Streaming jobs cho Bronze, Silver và realtime metrics.
- Bronze/Silver/Gold lakehouse layout trên MinIO và Iceberg.
- Trino SQL query layer cho Silver/Gold Iceberg.
- Incremental Gold refresh theo affected keys/windows.
- ClickHouse serving marts có thể rebuild từ Gold modeled hoặc Silver.
- Reconciliation checks cho Silver, Gold modeled và Gold serving marts.
- Cleanup script cho Kafka topic, MinIO paths, Iceberg tables, ClickHouse tables
  và local incremental state.
- Grafana dashboards cho realtime hot path và Gold analytics.

## Prerequisites

- Linux hoặc WSL Ubuntu.
- Docker và Docker Compose.
- Python 3.12.
- Java runtime tương thích với PySpark 3.5.1.
- Python dependencies trong `requirements.txt`.

Ví dụ setup Python environment:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Getting Started

Khởi động hạ tầng local:

```bash
docker compose up -d kafka minio hive-metastore trino clickhouse grafana
```

Tạo ClickHouse serving tables nếu chưa có:

```bash
PYTHONPATH=src python scripts/platform/create_clickhouse_gold_tables.py
```

Kiểm tra ClickHouse:

```bash
curl 'http://default:clickhouse@localhost:8123/?query=SELECT%201'
```

## Service Endpoints

| Service | Endpoint | Notes |
| --- | --- | --- |
| Kafka | `localhost:9092` | Broker dùng cho application chạy ngoài Docker |
| MinIO API | `http://localhost:9000` | S3-compatible endpoint |
| MinIO Console | `http://localhost:9001` | Local console, `minioadmin/minioadmin` |
| Hive Metastore | `thrift://localhost:9083` | Iceberg catalog metadata |
| Trino | `http://localhost:8080` | SQL query engine |
| ClickHouse HTTP | `http://localhost:8123` | `default/clickhouse` |
| ClickHouse Native | `localhost:9002` | Native protocol mapped from container |
| Grafana | `http://localhost:3000` | `admin/admin` |

## Usage

### Clean Local Data

Để bắt đầu một lần chạy sạch, cleanup toàn bộ dữ liệu đã ingest, bao gồm Kafka
topic:

```bash
PYTHONPATH=src:. python scripts/platform/cleanup_ingested_data.py \
  --confirm-delete \
  --include-kafka-topic
```

### Run Live Streaming Phase

```bash
PYTHONPATH=src:. python scripts/e2e/run_live_pipeline.py
```

Live pipeline khởi động các process:

- Python ingestion gateway.
- Spark Bronze writer.
- Spark realtime metrics stream.
- Spark Bronze-to-Silver streaming job.

Khi đã ingest đủ dữ liệu, nhấn `Ctrl+C` để dừng live pipeline trước khi chạy Gold
analytics refresh.

### Run Gold Analytics Refresh Phase

```bash
PYTHONPATH=src:. python scripts/lakehouse/run_lakehouse_path_incremental.py \
  --live-mode \
  --ignore-state \
  --gold-mode standard
```

Các mode chính:

- `--gold-mode fast`: refresh dashboard nhanh.
- `--gold-mode standard`: thêm Trino Gold modeled check.
- `--gold-mode strict`: thêm full serving reconciliation.

### Useful Checks

```bash
PYTHONPATH=src:. python scripts/lakehouse/check_lakehouse_path.py
PYTHONPATH=src python scripts/realtime/check_clickhouse_metrics.py
PYTHONPATH=src:. python scripts/gold/check/check_serving_v1.py
```

## Dashboard

Project có hai dashboard Grafana đại diện cho hai serving paths.

### Realtime Hot Path Dashboard

Dashboard realtime đọc các bảng ClickHouse theo phút để theo dõi freshness, event
volume, content activity, engagement activity, network activity và Spark
micro-batch health.

![Bluesky realtime hot path dashboard 1](docs/assets/dashboard-realtime-hot-path-1.png)

![Bluesky realtime hot path dashboard 2](docs/assets/dashboard-realtime-hot-path-2.png)

### Gold Analytics Dashboard

Dashboard Gold analytics đọc các serving marts được refresh từ Gold modeled
Iceberg, phục vụ phân tích post performance, content quality, conversation,
actor activity, network growth và data quality.

![Bluesky Gold analytics dashboard 1](docs/assets/dashboard-gold-analytics-1.png)

![Bluesky Gold analytics dashboard 2](docs/assets/dashboard-gold-analytics-2.png)

![Bluesky Gold analytics dashboard 3](docs/assets/dashboard-gold-analytics-3.png)

![Bluesky Gold analytics dashboard 4](docs/assets/dashboard-gold-analytics-4.png)

![Bluesky Gold analytics dashboard 5](docs/assets/dashboard-gold-analytics-5.png)

## Testing and Data Quality

Chạy unit tests:

```bash
PYTHONPATH=src pytest
```

Các checkpoint dữ liệu chính:

- Silver Iceberg row count và schema checks.
- Trino query checks cho Silver/Gold Iceberg.
- Gold fact/dimension key quality checks.
- Gold serving reconciliation giữa staging/Gold modeled và ClickHouse.
- Realtime metrics summary và batch health checks.

## Design Decisions

- **Kafka là short-term event log**, không phải historical source of truth.
- **Bronze dùng Parquet**, vì tầng này chủ yếu append, audit và replay.
- **Silver và Gold modeled dùng Iceberg**, vì cần table semantics, schema
  evolution, snapshot và query qua Trino.
- **ClickHouse là serving layer**, không phải source of truth duy nhất.
- **Realtime fast path và lakehouse path tách mục tiêu**: latency thấp cho
  operational dashboard, correctness/rebuildability cho analytics.
- **Gold refresh dùng incremental batch/micro-batch**, không full rebuild trong
  vận hành thường xuyên.
- **Airflow chưa nằm trong MVP**, nhưng phù hợp cho roadmap orchestration.

## Current Limitations

- Phiên bản hiện tại chạy trên môi trường local, chưa phải production deployment.
- Spark chạy ở `local[2]`; production nên dùng Spark Standalone, Kubernetes hoặc
  resource manager tương đương.
- Local runtime chạy theo hai phase độc lập để tránh oversubscribe tài nguyên
  WSL/local.
- Gold incremental state hiện lưu bằng local JSON trong `data/state/`.
- Project chưa tuyên bố exactly-once end-to-end.
- Network growth là observed network activity từ public stream đã ingest, không
  phải follower count toàn cục của Bluesky.
- Production monitoring cần bổ sung Prometheus metrics, Kafka consumer lag,
  alerting, service health, resource usage và runbook xử lý sự cố.

## Roadmap

- Airflow orchestration cho Gold incremental refresh, data quality, compaction,
  snapshot expiration, backfill và rebuild.
- Prometheus metrics, alerting và operational runbook.
- Spark Standalone hoặc cluster runtime với resource isolation rõ ràng hơn.
- Integration tests chạy với containerized infrastructure.
- Iceberg maintenance jobs như compaction và snapshot expiration.
- Extended analytics như hashtags, shared domains, language activity hoặc account
  lifecycle modeling.

## License

Repository hiện chưa có license file.

## Author

[dctk84](https://github.com/dctk84)
