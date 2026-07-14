# Distributed Bluesky Streaming Analytics Lakehouse

Project xây dựng một nền tảng xử lý dữ liệu streaming phân tán từ Bluesky
Jetstream.

Tài liệu kỹ thuật chi tiết:

- [Tổng quan dự án](docs/tong-quan-du-an.md)
- [Hướng dẫn làm việc với Codex](docs/huong-dan-lam-viec-voi-codex.md)
- [Gold data model v1](docs/gold-data-model-v1.md)
- [Gold analytics metrics v1](docs/gold-analytics-metrics-v1.md)
- [Script inventory](docs/script-inventory.md)
- [Project walkthrough khi phỏng vấn](docs/phong-van-project-walkthrough.md)

Project hiện được triển khai theo từng milestone nhỏ và có hai path phục vụ
dashboard:

- Lakehouse path: ưu tiên source of truth, rebuild và reconciliation; Bronze ->
  Silver chạy theo streaming để dữ liệu sạch được cập nhật liên tục.
- Realtime fast path: ưu tiên freshness cho dashboard gần thời gian thực.

## Cấu trúc repository

```text
src/bluesky_pipeline/
  clients/      helper giao tiếp service bên ngoài, ví dụ ClickHouse
  config/       cấu hình runtime cho Kafka, Spark, Iceberg
  schemas/      schema, table name và path contract
  state/        contract/state cho incremental refresh
  transforms/   logic biến đổi Bronze, Silver, Gold

scripts/
  discovery/    probe và phân tích dữ liệu nguồn
  ingestion/    publish sample và ghi Bronze
  e2e/          live pipeline runner cho demo local
  lakehouse/    build/check Silver, Gold modeled và lakehouse runners
  gold/
    build/      build Gold serving marts
    load/       load marts vào ClickHouse
    check/      reconciliation và validation
    refresh/    incremental refresh orchestration
    common/     helper dùng chung cho Gold scripts
  realtime/     hot-path metrics vào ClickHouse
  platform/     cleanup và setup object local
```

## Trạng thái hiện tại

Project hiện có lakehouse path local end-to-end:

```text
Jetstream
→ Kafka
→ Spark Bronze trên MinIO
→ Silver Iceberg v1 trên MinIO
→ Trino query layer
→ Gold modeled Iceberg
→ Gold analytics / aggregate marts
→ ClickHouse serving tables
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
- Silver được build trực tiếp từ Bronze qua transformation module dùng chung; live
  pipeline có streaming job đẩy Bronze mới sang Silver Iceberg.
- Trino query Silver/Gold Iceberg qua Hive Metastore catalog.
- Sau khi bật Trino/Hive Metastore lần đầu, cần build lại Silver Iceberg để các
  bảng được đăng ký vào metastore mới.
- Lakehouse Gold không chỉ là metric aggregate; project đã có contract Gold
  modeled fact/dim, bộ analytics metrics v1, incremental refresh và dashboard
  Grafana cho các serving marts chính.
- ClickHouse serving layer hiện có các bảng lakehouse serving marts:
  - `bluesky.gold_event_volume_by_type`
  - `bluesky.gold_post_engagement_summary`
  - `bluesky.gold_post_performance`
  - `bluesky.gold_content_quality_hourly`
  - `bluesky.gold_thread_conversation_summary`
  - `bluesky.gold_actor_activity_daily`
  - `bluesky.gold_network_growth_daily`
- Realtime serving layer có các bảng mart theo phút và bảng stream batch health.

## Chạy local

Khởi động hạ tầng:

```bash
docker compose up -d kafka minio hive-metastore trino clickhouse grafana
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
ICEBERG_CATALOG_TYPE=hive
ICEBERG_HIVE_METASTORE_URI=thrift://localhost:9083
```

## Demo workflow cho máy local

Với máy cá nhân/WSL, project demo theo hai phase độc lập để tránh chạy quá nhiều
Spark application cùng lúc:

```text
Phase 1 - Live streaming:
Jetstream -> Kafka -> Bronze Parquet -> Silver Iceberg
          -> realtime ClickHouse metrics -> Grafana hot dashboard

Phase 2 - Gold analytics refresh:
Silver Iceberg -> Gold modeled Iceberg -> Gold serving marts -> ClickHouse
```

Trước khi chạy một demo sạch, cleanup toàn bộ dữ liệu cũ đã ingest, bao gồm Kafka
topic nếu muốn dashboard chỉ phản ánh dữ liệu của lần chạy hiện tại:

```bash
PYTHONPATH=src:. python scripts/platform/cleanup_ingested_data.py \
  --confirm-delete \
  --include-kafka-topic
```

Sau đó chạy live streaming phase:

```bash
PYTHONPATH=src:. python scripts/e2e/run_live_pipeline.py
```

Khi đã ingest đủ dữ liệu để demo, nhấn `Ctrl+C` để dừng live pipeline. Sau đó chạy
Gold analytics refresh phase:

```bash
PYTHONPATH=src:. python scripts/lakehouse/run_lakehouse_path_incremental.py \
  --live-mode \
  --ignore-state \
  --gold-mode standard
```

Trong đó:

- `--live-mode` dùng readiness check nhẹ cho Silver Iceberg đã được live pipeline
  ghi trước đó.
- `--ignore-state` dùng cho lần bootstrap Gold sau cleanup để xử lý toàn bộ dữ
  liệu Silver hiện có. Với các lần refresh định kỳ sau đó, có thể bỏ flag này để
  chạy incremental theo state.
- `--gold-mode fast` phù hợp khi chỉ muốn refresh dashboard nhanh.
- `--gold-mode standard` thêm Trino Gold modeled check, phù hợp cho demo/chụp ảnh.
- `--gold-mode strict` thêm full serving reconciliation, phù hợp cho audit/debug.

Project không còn dùng một runner gộp hot path và Gold path chạy song song trong
cùng một terminal vì mô hình đó dễ oversubscribe tài nguyên WSL local. Trong môi
trường production, hai phase này có thể được orchestration bằng Airflow/Spark
cluster với resource isolation rõ ràng.

## Kết quả dashboard demo

Sau một clean demo run, project có hai dashboard Grafana dùng để trình bày hai
nhánh serving khác nhau của kiến trúc.

### Realtime hot path dashboard

Dashboard realtime hot path đọc các bảng ClickHouse theo phút, phục vụ quan sát
freshness, event volume, content activity, engagement, network activity và health
của Spark micro-batches.

![Bluesky realtime hot path dashboard 1](docs/assets/dashboard-realtime-hot-path-1.png)

![Bluesky realtime hot path dashboard 2](docs/assets/dashboard-realtime-hot-path-2.png)

### Gold analytics dashboard

Dashboard Gold analytics đọc các serving marts được refresh từ Gold modeled
Iceberg, phục vụ phân tích sâu hơn về post performance, content quality,
conversation, actor activity, network growth và data quality.

![Bluesky Gold analytics dashboard 1](docs/assets/dashboard-gold-analytics-1.png)

![Bluesky Gold analytics dashboard 2](docs/assets/dashboard-gold-analytics-2.png)

![Bluesky Gold analytics dashboard 3](docs/assets/dashboard-gold-analytics-3.png)

![Bluesky Gold analytics dashboard 4](docs/assets/dashboard-gold-analytics-4.png)

![Bluesky Gold analytics dashboard 5](docs/assets/dashboard-gold-analytics-5.png)

## Các lệnh chạy riêng khi debug

Chạy Spark Bronze writer:

```bash
PYTHONPATH=src python scripts/ingestion/spark_read_kafka_raw.py
```

Ở terminal khác, chạy ingestion gateway để publish live events vào Kafka:

```bash
PYTHONPATH=src MAX_EVENTS=300 python -m bluesky_pipeline.ingestion_gateway
```

Tạo ClickHouse lakehouse/realtime serving tables nếu chưa có:

```bash
PYTHONPATH=src python scripts/platform/create_clickhouse_gold_tables.py
```

## Lakehouse path

Chạy toàn bộ lakehouse path:

```bash
PYTHONPATH=src:. python scripts/lakehouse/run_lakehouse_path.py
```

Lệnh này thực hiện theo thứ tự:

- Build Silver Iceberg v1 trực tiếp từ Bronze.
- Check Silver Iceberg v1.
- Build Gold modeled v1.
- Check Trino query được Gold modeled v1.
- Refresh Gold analytics/serving marts từ lakehouse Iceberg.
- Check Gold serving v1.

Kiểm tra lakehouse path hiện có mà không build/refresh lại dữ liệu:

```bash
PYTHONPATH=src:. python scripts/lakehouse/check_lakehouse_path.py
```

Các script con vẫn có thể chạy riêng khi cần debug từng tầng:

```bash
PYTHONPATH=src python scripts/lakehouse/build_iceberg_silver_v1.py
PYTHONPATH=src python scripts/lakehouse/check_iceberg_silver_v1.py
PYTHONPATH=src python scripts/lakehouse/check_trino_silver_v1.py
PYTHONPATH=src:. python scripts/gold/refresh/refresh_serving_from_iceberg.py
PYTHONPATH=src python scripts/gold/check/check_event_volume_reconciliation.py
PYTHONPATH=src python scripts/gold/check/check_post_engagement_reconciliation.py
PYTHONPATH=src python scripts/gold/check/check_thread_conversation_summary_reconciliation.py
PYTHONPATH=src python scripts/gold/check/check_actor_activity_daily_reconciliation.py
PYTHONPATH=src python scripts/gold/check/check_network_growth_daily_reconciliation.py
PYTHONPATH=src:. python scripts/gold/check/check_serving_v1.py
```

## Realtime fast path

Chạy live pipeline từ Bluesky Jetstream tới ClickHouse/Grafana:

```bash
PYTHONPATH=src:. python scripts/e2e/run_live_pipeline.py
```

Lệnh này start ingestion gateway, Bronze writer, realtime metrics stream và
Bronze-to-Silver streaming job. Nhấn `Ctrl+C` để dừng toàn bộ process con trước
khi chạy Gold analytics refresh phase.

Terminal 1, chạy Spark streaming job ghi realtime metrics vào ClickHouse:

```bash
PYTHONPATH=src python scripts/realtime/stream_metrics_to_clickhouse.py
```

Terminal 2, publish live events vào Kafka:

```bash
PYTHONPATH=src MAX_EVENTS=300 python -m bluesky_pipeline.ingestion_gateway
```

Kiểm tra bảng realtime bằng CLI:

```bash
PYTHONPATH=src python scripts/realtime/check_clickhouse_metrics.py
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

## Known Limitations

Project này là bản MVP/portfolio chạy trên máy local, chưa phải production
deployment. Các giới hạn chính cần trình bày trung thực:

- Spark hiện chạy ở `local[2]` để phù hợp tài nguyên WSL/local. Production nên
  chạy Spark Standalone, Kubernetes hoặc một resource manager tương đương để có
  resource isolation, retry và scaling rõ ràng hơn.
- Airflow chưa được triển khai trong MVP. Gold analytics refresh đang chạy thủ
  công sau live pipeline; Airflow là hướng mở rộng để schedule Gold incremental,
  data quality, compaction, snapshot expiration và backfill.
- Demo local chạy theo hai phase độc lập: live streaming trước, dừng pipeline,
  rồi chạy Gold analytics refresh. Cách này giúp tránh oversubscribe tài nguyên
  local, nhưng production nên tách tài nguyên và orchestration thay vì phụ thuộc
  thao tác thủ công.
- State của Gold incremental hiện lưu bằng local JSON trong `data/state/`. Cách
  này đủ cho học tập/demo, nhưng production nên dùng durable metadata store hoặc
  orchestrator state.
- Project chưa tuyên bố exactly-once end-to-end. Luồng hiện tại có checkpoint,
  deterministic keys và reconciliation, nhưng ClickHouse serving writes vẫn cần
  được xem theo hướng at-least-once/idempotent-by-design tùy từng mart.
- Network growth là observed network activity từ public stream đã ingest, không
  phải follower count toàn cục của Bluesky. Một số delete/follow event có thể
  thiếu context nếu create event tương ứng không nằm trong dữ liệu đã observe.
- Dashboard operational hiện tại đủ cho demo local, nhưng production monitoring
  cần bổ sung Prometheus metrics, Kafka consumer lag, alerting, service health,
  resource usage và runbook xử lý sự cố.

## Hướng phát triển tiếp theo

### Orchestration bằng Airflow

Trong MVP local hiện tại, Gold analytics refresh được chạy thủ công sau khi dừng
live streaming pipeline:

```bash
PYTHONPATH=src:. python scripts/lakehouse/run_lakehouse_path_incremental.py \
  --live-mode \
  --gold-mode standard
```

Trong phiên bản production-like tiếp theo, Airflow có thể được thêm vào để
orchestrate các workflow hữu hạn:

- Gold incremental refresh từ Silver Iceberg sang Gold modeled.
- Refresh Gold serving marts vào ClickHouse.
- Data quality checks và reconciliation.
- Iceberg compaction.
- Snapshot expiration.
- Backfill hoặc rebuild khi cần replay dữ liệu.

Airflow không xử lý từng streaming event và không thay thế Kafka/Spark Structured
Streaming trên hot path. Vai trò của Airflow là lập lịch, retry, theo dõi lịch sử
chạy và điều phối các job batch/incremental có điểm bắt đầu và kết thúc rõ ràng.
