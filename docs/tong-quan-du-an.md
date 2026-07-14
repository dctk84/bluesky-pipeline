# Tổng quan dự án

## 1. Tên dự án

**Distributed Bluesky Streaming Analytics Lakehouse**

Tên mô tả ngắn:

> Nền tảng xử lý dữ liệu streaming phân tán từ Bluesky Jetstream, sử dụng Kafka,
> Spark Structured Streaming, S3-compatible Data Lake, Apache Iceberg, Trino và
> ClickHouse.

---

## 2. Bối cảnh

Dự án được xây dựng nhằm phục vụ mục tiêu học tập và portfolio cho vị trí
Data Engineer/Data Platform Engineer.

Người thực hiện đã có một project batch Data Warehouse với các nội dung:

- Ingest dữ liệu từ API và file.
- Incremental loading.
- Python ETL.
- SQL Server Data Warehouse.
- Dimensional modeling.
- Fact và dimension tables.
- SCD Type 1.
- Power BI.

Project hiện tại phải bổ sung các năng lực mà project batch chưa chứng minh rõ:

- Continuous streaming ingestion.
- Message broker và event log.
- Xử lý dữ liệu phân tán.
- Stateful stream processing.
- Event-time processing.
- S3-compatible object storage.
- Data Lake và Lakehouse.
- Analytical serving database.
- Checkpoint, replay và failure recovery.
- Monitoring, data quality và platform operations.

Project không được lặp lại bài toán batch order processing dưới một công nghệ khác.

---

## 3. Bài toán

Bluesky phát sinh liên tục các sự kiện công khai như:

- Tạo bài viết.
- Cập nhật bài viết.
- Xóa bài viết.
- Like.
- Repost.
- Follow.

Project xây dựng một nền tảng có khả năng:

1. Kết nối liên tục tới Bluesky Jetstream qua WebSocket.
2. Tiếp nhận và buffer sự kiện bằng Apache Kafka.
3. Xử lý dữ liệu bằng Spark Structured Streaming.
4. Lưu dữ liệu lịch sử trên S3-compatible object storage.
5. Tổ chức dữ liệu theo các tầng Bronze, Silver và Gold.
6. Quản lý các bảng Silver Lakehouse bằng Apache Iceberg.
7. Query dữ liệu lakehouse bằng Trino cho ad-hoc analytics.
8. Phục vụ truy vấn gần thời gian thực bằng ClickHouse.
9. Hiển thị business metrics và platform metrics qua Grafana.
10. Có hướng mở rộng để orchestration backfill, data quality, compaction và
    rebuild bằng Airflow.
11. Theo dõi độ trễ, throughput, consumer lag và lỗi pipeline.

---

## 4. Mục tiêu kỹ thuật

### 4.1. Streaming ingestion

Hệ thống phải tiếp nhận sự kiện liên tục thay vì chạy theo schedule.

Cần chứng minh:

- WebSocket connection dài hạn.
- Reconnect khi nguồn bị ngắt.
- Bounded retry.
- Graceful shutdown.
- Không giữ queue vô hạn trong memory.
- Không làm mất kiểm soát khi downstream chậm.

### 4.2. Event streaming với Kafka

Kafka đóng vai trò:

- Buffer giữa nguồn bên ngoài và compute engine.
- Tách ingestion gateway khỏi Spark.
- Lưu event theo retention ngắn hạn.
- Cho phép replay từ offset.
- Phân phối dữ liệu theo partition.
- Hỗ trợ nhiều consumer group độc lập.
- Theo dõi consumer lag.

Kafka không phải kho lưu trữ lịch sử dài hạn của project.

### 4.3. Distributed processing với Spark

Spark Structured Streaming được sử dụng để:

- Parse event schema.
- Validate dữ liệu.
- Chuẩn hóa nhiều loại event.
- Deduplicate event.
- Xử lý event time.
- Xử lý late và out-of-order events.
- Thực hiện window aggregation.
- Thực hiện stateful processing.
- Ghi raw data vào Bronze Parquet trên MinIO.
- Ghi dữ liệu chuẩn hóa vào Silver Iceberg.
- Tạo aggregate phục vụ ClickHouse.

Trong target production-like, Spark nên chạy qua Spark Standalone hoặc một
resource manager tương đương để chứng minh distributed execution và resource
isolation. Trong MVP local hiện tại, project dùng `SPARK_MASTER=local[2]` để phù
hợp tài nguyên WSL và tránh oversubscribe khi chạy nhiều Spark application.

Trong lakehouse path, Spark là compute engine chính cho các đoạn xử lý dữ liệu:

```text
Bronze / raw lake -> Spark -> Silver Iceberg
Silver Iceberg -> Spark -> Gold modeled tables
Gold modeled tables -> Trino -> ad-hoc analytics
Gold modeled tables -> Spark -> Gold aggregate / serving marts -> ClickHouse
```

ClickHouse không xử lý dữ liệu gốc thay Spark; ClickHouse nhận các bảng aggregate
hoặc mart đã được Spark chuẩn bị để phục vụ truy vấn dashboard có độ trễ thấp.

Yêu cầu latency của từng lớp không giống nhau:

- Bronze được ghi liên tục từ Kafka để giữ raw history gần nguồn.
- Silver Iceberg là lớp dữ liệu sạch/chuẩn hóa nên Bronze -> Silver cần chạy theo
  streaming/continuous transform để dữ liệu sạch được cập nhật liên tục.
- Gold modeled tables là nơi data modeling cho lakehouse path, ví dụ fact/dim
  hoặc semantic marts phục vụ phân tích sâu.
- Gold aggregate/serving marts tính metric từ Gold modeled tables và có thể chấp
  nhận latency cao hơn Silver, ví dụ theo giờ hoặc theo lịch Airflow.

### 4.4. Storage architecture

MinIO được sử dụng làm S3-compatible object storage trong môi trường local.

Dữ liệu được tổ chức theo các tầng với công nghệ phù hợp cho từng mục đích:

- Bronze: dữ liệu gần nguồn, lưu dạng Parquet partitioned trên MinIO.
- Silver: dữ liệu đã chuẩn hóa và kiểm tra, quản lý bằng Apache Iceberg trên
  MinIO, được cập nhật liên tục từ Bronze để làm lakehouse serving dataset.
- Gold modeled: dữ liệu đã được thiết kế mô hình phân tích, ví dụ fact/dim hoặc
  semantic marts, có thể lưu trên Iceberg để phục vụ ad-hoc analytics và làm
  nguồn tính metric.
- Gold aggregate/serving: dữ liệu metric hoặc mart đã tổng hợp phục vụ dashboard,
  lưu trong ClickHouse để truy vấn độ trễ thấp.

Bronze chưa cần dùng Iceberg ngay lập tức vì tầng này chủ yếu phục vụ append,
audit và replay. Thiết kế đơn giản bằng Parquet partitioned giúp dễ quan sát dữ
liệu và giảm độ phức tạp ở giai đoạn đầu.

Apache Iceberg được dùng từ Silver vì tầng này cần table semantics, snapshot,
schema evolution, deduplication và xử lý thay đổi dữ liệu rõ ràng hơn.

ClickHouse đóng vai trò serving/metric store cho truy vấn dashboard có độ trễ
thấp. ClickHouse không phải toàn bộ tầng Gold và không phải source of truth duy
nhất; các bảng serving trong ClickHouse phải có thể được rebuild từ Gold modeled
tables hoặc từ Silver khi cần.

### 4.5. Query engine layer

Trino được sử dụng làm query engine cho lakehouse path.

Trino phục vụ:

- Query SQL trực tiếp lên các bảng Iceberg.
- Ad-hoc analytics trên Silver và Gold modeled tables.
- Kiểm tra dữ liệu lakehouse mà không cần viết Spark job riêng cho từng câu hỏi.
- Demo rõ lớp Query Engine Layer của kiến trúc lakehouse hiện đại.

Trino không thay thế Spark. Spark vẫn là compute engine cho streaming,
transformation, backfill và build các bảng Silver/Gold. Trino cũng không thay thế
ClickHouse. ClickHouse vẫn là serving/metric store cho dashboard có latency thấp.

### 4.6. Analytical serving

ClickHouse được sử dụng làm serving layer cho:

- Dashboard gần thời gian thực.
- Query aggregate có độ trễ thấp.
- Business metrics.
- Platform metrics đã tổng hợp.

ClickHouse không phải source of truth duy nhất.

Các bảng ClickHouse phải có khả năng rebuild từ Gold modeled tables hoặc từ
Silver Iceberg khi cần.

### 4.7. Platform operations

Airflow là hướng orchestration dự kiến cho các workflow có điểm bắt đầu và kết
thúc rõ ràng:

- Lakehouse backfill.
- Gold lakehouse modeling từ Silver Iceberg.
- Gold aggregate/serving refresh từ Gold modeled tables sang ClickHouse.
- Data quality checks.
- Reconciliation.
- Iceberg compaction.
- Snapshot expiration.
- ClickHouse incremental load.
- ClickHouse rebuild.

Airflow không được sử dụng để xử lý từng streaming event.
Airflow cũng không thay thế live ingestion gateway hoặc Spark streaming consumer.
Trong MVP local hiện tại, Airflow chưa được triển khai; Gold analytics refresh
được chạy thủ công sau live pipeline. Trong milestone tiếp theo, Airflow phù hợp
để schedule các job hữu hạn như `Silver Iceberg -> Gold modeled tables`, chạy
Trino validation/query checkpoint, rồi `Gold modeled tables -> Gold
aggregate/serving marts -> ClickHouse` và các checkpoint sau load.

### 4.8. Observability

Hệ thống cần theo dõi:

- Logs.
- Metrics.
- Alerts.
- Data freshness.
- Consumer lag.
- Processing latency.
- Invalid event rate.
- Pipeline throughput.
- Service availability.

Prometheus thu thập metrics.

Grafana hiển thị:

- Technical dashboard.
- Business analytics dashboard.

---

## 5. Kiến trúc tổng thể

```text
Bluesky Jetstream
        │
        │ WebSocket JSON events
        ▼
┌─────────────────────────────┐
│ Python Ingestion Gateway    │
│                             │
│ - Connect WebSocket         │
│ - Reconnect                 │
│ - Minimal validation        │
│ - Add ingestion metadata    │
│ - Publish to Kafka          │
└──────────────┬──────────────┘
               │
               ▼
┌─────────────────────────────┐
│ Apache Kafka                │
│                             │
│ - Raw event topics          │
│ - Partitioning              │
│ - Offset management         │
│ - Retention                 │
│ - Consumer groups           │
│ - Replay                    │
└───────┬─────────────────────┘
        │
        ├─────────────────────────────────────────────────────────────┐
        │                                                             │
        │ Fast path: near-real-time dashboard                          │
        ▼                                                             │
┌─────────────────────────────┐                                       │
│ Spark Structured Streaming  │                                       │
│                             │                                       │
│ - Parse event envelope      │                                       │
│ - Window aggregation        │                                       │
│ - Micro-batch checkpoint    │                                       │
└──────────────┬──────────────┘                                       │
               │                                                      │
               ▼                                                      │
┌─────────────────────────────┐                                       │
│ ClickHouse realtime marts   │                                       │
│                             │                                       │
│ - Event volume by minute    │                                       │
│ - Low-latency dashboard     │                                       │
└──────────────┬──────────────┘                                       │
               │                                                      │
               ▼                                                      │
            Grafana                                                   │
                                                                      │
        Lakehouse path                                                │
        ▼                                                             │
┌─────────────────────────────┐                                       │
│ Spark Structured Streaming  │                                       │
│                             │                                       │
│ - Parse                     │                                       │
│ - Validate                  │                                       │
│ - Deduplicate               │                                       │
│ - Watermark                 │                                       │
│ - Event-time window         │                                       │
│ - Stateful aggregation      │                                       │
│ - Enrichment                │                                       │
└──────────────┬──────────────┘                                       │
               │ Raw append data                                      │
               ▼                                                      │
┌─────────────────────────────┐                                       │
│ Bronze data lake            │                                       │
│ MinIO + partitioned Parquet │                                       │
└──────────────┬──────────────┘                                       │
               │ Clean and normalized data                            │
               ▼                                                      │
┌─────────────────────────────┐                                       │
│ Silver Lakehouse            │                                       │
│ MinIO + Apache Iceberg      │                                       │
└──────────────┬──────────────┘                                       │
               │ Modeled analytical data                              │
               ▼                                                      │
┌─────────────────────────────┐                                       │
│ Gold modeled lakehouse      │                                       │
│ MinIO + Apache Iceberg      │                                       │
│                             │                                       │
│ - Fact tables               │                                       │
│ - Dimension tables          │                                       │
│ - Semantic marts            │                                       │
└──────────────┬──────────────┘                                       │
               │ SQL / ad-hoc analytics                              │
               ├──────────────▶ Trino query engine                    │
               │                - Query Iceberg                       │
               │                - Analyst SQL                         │
               │ Aggregates / serving marts                           │
               ▼                                                      │
┌─────────────────────────────┐                                       │
│ ClickHouse lakehouse marts  │                                       │
│                             │                                       │
│ - Baseline aggregates       │                                       │
│ - Rebuildable serving data  │                                       │
└──────────────┬──────────────┘                                       │
               │                                                      │
               ▼                                                      │
            Grafana                                                   │

┌─────────────────────────────┐
│ Apache Airflow (future)     │
│                             │
│ - Backfill                  │
│ - Data quality              │
│ - Compaction                │
│ - Reconciliation            │
│ - ClickHouse rebuild        │
└─────────────────────────────┘
```

Prometheus thu thập metrics từ các service và cung cấp dữ liệu cho Grafana.

Kiến trúc dashboard có hai luồng phục vụ khác nhau:

- **Fast path** đọc trực tiếp từ Kafka bằng Spark Structured Streaming, aggregate
  theo micro-batch và ghi vào ClickHouse realtime marts. Luồng này phục vụ các
  chỉ số cần cập nhật gần thời gian thực trên Grafana. Gần thời gian thực nghĩa là
  vẫn có độ trễ từ trigger interval của Spark, thời gian ghi ClickHouse và chu kỳ
  refresh của Grafana.
- **Lakehouse path** ghi dữ liệu qua Bronze và Silver Iceberg trước
  khi tạo Gold modeled tables. Trino query trực tiếp các bảng Iceberg để phục vụ
  ad-hoc analytics. Từ Gold modeled, Spark hoặc các job được Airflow schedule sẽ
  tính aggregate/serving marts và load vào ClickHouse. Luồng này là nền tảng cho
  backfill, rebuild, reconciliation và phân tích chuyên sâu. Trong path này,
  Bronze và Silver là các tầng streaming/continuous; Gold modeled và Gold
  aggregate refresh có thể chạy thưa hơn bằng Airflow.
- Fast path không thay thế Silver Iceberg. ClickHouse realtime marts là serving
  table cho dashboard, không phải source of truth duy nhất.
- Prometheus và Grafana technical dashboard phục vụ observability như lag,
  freshness, throughput và service health; luồng này tách với business analytics.

### 5.1. Phân loại kiến trúc

Kiến trúc của project là **lambda-like streaming lakehouse architecture** cho bài
toán streaming analytics.

Project có hai path cùng đọc từ Kafka:

```text
Kafka topic
├── Consumer group 1: lakehouse_ingestion
│   └── Spark -> Bronze/Silver/Gold Iceberg -> Trino -> analytics
└── Consumer group 2: realtime_metrics
    └── Spark -> ClickHouse -> Grafana operational dashboard
```

Fast path đi từ Kafka qua Spark Structured Streaming tới ClickHouse realtime marts
để phục vụ metric có freshness thấp. Đây là **operational serving view**: nhanh
hơn, ít tầng hơn, phù hợp quan sát tức thời, nhưng không phải single source of
truth.

Lakehouse path lưu dữ liệu vào Bronze, Silver Iceberg và Gold modeled Iceberg.
Trino là query engine để analyst hoặc người vận hành query trực tiếp lakehouse
tables. Các metric phục vụ Grafana được tính từ Gold modeled thành
aggregate/serving marts rồi load vào ClickHouse. Kafka là short-term replay log;
source of truth phân tích nằm ở Bronze Parquet và các bảng Silver/Gold Iceberg
trên lakehouse.

Vì có hai consumer/job khác nhau cùng đọc Kafka, dữ liệu giữa hai path có thể lệch
tạm thời. Các nguyên nhân thường gặp:

- Một job xử lý trước, job kia còn lag.
- Logic transform giữa fast path và lakehouse path khác nhau.
- Late event hoặc out-of-order event.
- Duplicate event hoặc replay từ Kafka.
- Retry khi ghi ClickHouse làm một micro-batch có thể được gửi lại.
- Watermark hoặc schema thay đổi.
- Một job fail trong khi job còn lại vẫn chạy.

Đây không phải lỗi thiết kế, nhưng phải được kiểm soát bằng checkpoint,
reconciliation, data freshness metrics và quy tắc rebuild rõ ràng.

Thiết kế này không còn nên gọi là Kappa-like. Kappa Architecture thuần thường dựa
trên một stream processing path duy nhất và replay từ log để rebuild state. Project
này có lakehouse path riêng với Bronze/Silver/Gold Iceberg, Trino cho ad-hoc
analytics và ClickHouse cho serving, nên mô tả chính xác hơn là lambda-like. Điểm
giống Kappa còn lại chỉ là Kafka đóng vai trò event backbone chung và Spark được
tái sử dụng ở nhiều đoạn xử lý.

---

## 6. Tech stack

### 6.1. Ngôn ngữ và công cụ phát triển

- Python 3.12.2.
- SQL.
- Bash cơ bản.
- Git.
- Docker.
- Docker Compose.
- WSL Ubuntu.

### 6.2. Ingestion

- Python `asyncio`.
- WebSocket client.
- Kafka producer client.
- Structured logging.
- Environment-based configuration.

### 6.3. Event streaming

- Apache Kafka.
- Kafka topics.
- Kafka partitions.
- Kafka message keys.
- Kafka offsets.
- Consumer groups.
- Retention.
- Replay.

### 6.4. Distributed compute

- Apache Spark.
- PySpark.
- Spark SQL.
- Spark Structured Streaming.
- Spark local mode cho MVP hiện tại; Spark Standalone cluster là hướng mở rộng.
- Spark UI.
- Spark History Server.

### 6.5. Storage

- MinIO.
- S3-compatible API.
- Parquet.
- Apache Iceberg.
- Iceberg catalog.

### 6.6. Query engine

- Trino.
- Iceberg connector.
- SQL ad-hoc analytics trên lakehouse tables.

### 6.7. Serving

- ClickHouse.
- MergeTree-family engines.
- Materialized views khi có use case.
- Grafana.

### 6.8. Orchestration

- Apache Airflow cho milestone orchestration tiếp theo.

### 6.9. Monitoring

- Prometheus.
- Grafana.
- Structured application logs.

### 6.10. Testing và code quality

- pytest.
- ruff.
- mypy khi phù hợp.
- Integration tests.
- Docker health checks.

---

## 7. Cấu trúc repository

Repository cần được tổ chức rõ ràng theo vai trò của từng nhóm file để project dễ
mở rộng, dễ review và gần với cách làm thực tế.

Cấu trúc repository không cố định ngay từ đầu. Ở giai đoạn đầu, project có thể giữ
cấu trúc đơn giản. Khi số lượng file tăng lên, repository phải được tách dần theo
trách nhiệm hoặc tầng xử lý.

Cấu trúc hiện tại:

```text
bluesky-pipeline/
├── docs/
│   ├── huong-dan-lam-viec-voi-codex.md
│   ├── tong-quan-du-an.md
│   └── jetstream-schema-notes.md
├── src/
│   └── bluesky_pipeline/
│       ├── clients/
│       ├── config/
│       ├── schemas/
│       ├── state/
│       ├── transforms/
│       └── ingestion_gateway.py
├── scripts/
│   ├── discovery/
│   ├── ingestion/
│   ├── e2e/
│   ├── lakehouse/
│   ├── gold/
│   │   ├── build/
│   │   ├── load/
│   │   ├── check/
│   │   ├── refresh/
│   │   └── common/
│   ├── realtime/
│   └── platform/
├── tests/
│   └── test_*.py
├── data/
│   └── probe/
│       └── *.jsonl
├── requirements.txt
└── .gitignore
```

Vai trò:

- `src/bluesky_pipeline/`: code Python có thể dùng lại trong pipeline.
- `scripts/`: entrypoint và script chạy tay, được tách theo vai trò như
  discovery, ingestion, lakehouse, gold, realtime và platform.
- `tests/`: test cho các module có logic đáng kiểm chứng.
- `docs/`: tài liệu thiết kế, phạm vi dự án, ghi chú schema và quyết định kỹ
  thuật.
- `data/`: dữ liệu local sinh ra khi chạy probe hoặc sample; không commit.
- `requirements.txt`: dependency Python cho milestone hiện tại.

Package `src/bluesky_pipeline/` được tách theo các nhóm trách nhiệm chính:

```text
src/bluesky_pipeline/
├── clients/
├── config/
├── schemas/
├── state/
└── transforms/
```

Ý nghĩa:

- `clients/`: helper giao tiếp service bên ngoài, ví dụ ClickHouse HTTP API.
- `config/`: đọc cấu hình Kafka, Spark và Iceberg từ environment/local config.
- `schemas/`: schema, table name, path contract và DDL dùng chung.
- `state/`: contract/state cho incremental refresh.
- `transforms/`: logic biến đổi dữ liệu Bronze, Silver, Gold và event envelope.

Quy ước:

- Không commit dữ liệu trong `data/`.
- Không đặt sample JSONL trong `src/`.
- Không để script discovery phát triển lẫn với logic pipeline lâu dài. Khi số
  lượng script chạy tay tăng lên, cần tách sang thư mục riêng như `scripts/`.
- Chỉ tạo thư mục mới khi có vai trò rõ ràng trong milestone hiện tại.
- Ưu tiên cấu trúc đơn giản trước, chỉ tách module/thư mục khi số lượng file hoặc
  độ phức tạp thật sự tăng.

---

## 8. Vai trò của từng thành phần

### 8.1. Bluesky Jetstream

Vai trò:

- Nguồn streaming công khai.
- Phát sự kiện liên tục qua WebSocket.
- Tạo dữ liệu thật thay vì sử dụng dữ liệu simulator.

Project sử dụng Jetstream cho mục đích:

- Streaming analytics.
- Informal metrics.
- Data engineering demonstration.

Project không coi Jetstream là authoritative archive.

### 8.2. Ingestion Gateway

Gateway chỉ chịu trách nhiệm vận chuyển dữ liệu từ Jetstream sang Kafka.

Gateway không thực hiện:

- Business aggregation.
- Trending calculation.
- Sentiment analysis.
- Deduplication phức tạp.
- Silver/Gold transformation.

Gateway thực hiện:

- Kết nối WebSocket.
- Filter collection.
- Reconnect.
- Bounded exponential backoff.
- Thêm `received_at`.
- Tạo event envelope.
- Chọn Kafka key.
- Publish event.
- Ghi metrics và logs.

### 8.3. Kafka

Kafka là event backbone của platform.

Kafka giải quyết:

- Decoupling.
- Buffering.
- Short-term replay.
- Partitioned parallelism.
- Consumer isolation.
- Downstream recovery.

### 8.4. Spark

Spark là compute engine chính.

Spark giải quyết:

- Distributed transformation.
- Stateful aggregation.
- Window processing.
- Event-time semantics.
- Late data.
- Deduplication.
- Batch backfill từ Bronze Parquet hoặc Silver Iceberg.

### 8.5. MinIO

MinIO là môi trường local thay thế S3.

MinIO giải quyết:

- Persistent object storage.
- Long-term event history.
- Replay và backfill.
- Separation of compute and storage.
- Storage layout experiments.

### 8.6. Parquet

Parquet là file format.

Parquet được sử dụng vì:

- Columnar storage.
- Compression.
- Column pruning.
- Predicate pushdown.
- Phù hợp analytical workload.

### 8.7. Iceberg

Iceberg là table format.

Iceberg không thay thế MinIO hoặc Parquet.

```text
MinIO   = object storage
Parquet = file format
Iceberg = table format
Spark   = compute engine
Trino   = query engine
```

### 8.8. Trino

Trino là query engine của lakehouse path.

Trino phục vụ:

- Query SQL trực tiếp trên Silver và Gold modeled Iceberg tables.
- Ad-hoc analytics và kiểm tra dữ liệu bằng SQL.
- Demo lớp query engine của kiến trúc lakehouse.

Trino không ghi thay Spark và không phục vụ dashboard realtime thay ClickHouse.
Spark vẫn build/refresh dữ liệu; ClickHouse vẫn giữ metric serving marts cho
Grafana.

### 8.9. ClickHouse

ClickHouse là analytical serving database.

Nó phục vụ:

- Dashboard.
- Near-real-time metrics.
- Low-latency analytical queries.
- Metric marts đã được pre-aggregate hoặc denormalize để query nhanh.

ClickHouse không phải source of truth của lakehouse. Nếu mất dữ liệu serving,
pipeline phải rebuild được từ Gold modeled Iceberg hoặc Silver Iceberg.

### 8.10. Airflow

Airflow orchestration các batch workflow hữu hạn.

Airflow không chạy continuous consumer.

### 8.11. Prometheus và Grafana

Prometheus lưu technical metrics.

Grafana hiển thị:

- Business dashboard.
- Platform dashboard.
- SLI/SLO dashboard nếu được triển khai.

---

## 8. Phạm vi dữ liệu

### 8.1. Phạm vi nguồn hoàn chỉnh

Project tập trung vào các event công khai chính của Bluesky Jetstream để bao phủ
bốn nhóm tín hiệu:

- Content activity.
- Engagement activity.
- Network activity.
- Account lifecycle activity.

Các commit collection thuộc scope hoàn chỉnh:

```text
app.bsky.feed.post
app.bsky.feed.like
app.bsky.feed.repost
app.bsky.graph.follow
```

Các operation cần quan sát:

```text
create
update
delete
```

Không phải collection nào cũng có đủ cả ba operation. Milestone khám phá schema
phải xác nhận operation thực tế của từng collection trước khi thiết kế Silver.

Ngoài commit event, project cũng đưa vào scope các non-commit event sau:

```text
identity
account
```

Các event này không có `commit.collection` hoặc `commit.operation`, nên không được
ép vào cùng schema/layout với post, like, repost và follow. Chúng phục vụ nhóm bài
toán account lifecycle như thay đổi identity, account activation/deactivation và
tỷ lệ account lifecycle activity so với commit activity.

### 8.2. Thứ tự triển khai

Triển khai theo thứ tự để tránh xử lý quá nhiều schema cùng lúc:

1. Probe `app.bsky.feed.post` để xác nhận kết nối và shape event cơ bản.
2. Probe thêm `app.bsky.feed.like`, `app.bsky.feed.repost` và
   `app.bsky.graph.follow`.
3. Ingest raw multi-collection event vào Kafka/Bronze.
4. Bổ sung `event_kind` để phân biệt `commit`, `identity` và `account`.
5. Tách Bronze output theo event family để mỗi nhóm có schema/layout phù hợp.
6. Normalize từng collection hoặc event family trong Spark theo use case.

### 8.3. Derived signals

Một số dữ liệu phục vụ analytics không cần lấy từ collection riêng mà được trích
xuất từ post record trong Spark:

```text
hashtag
shared_domain
language
is_reply
is_quote
text_length
```

Các tín hiệu này phục vụ:

- Trending hashtags.
- Top shared domains.
- Reply/quote ratio.
- Language activity.
- Content volume theo thời gian.

### 8.4. Use cases theo nhóm dữ liệu

Content events phục vụ:

- Post volume.
- Create/update/delete activity.
- Hashtag và shared domain analytics.

Engagement events phục vụ:

- Like/repost volume.
- Engagement velocity.
- Rapidly growing posts.

Network events phục vụ:

- Follow events per minute.
- Network activity trend.
- Active repositories theo follow activity.

Account lifecycle events phục vụ:

- Identity event volume theo thời gian.
- Account activation/deactivation volume.
- Tỷ lệ account lifecycle events so với commit activity.
- Tín hiệu hỗ trợ governance và audit khi account thay đổi trạng thái.

---

## 9. Event envelope

Gateway phải giữ payload nguồn gần như nguyên vẹn và bổ sung metadata ingest.

Ví dụ logic:

```json
{
  "schema_version": 1,
  "source": "bluesky_jetstream",
  "event_kind": "commit",
  "received_at": "2026-07-02T12:30:15.123Z",
  "collection": "app.bsky.feed.post",
  "operation": "create",
  "repository_did": "did:plc:example",
  "payload": {
    "original": "Jetstream event"
  }
}
```

Nguyên tắc:

- `payload` giữ dữ liệu nguồn.
- Không sửa dữ liệu nguồn trong ingestion gateway.
- Metadata platform được đặt ngoài `payload`.
- Envelope có `schema_version`.
- Envelope có `event_kind` để phân biệt commit event với non-commit event như
  `identity` và `account`.
- Timestamp dùng UTC.
- Không hard-code schema business quá sớm.

---

## 10. Thiết kế Kafka ban đầu

### 10.1. Topic MVP

```text
bluesky.raw.events.v2
```

Topic dùng cho raw Jetstream events thuộc scope hiện tại.

Topic riêng theo collection chỉ được tạo khi có use case rõ ràng, ví dụ cần
retention, partitioning hoặc consumer isolation khác nhau.

### 10.2. Message key

Kafka key dự kiến:

```text
repository DID
```

Mục tiêu:

- Các operation thuộc cùng repository có khả năng vào cùng partition.
- Giữ ordering tương đối trong phạm vi key.
- Phân phối dữ liệu giữa nhiều partition.

Không sử dụng hashtag làm partition key vì hashtag nổi bật có thể tạo hot partition.

### 10.3. Consumer groups dự kiến

```text
spark-post-processing
raw-archive
platform-metrics
```

Không cần triển khai tất cả consumer group ngay trong milestone Kafka đầu tiên.

---

## 11. Xử lý thời gian

Hệ thống phân biệt:

### Event time

Thời điểm sự kiện xảy ra tại nguồn.

### Ingestion time

Thời điểm gateway nhận event.

### Processing time

Thời điểm Spark xử lý event.

Các cột dự kiến:

```text
event_time
received_at
processed_at
```

Các metric có thể tính:

```text
source_to_gateway_delay
gateway_to_processing_delay
end_to_end_latency
```

---

## 12. Late, duplicate và out-of-order events

Project phải xem xét:

- Event đến muộn.
- Event đến không đúng thứ tự.
- Event bị gửi lại.
- Event được replay từ Kafka.
- Update hoặc delete đến khi state cũ chưa tồn tại trong serving layer.

Chiến lược dự kiến:

```text
At-least-once processing
+
Deduplication
+
Idempotent writes
```

Project không được tuyên bố exactly-once end-to-end nếu chưa chứng minh đầy đủ source,
processing và sink semantics.

Deduplication key phải được xác định từ các trường thực tế của Jetstream sau khi đã
khảo sát event schema.

Với Spark Structured Streaming ghi thẳng sang ClickHouse bằng `foreachBatch`, cần
hiểu mặc định là at-least-once ở sink. Spark có `batchId`, nhưng chính pipeline
phải dùng `batchId` hoặc khóa logic để tránh ghi trùng nếu micro-batch bị retry.
Với bảng metric theo window, khóa logic nên có dạng:

```text
window_start + metric_name + dimension
```

Ví dụ:

```text
2026-07-09 10:01:00 | event_count | post_created
2026-07-09 10:01:00 | event_count | like_created
2026-07-09 10:02:00 | error_count | api_error
```

Không nên chỉ append mù vào ClickHouse mà không có cơ chế deduplicate hoặc
reconciliation, vì Spark retry một micro-batch có thể khiến ClickHouse nhận lại
cùng một batch.

---

## 13. Spark Structured Streaming

Spark Structured Streaming dự kiến thực hiện:

1. Đọc từ Kafka.
2. Parse event envelope.
3. Parse source payload.
4. Validate required fields.
5. Chuyển invalid event sang quarantine hoặc DLQ.
6. Tạo event-time column.
7. Áp dụng watermark.
8. Deduplicate.
9. Chuẩn hóa post records.
10. Extract hashtag và shared domain khi cần.
11. Tạo window aggregates.
12. Ghi raw events vào Bronze Parquet trên MinIO.
13. Ghi dữ liệu chuẩn hóa vào Silver Iceberg.
14. Tạo serving aggregates cho ClickHouse hoặc để Airflow load.

### Transformations stateless

- Select.
- Filter.
- Parse.
- Normalize.
- Add derived columns.

### Transformations stateful

- Window aggregation.
- Deduplication theo watermark.
- Trending calculation.
- Engagement velocity.
- Stream-stream join nếu được bổ sung sau.

---

## 14. Thiết kế Data Lake

### 14.1. Bronze layer

Mục tiêu:

- Giữ dữ liệu gần nguồn.
- Audit.
- Replay.
- Reprocess.

Các trường dự kiến:

```text
raw_payload
schema_version
source
event_kind
collection
operation
repository_did
event_time
received_at
kafka_topic
kafka_partition
kafka_offset
ingest_date
ingest_hour
```

Layout Bronze được tách theo event family để tránh ép các event không cùng schema
vào một partition layout duy nhất.

Layout dự kiến cho commit events:

```text
s3://bluesky-lake/bronze/bluesky_commit_events/
    ingest_date=YYYY-MM-DD/
        ingest_hour=HH/
            collection=app.bsky.feed.post/
```

Layout dự kiến cho identity events:

```text
s3://bluesky-lake/bronze/bluesky_identity_events/
    ingest_date=YYYY-MM-DD/
        ingest_hour=HH/
```

Layout dự kiến cho account events:

```text
s3://bluesky-lake/bronze/bluesky_account_events/
    ingest_date=YYYY-MM-DD/
        ingest_hour=HH/
```

Không partition theo:

- DID.
- Hashtag.
- Post URI.

Không dùng `collection=__HIVE_DEFAULT_PARTITION__` như một layout chính thức cho
`identity/account`. Nếu event family không có collection, cần ghi sang path riêng
hoặc schema riêng.

### 14.2. Silver layer

Mục tiêu:

- Clean dữ liệu event-level.
- Parse JSON raw thành các cột rõ ràng.
- Normalize schema theo domain.
- Validate field quan trọng.
- Deduplicate event khi bổ sung khóa dedup/watermark đầy đủ.
- Data typing: ép kiểu thời gian, số, boolean thay vì để toàn bộ là string.
- Enrichment nhẹ khi cần, ví dụ phân loại event, text length, reply flag hoặc
  subject URI.
- Xử lý create/update/delete thành các bảng Silver có nghĩa nghiệp vụ rõ ràng.

Điểm quan trọng là **Bronze và Silver có thể có cùng granularity**. Trong project
này, cả Bronze và Silver đều chủ yếu lưu dữ liệu ở mức event-level hoặc row-level.
Sự khác biệt không nằm ở độ mịn dữ liệu, mà nằm ở chất lượng và cấu trúc:

- **Bronze raw events** giữ gần như mọi thứ nhận được từ Bluesky/Kafka: raw JSON,
  metadata Kafka, offset, timestamp, collection, operation và các field audit. Nếu
  nguồn trả event thiếu field, duplicate do retry hoặc payload còn lộn xộn, Bronze
  vẫn giữ lại để audit/replay.
- **Silver clean events** vẫn là từng event hoặc từng record nghiệp vụ, nhưng đã
  được parse, chuẩn hóa và tách thành bảng rõ nghĩa. Downstream không cần parse
  lại JSON thô khi build Gold hoặc chạy reconciliation.

Bảng Silver hiện tại của project:

```text
silver_posts
silver_deleted_records
silver_engagements
silver_follows
```

Ví dụ vai trò từng bảng:

- `silver_posts`: post create/update events đã chuẩn hóa thành `post_uri`,
  `author_did`, `record_created_at`, `text`, `text_length`, `is_reply`,
  `reply_root_uri`, `reply_parent_uri`.
- `silver_engagements`: like/repost create events đã chuẩn hóa thành
  `engagement_uri`, `actor_did`, `engagement_type`, `subject_uri`,
  `subject_cid`.
- `silver_follows`: follow create events đã chuẩn hóa thành `follow_uri`,
  `actor_did`, `target_actor_did`.
- `silver_deleted_records`: delete operations đã chuẩn hóa thành `record_uri`,
  `deleted_collection`, `deleted_rkey`.

Silver không phải metric layer. Silver cũng không phải data model phân tích cuối
cùng. Silver là **clean event foundation** để các bước Gold modeling, aggregate,
backfill và reconciliation dùng chung.

Việc lưu toàn bộ post text trong Silver cần được đánh giá theo use case và retention.

### 14.3. Gold modeled layer

Mục tiêu:

- Business analytics.
- Data modeling.
- Tạo fact/dimension hoặc semantic marts từ Silver.
- Tạo lớp dữ liệu dễ JOIN, dễ phân tích và ổn định hơn Silver event tables.
- Làm nguồn chuẩn để tính metric phục vụ dashboard hoặc ad-hoc analytics.

Gold trong lakehouse path **không chỉ là metric aggregate**. Nếu chỉ lấy Silver
để tính thẳng một vài metric rồi đẩy vào ClickHouse, hệ thống sẽ nhanh bị hụt hơi
khi cần phân tích chuyên sâu hơn. Tầng Gold là nơi diễn ra data modeling: biến
các event tables tương đối phẳng và rời rạc ở Silver thành mô hình phân tích có
business logic rõ ràng.

Luồng tính toán lakehouse chuẩn hơn:

```text
Silver clean events
→ Gold modeled tables / semantic marts
→ Trino ad-hoc SQL
→ Gold aggregated metrics
→ ClickHouse serving marts / Grafana
```

Các bảng Gold modeled dự kiến cho project Bluesky:

```text
gold_dim_actors
gold_dim_posts
gold_fact_content_events
gold_fact_engagement_events
gold_fact_network_events
```

Ý nghĩa ví dụ:

- `gold_dim_actors`: một dòng cho mỗi actor DID đã xuất hiện trong dữ liệu.
- `gold_dim_posts`: một dòng cho mỗi post URI, đại diện cho trạng thái mới nhất
  mà lakehouse biết về bài viết.
- `gold_fact_content_events`: các event liên quan tới post lifecycle như create,
  update, delete và reply.
- `gold_fact_engagement_events`: các interaction hướng vào post như like/repost,
  actor, target post và event time.
- `gold_fact_network_events`: các event quan hệ social graph như follow/unfollow.

Các dimension bổ sung như date, collection hoặc event type có thể được thêm sau
khi query pattern đủ rõ. Không tạo quá sớm nếu các cột hiện tại đã đáp ứng được
dashboard và Trino ad-hoc query.

Khi đã có Gold modeled layer, Data Analyst, người vận hành hoặc pipeline metric
có thể query bằng Trino theo business logic rõ ràng hơn, ví dụ:

```text
Người dùng tương tác nhiều nhất với loại nội dung nào?
Bài viết dạng reply có nhận engagement khác original post không?
Collection nào tăng trưởng mạnh theo giờ/ngày?
Actor nào tạo nhiều follow events trong một khoảng thời gian?
```

Các câu hỏi này khó trả lời nếu chỉ có các bảng metric aggregate đã tính sẵn.
Gold modeled layer giữ khả năng phân tích sâu qua Trino; Gold aggregated layer
tối ưu tốc độ cho dashboard qua ClickHouse.

### 14.4. Gold aggregated / serving layer

Mục tiêu:

- Dashboard.
- API metrics.
- ClickHouse serving.
- Low-latency query cho Grafana.

Gold aggregated là lớp metric/mart được tính từ Gold modeled hoặc từ Silver khi
modeling chưa hoàn thiện. Đây là phần phù hợp để đưa vào ClickHouse vì dashboard
cần query nhanh và thường chỉ đọc kết quả đã tổng hợp.

Về mặt kỹ thuật, Grafana hoặc analyst hoàn toàn có thể query trực tiếp từ Gold
modeled layer nếu query engine có các bảng fact và dimension. Trong project này,
query engine đó là Trino. Ví dụ, với Gold modeled v1 đã có
`gold_fact_content_events` và `gold_dim_posts`, query có thể có dạng:

```sql
SELECT
    date_trunc('minute', content_events.event_time) AS minute,
    posts.is_reply,
    count(*) AS total_content_events
FROM gold_fact_content_events AS content_events
JOIN gold_dim_posts AS posts
    ON content_events.post_uri = posts.post_uri
WHERE content_events.event_time >= current_timestamp - INTERVAL '1' HOUR
GROUP BY
    minute,
    posts.is_reply
ORDER BY
    minute,
    posts.is_reply
```

Cách này linh hoạt vì người dùng đọc trực tiếp mô hình phân tích trên lakehouse.
Tuy nhiên, với dashboard realtime hoặc near-realtime, Grafana thường refresh liên
tục. Nếu mỗi 5-30 giây dashboard lại bắt Trino hoặc ClickHouse đọc nhiều dòng
fact, JOIN dimension và GROUP BY lại từ đầu, hệ thống sẽ lặp lại cùng một phép
tính rất nhiều lần. Khi nhiều người cùng mở dashboard, tải CPU và I/O sẽ tăng
theo số người xem và số panel.

Gold Aggregated giải quyết bài toán này bằng cách pre-aggregate các metric phổ
biến trước khi Grafana hỏi. Thay vì để Grafana kích hoạt query nặng mỗi lần
refresh, pipeline hoặc database tính sẵn kết quả theo grain phù hợp, ví dụ theo
phút, event type, collection hoặc engagement type. Grafana chỉ cần query bảng nhỏ:

```sql
SELECT
    window_start,
    content_activity_type,
    sum(activity_count) AS activity_count
FROM bluesky.gold_content_activity_1m_stream
WHERE window_start >= now() - INTERVAL 1 HOUR
GROUP BY
    window_start,
    content_activity_type
ORDER BY
    window_start,
    content_activity_type
```

Bảng Gold Aggregated thường nhỏ hơn rất nhiều so với fact table vì mỗi dòng đã là
kết quả gom cụm theo window và dimension chính. Điều này giúp:

- Dashboard load nhanh hơn vì query ít JOIN và ít GROUP BY nặng.
- ClickHouse chịu tải tốt hơn khi nhiều người cùng xem dashboard.
- Công thức metric được quản lý tập trung trong pipeline/model thay vì rải ở từng
  panel Grafana.
- Có thể kiểm chứng metric bằng reconciliation giữa Gold modeled, Gold aggregated
  và ClickHouse serving.

Trong project hiện tại, realtime fast path đã áp dụng tư duy Gold Aggregated:
Spark Structured Streaming tính trước các bảng theo phút như
`gold_event_volume_1m_stream`, `gold_content_activity_1m_stream`,
`gold_engagement_1m_stream` và `gold_network_activity_1m_stream`, sau đó ghi vào
ClickHouse để Grafana query nhẹ.

Với lakehouse path, thiết kế hiện tại là:

```text
Silver clean events
→ Gold modeled fact/dim hoặc semantic marts
→ Trino ad-hoc analytics
→ Gold analytics / serving metrics
→ ClickHouse serving marts
→ Grafana
```

Gold aggregated trong lakehouse path không nên chỉ lặp lại các metric realtime đơn
giản như event volume theo phút. Các serving marts ở lớp này cần trả lời được câu
hỏi phân tích sâu hơn, ví dụ:

- Post nào có performance tốt nhất, xét cả like, repost, tốc độ nhận engagement
  và trạng thái deleted.
- Reply và original post khác nhau thế nào về engagement.
- Thread nào tạo nhiều conversation nhất và có bao nhiêu actor tham gia.
- Actor nào là creator, actor nào là engager, actor nào nhận nhiều engagement.
- Network growth quan sát được theo target actor thay đổi ra sao.

Bộ metric lakehouse serving v1 được mô tả trong
`docs/gold-analytics-metrics-v1.md` và đã được triển khai cho dashboard
`Bluesky Gold Analytics`. Các bảng cũ như
`gold_event_volume_by_type` và `gold_post_engagement_summary` chỉ là checkpoint
serving tối thiểu trong giai đoạn đầu, không phải thiết kế analytics cuối cùng.

ClickHouse cũng có một cơ chế mạnh để hỗ trợ lớp serving: Materialized View kết
hợp với các engine như `SummingMergeTree` hoặc `AggregatingMergeTree`. Nếu sau
này có use case đưa một phần serving model vào ClickHouse, có thể tạo
Materialized View để ClickHouse tự động cập nhật bảng Gold Aggregated khi dữ liệu
mới được insert. Trong project hiện tại, realtime marts đang được pre-aggregate
bằng Spark trước khi insert vào ClickHouse; Materialized View là hướng tối ưu có
thể cân nhắc sau khi Gold modeled tables và serving use case ổn định.

Bảng có thể cân nhắc trong các milestone mở rộng sau:

```text
gold_event_volume_1m
gold_trending_hashtags_5m
gold_top_domains_15m
gold_language_activity_hourly
gold_post_engagement_velocity
gold_activity_spike_alerts
gold_pipeline_quality_metrics
```

Trong MVP hiện tại, project đã có các bảng aggregate/serving chính như:

```text
bluesky.gold_event_volume_by_type
bluesky.gold_post_engagement_summary
bluesky.gold_post_performance
bluesky.gold_content_quality_hourly
bluesky.gold_thread_conversation_summary
bluesky.gold_actor_activity_daily
bluesky.gold_network_growth_daily
bluesky.gold_event_volume_1m_stream
bluesky.gold_content_activity_1m_stream
bluesky.gold_engagement_1m_stream
bluesky.gold_network_activity_1m_stream
bluesky.gold_realtime_stream_batches
```

Các bảng realtime marts thuộc fast path có thể được tính trực tiếp từ Kafka bằng
Spark Structured Streaming để đạt freshness thấp. Chúng không đại diện cho toàn
bộ tầng Gold lakehouse. Chúng là **serving marts tối ưu latency**.

Không cần triển khai toàn bộ bảng mở rộng trong MVP đầu tiên.

---

## 15. Business use cases

### 15.1. Event volume

- Posts per minute.
- Creates, updates và deletes theo thời gian.
- Activity theo collection.

### 15.2. Trending hashtags

- Post count theo hashtag.
- Unique author count.
- Growth rate so với cửa sổ trước.
- Trend score đơn giản, có công thức rõ ràng.

### 15.3. Shared domains

- Domain được chia sẻ nhiều.
- Domain có tốc độ tăng nhanh.
- Số unique author chia sẻ domain.

### 15.4. Engagement velocity

Chỉ triển khai sau khi bổ sung like/repost.

Có thể đo:

```text
likes_per_minute
reposts_per_minute
engagement_growth_rate
```

### 15.5. Platform quality

- Invalid event rate.
- Duplicate event rate.
- Late event rate.
- Processing latency.
- Kafka consumer lag.
- Bronze-to-Silver count difference.

---

## 16. ClickHouse serving / metric store model

ClickHouse ưu tiên lưu:

- Window aggregates.
- Trending results.
- Platform metrics.
- Latest dashboard state.
- Gold aggregated marts đã được tính sẵn.

ClickHouse không ưu tiên lưu toàn bộ raw event, Silver event-level hoặc toàn bộ
Gold modeled fact/dim. Những lớp đó thuộc lakehouse storage trên MinIO/Iceberg.

Nguyên tắc:

```text
Bronze Parquet             = raw history và replay
Silver Iceberg             = clean event-level source of truth
Gold modeled Iceberg       = fact/dim hoặc semantic marts
Trino                       = query engine cho lakehouse SQL/ad-hoc analytics
ClickHouse serving marts   = metric store / rebuildable serving layer
```

Nếu ClickHouse mất dữ liệu:

1. Xác định khoảng thời gian bị ảnh hưởng.
2. Đọc Gold modeled tables từ Iceberg nếu đã có model hoàn chỉnh.
3. Nếu Gold modeled chưa có đủ, rebuild tạm từ Silver Iceberg.
4. Tính lại aggregate/serving marts cho khoảng thời gian đó.
5. Load lại ClickHouse.
6. Chạy reconciliation.
7. Xác nhận dashboard hoạt động.

Vì vậy ClickHouse là nơi tối ưu latency cho dashboard, không phải nơi giữ toàn bộ
business model của lakehouse. Điều này giúp project vừa có dashboard realtime
nhanh, vừa giữ được khả năng phân tích sâu bằng Trino và khả năng rebuild trong
lakehouse.

---

## 17. Airflow workflows dự kiến

### 17.1. Lakehouse backfill

Input:

```text
start_date
end_date
```

Workflow:

```text
validate_parameters
→ submit_spark_backfill
→ validate_output
→ load_clickhouse
→ reconciliation
```

### 17.2. Scheduled Gold modeling and serving refresh

Input:

```text
refresh_window
```

Workflow:

```text
check_silver_freshness
→ build_gold_modeled_tables_from_silver
→ validate_gold_modeled_tables
→ validate_gold_modeled_tables_with_trino
→ build_gold_aggregates_from_modeled_tables
→ load_clickhouse_gold_marts
→ run_gold_reconciliation
→ publish_refresh_metrics
```

Luồng này được schedule chậm hơn Bronze -> Silver streaming, ví dụ theo giờ, vì
Gold lakehouse path thường gồm data modeling và aggregate phức tạp, ưu tiên khả
năng kiểm chứng/rebuild hơn freshness vài giây. Khi cần mở rộng phân tích, chỉ
cần thay đổi model hoặc câu query aggregate từ Gold modeled layer, không phải xử
lý lại raw JSON ở Silver cho mọi dashboard.

### 17.3. Silver Iceberg maintenance

```text
compact_small_files
→ expire_old_snapshots
→ remove_orphan_files
→ publish_maintenance_metrics
```

Chỉ triển khai các operation Iceberg đã được hiểu và thử nghiệm an toàn.

### 17.4. Data quality

```text
check_data_freshness
→ check_required_fields
→ compare_layer_counts
→ check_duplicate_rate
→ publish_quality_report
```

### 17.5. ClickHouse rebuild

```text
identify_missing_period
→ read_gold_lakehouse
→ rebuild_clickhouse_partition
→ validate_counts
```

---

## 18. Observability

### 18.1. Logs

Các service phải có structured logs.

Ví dụ event:

```text
websocket_connected
websocket_disconnected
websocket_reconnect_attempt
kafka_publish_failed
spark_batch_started
spark_batch_completed
clickhouse_load_failed
invalid_event_detected
```

### 18.2. Metrics

Gateway:

```text
jetstream_events_received_total
kafka_events_published_total
gateway_publish_failures_total
gateway_reconnect_total
```

Kafka/Spark:

```text
kafka_consumer_lag
spark_input_rows_per_second
spark_processed_rows_per_second
spark_batch_duration_seconds
spark_failed_batches_total
```

Data quality:

```text
invalid_events_total
duplicate_events_total
late_events_total
last_successful_event_timestamp
bronze_silver_count_difference
```

Serving:

```text
clickhouse_insert_failures_total
clickhouse_query_duration_seconds
clickhouse_last_load_timestamp
```

### 18.3. Alerts

Các alert ban đầu:

- Không nhận được event mới.
- Kafka consumer lag tăng liên tục.
- Spark processing rate thấp hơn input rate.
- DLQ hoặc quarantine có event mới.
- Data freshness vượt ngưỡng.
- ClickHouse không khả dụng.

Không tạo alert nếu không có hành động xử lý tương ứng.

---

## 19. Reliability và failure scenarios

Project phải kiểm tra ít nhất các trường hợp:

### 19.1. WebSocket disconnect

Kỳ vọng:

- Gateway phát hiện disconnect.
- Retry có backoff.
- Không crash vô hạn.
- Có log và metric.

### 19.2. Kafka unavailable

Kỳ vọng:

- Gateway không giữ message vô hạn trong RAM.
- Có retry giới hạn.
- Có log rõ ràng.
- Hành vi mất dữ liệu hoặc backpressure phải được tài liệu hóa.

### 19.3. Spark restart

Kỳ vọng:

- Kafka giữ backlog.
- Consumer lag tăng.
- Spark restart từ checkpoint.
- Backlog được xử lý sau khi phục hồi.

### 19.4. Spark worker failure

Kỳ vọng:

- Executor bị mất.
- Task được schedule lại.
- Job tiếp tục hoặc lỗi được phát hiện rõ.
- Kết quả được ghi lại bằng Spark UI hoặc logs.

### 19.5. ClickHouse down

Kỳ vọng:

- Bronze/Silver vẫn giữ dữ liệu cần thiết để rebuild.
- Serving data có thể load lại.
- Pipeline không mất source of truth.

### 19.6. Schema change

Kỳ vọng:

- Event không hợp lệ không làm toàn pipeline crash.
- Schema version được nhận biết.
- Unknown field không nhất thiết gây lỗi.
- Required field thiếu phải được quarantine.

---

## 20. Data quality

Các kiểm tra dự kiến:

- Required field không null.
- Timestamp parse được.
- Collection thuộc phạm vi hỗ trợ.
- Operation hợp lệ.
- Event key tồn tại.
- Duplicate rate nằm dưới ngưỡng.
- Data freshness đạt yêu cầu.
- Count giữa các layer không chênh lệch bất thường.
- ClickHouse load count khớp aggregate tính lại từ Silver.

Data quality không chỉ là unit test cho code.

Nó phải kiểm tra dữ liệu đang chạy trong pipeline.

---

## 21. Privacy và governance

Dữ liệu công khai vẫn phải được xử lý có trách nhiệm.

Nguyên tắc:

- Không hiển thị danh tính tài khoản cụ thể trong portfolio.
- Hash hoặc pseudonymize DID trong Silver/Gold.
- Không xây tính năng profiling cá nhân.
- Không giữ post text lâu hơn nhu cầu kỹ thuật.
- Tôn trọng delete event.
- Raw data có retention rõ ràng.
- Dashboard ưu tiên dữ liệu aggregate.
- Không mô tả Jetstream là authoritative archive.

Dữ liệu vị trí hoặc thông tin nhạy cảm khác chỉ được lưu nếu có use case rõ ràng.

---

## 22. Performance và distributed-system experiments

Project phải có một số thí nghiệm có kết quả đo được.

### 22.1. Kafka partition experiment

So sánh:

```text
1 partition
4 partitions
8 partitions
```

Quan sát:

- Producer throughput.
- Consumer parallelism.
- Ordering.
- Partition distribution.

### 22.2. Spark worker scaling

So sánh:

```text
1 worker
2 workers
3 workers
```

Đo:

- Runtime.
- Input rate.
- Processed rate.
- Task distribution.
- Shuffle.
- CPU/memory.

Không giả định performance tăng tuyến tính.

### 22.3. Spark partition experiment

So sánh số partition khác nhau và quan sát:

- Số task.
- Task duration.
- Scheduling overhead.
- Output file count.

### 22.4. Data skew

Tạo một key chiếm tỷ trọng event lớn.

Quan sát:

- Một task chạy chậm.
- Executor utilization không cân bằng.
- Stage completion time.

### 22.5. Small-file problem

Quan sát khi Structured Streaming ghi quá nhiều file nhỏ.

Thử:

- Điều chỉnh trigger interval.
- Điều chỉnh output partitions.
- Compaction.

### 22.6. Partition pruning

So sánh query:

- Không có filter partition.
- Có filter theo ngày/giờ.

Đo:

- Số file scan.
- Dung lượng đọc.
- Query time.

---

## 23. Nguyên tắc thiết kế

### 23.1. Không over-engineering

Mỗi thành phần phải giải quyết một vấn đề cụ thể.

Không thêm:

- Redis nếu chưa có caching use case.
- RabbitMQ nếu Kafka đã đáp ứng event streaming.
- Flink khi Spark đang là compute engine.
- Kubernetes trước khi Docker Compose architecture hoạt động.
- Connector hoặc federated source chỉ thêm khi có nhu cầu query rõ ràng.
- OpenMetadata chỉ để chụp giao diện.

### 23.2. Xây theo vertical slice

Mỗi milestone phải tạo một luồng nhỏ chạy được.

Ví dụ:

```text
Jetstream → terminal
```

sau đó:

```text
Jetstream → Kafka → consumer
```

sau đó:

```text
Kafka → Spark → console
```

Không dựng toàn bộ hạ tầng trước khi có data flow.

### 23.3. Source of truth rõ ràng

- Kafka: replay ngắn hạn.
- Bronze Parquet: raw history và replay.
- Silver Iceberg: analytical source of truth.
- Gold modeled Iceberg: business-ready fact/dim hoặc semantic marts.
- ClickHouse serving marts: metric store và dashboard serving layer.
- Dashboard: presentation layer.

### 23.4. Idempotency

Mọi batch load hoặc rebuild phải có thể chạy lại an toàn.

### 23.5. Configuration

- Không hard-code credentials.
- Dùng environment variables.
- Cung cấp `.env.example`.
- Không commit `.env`.

### 23.6. Phiên bản

- Docker image dùng version cụ thể.
- Dependency quan trọng phải pin version.
- Không mặc định dùng `latest`.

### 23.7. Tài liệu trung thực

Chỉ mô tả tính năng đã chạy thực tế.

Không dùng các cụm từ:

- Production-scale.
- Exactly-once.
- High availability.
- Zero data loss.

trừ khi project đã chứng minh được.

---

## 24. Phạm vi triển khai theo milestone

### Milestone 0 — Chuẩn bị môi trường

Mục tiêu:

- Xác nhận WSL.
- Xác nhận Git.
- Xác nhận Python.
- Xác nhận Docker và Docker Compose.
- Khởi tạo repository.
- Thiết lập tài liệu hướng dẫn Codex.

Không triển khai Kafka hoặc Spark.

### Milestone 1 — Khám phá Jetstream

Luồng:

```text
Jetstream → Python script → terminal/JSONL
```

Mục tiêu:

- Kết nối WebSocket.
- Nhận event thật.
- Quan sát schema.
- Xác định collection và operation.
- Xác định event key tiềm năng.
- Kiểm tra reconnect cơ bản.

### Milestone 2 — Ingestion Gateway và Kafka

Luồng:

```text
Jetstream → Python Gateway → Kafka → sample consumer
```

Mục tiêu:

- Event envelope.
- Kafka topic.
- Kafka key.
- Partitions.
- Producer retry.
- Consumer group.
- Offset và replay.

### Milestone 3 — Spark Structured Streaming

Luồng:

```text
Kafka → Spark Structured Streaming → console/debug sink
```

Mục tiêu:

- Spark cluster.
- Kafka source.
- Schema parsing.
- Event time.
- Checkpoint.
- Restart recovery.
- Spark UI.

### Milestone 4 — Bronze Data Lake

Luồng:

```text
Kafka → Spark → Bronze Parquet trên MinIO
```

Mục tiêu:

- S3A connection.
- Bucket.
- Object layout.
- Parquet.
- Storage partitioning.
- Replay từ Bronze.

### Milestone 5 — Silver processing

Luồng:

```text
Bronze/Kafka → Spark → Silver
```

Mục tiêu:

- Validation.
- Deduplication.
- Watermark.
- Late events.
- Delete/update handling.
- Pseudonymization.
- Quarantine.

### Milestone 6 — Gold analytics

Mục tiêu:

- Gold modeled fact/dim hoặc semantic marts.
- Event volume.
- Trending hashtags.
- Shared domains.
- Data quality metrics.
- Window aggregation.
- Aggregate/serving marts được tính từ Gold modeled layer.

### Milestone 7 — Apache Iceberg

Mục tiêu:

- Quản lý Silver tables bằng Iceberg.
- Snapshot.
- Time travel.
- Schema evolution.
- Compaction.
- Snapshot expiration.

### Milestone 8 — Trino Query Engine

Mục tiêu:

- Cấu hình Trino local.
- Kết nối Trino tới Iceberg catalog trên MinIO.
- Query Silver và Gold modeled Iceberg bằng SQL.
- Tạo các SQL kiểm tra schema, row count và freshness.
- Chứng minh lakehouse có query engine layer độc lập với Spark job.

### Milestone 9 — ClickHouse và Grafana

Mục tiêu:

- Serving tables.
- Incremental load.
- Dashboard.
- Rebuild ClickHouse serving marts từ Gold modeled hoặc Silver.
- Query tuning cơ bản.

### Milestone 10 — Airflow

Mục tiêu:

- Backfill DAG.
- Data quality DAG.
- Maintenance DAG.
- ClickHouse rebuild DAG.

### Milestone 11 — Observability và failure testing

Mục tiêu:

- Prometheus metrics.
- Grafana technical dashboard.
- Alerts.
- Worker failure.
- Spark restart.
- Kafka backlog.
- ClickHouse recovery.

---

## 25. Công nghệ mở rộng

Các công nghệ sau không thuộc core MVP.

Chỉ bổ sung sau khi core architecture hoàn thành.

### dbt

Có thể dùng cho:

- Analytical SQL models trên Trino hoặc ClickHouse tùy lớp dữ liệu.
- Tests.
- Documentation.
- Metric definitions.

### Trino connectors mở rộng

Trino đã được chọn làm query engine cho Iceberg lakehouse. Các connector mở rộng
chỉ bổ sung khi có use case rõ ràng:

- Federated query giữa Iceberg, PostgreSQL và ClickHouse.
- Query thêm external data source ngoài lakehouse.
- Analyst workspace cần kết hợp nhiều hệ thống dữ liệu.

### Cube.js

Chỉ dùng khi cần:

- Semantic layer.
- Centralized metrics.
- Metrics API.
- Pre-aggregation.

### OpenLineage/OpenMetadata

Chỉ dùng khi cần chứng minh:

- Dataset lineage.
- Ownership.
- Tags.
- Metadata catalog.
- Schema change tracking.

### MLflow

Chỉ bổ sung nếu project mở rộng sang ML platform.

Không thuộc phạm vi core streaming analytics.

---

## 26. Những thành phần không sử dụng trong phiên bản đầu

- Apache Flink.
- Kubernetes.
- RabbitMQ.
- Redis.
- Sentiment analysis.
- LLM classification.
- User profiling.
- Feature Store.
- Online model inference.
- Multi-region deployment.
- Production high availability.

---

## 27. Deliverables cuối dự án

Repository MVP hiện tại cần có:

- Docker Compose có thể chạy lại.
- Hướng dẫn setup.
- Event schema.
- Topic design.
- Bronze/Silver storage layout.
- ClickHouse schema.
- Grafana dashboards.
- Unit tests.
- Design decisions.
- Trade-offs và limitations.
- Demo video hoặc screenshots.

Các deliverables phù hợp cho milestone production-like tiếp theo:

- Architecture diagram và data flow diagram dạng hình hóa chính thức.
- Airflow DAGs cho Gold incremental, data quality, compaction và backfill.
- Integration tests chạy với hạ tầng container.
- Failure experiment results.
- Performance benchmark results.
- Alerting rules và runbook vận hành.

---

## 28. Các câu hỏi project phải trả lời được khi phỏng vấn

### Streaming

- Vì sao cần Kafka giữa Jetstream và Spark?
- Kafka khác message queue truyền thống như thế nào?
- Ordering được đảm bảo ở mức nào?
- Vì sao chọn message key hiện tại?
- Consumer lag có ý nghĩa gì?

### Spark

- Driver, executor, stage và task khác nhau thế nào?
- Shuffle xuất hiện ở đâu?
- Watermark giải quyết vấn đề gì?
- Checkpoint chứa gì?
- Worker chết thì chuyện gì xảy ra?
- Vì sao dùng Spark thay vì Python consumer?

### Storage

- S3, Parquet và Iceberg khác nhau thế nào?
- Vì sao cần Bronze/Silver/Gold?
- Vì sao không lưu lịch sử chỉ trong Kafka?
- Small-file problem là gì?
- Partitioning ảnh hưởng query như thế nào?

### Serving

- Vì sao cần ClickHouse khi đã có Iceberg?
- ClickHouse mất dữ liệu thì khôi phục như thế nào?
- Dữ liệu nào không nên đưa vào ClickHouse?

### Reliability

- Pipeline có thể tạo duplicate không?
- Delivery semantics là gì?
- Spark restart tiếp tục từ đâu?
- Nếu Kafka hoặc MinIO down thì sao?
- Làm sao phát hiện dữ liệu bị stale?

### Engineering decisions

- Những công nghệ nào đã chủ động không sử dụng?
- Project đã tránh over-engineering như thế nào?
- Giới hạn của môi trường local là gì?
- Điều gì phải thay đổi nếu triển khai production?
