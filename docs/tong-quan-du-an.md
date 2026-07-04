# Tổng quan dự án

## 1. Tên dự án

**Distributed Bluesky Streaming Analytics Lakehouse**

Tên mô tả ngắn:

> Nền tảng xử lý dữ liệu streaming phân tán từ Bluesky Jetstream, sử dụng Kafka,
> Spark Structured Streaming, S3-compatible Data Lake, Apache Iceberg và
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
7. Phục vụ truy vấn gần thời gian thực bằng ClickHouse.
8. Hiển thị business metrics và platform metrics qua Grafana.
9. Hỗ trợ backfill, data quality, compaction và rebuild bằng Airflow.
10. Theo dõi độ trễ, throughput, consumer lag và lỗi pipeline.

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

Spark phải được chạy ở chế độ multi-worker Spark Standalone bằng Docker Compose,
không chỉ chạy bằng `local[*]`.

### 4.4. Storage architecture

MinIO được sử dụng làm S3-compatible object storage trong môi trường local.

Dữ liệu được tổ chức theo các tầng với công nghệ phù hợp cho từng mục đích:

- Bronze: dữ liệu gần nguồn, lưu dạng Parquet partitioned trên MinIO.
- Silver: dữ liệu đã chuẩn hóa và kiểm tra, quản lý bằng Apache Iceberg trên MinIO.
- Gold: dữ liệu aggregate phục vụ dashboard, lưu trong ClickHouse.

Bronze chưa cần dùng Iceberg ngay lập tức vì tầng này chủ yếu phục vụ append,
audit và replay. Thiết kế đơn giản bằng Parquet partitioned giúp dễ quan sát dữ
liệu và giảm độ phức tạp ở giai đoạn đầu.

Apache Iceberg được dùng từ Silver vì tầng này cần table semantics, snapshot,
schema evolution, deduplication và xử lý thay đổi dữ liệu rõ ràng hơn.

ClickHouse đóng vai trò Gold serving layer cho truy vấn phân tích có độ trễ thấp.
ClickHouse không phải source of truth duy nhất; dữ liệu Gold trong ClickHouse phải
có thể được rebuild từ Silver khi cần.

### 4.5. Analytical serving

ClickHouse được sử dụng làm serving layer cho:

- Dashboard gần thời gian thực.
- Query aggregate có độ trễ thấp.
- Business metrics.
- Platform metrics đã tổng hợp.

ClickHouse không phải source of truth duy nhất.

Các bảng ClickHouse phải có khả năng rebuild từ Silver Iceberg khi cần.

### 4.6. Platform operations

Airflow được sử dụng cho các workflow có điểm bắt đầu và kết thúc rõ ràng:

- Historical backfill.
- Data quality checks.
- Reconciliation.
- Iceberg compaction.
- Snapshot expiration.
- ClickHouse incremental load.
- ClickHouse rebuild.

Airflow không được sử dụng để xử lý từng streaming event.

### 4.7. Observability

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
└──────────────┬──────────────┘
               │
               ▼
┌─────────────────────────────┐
│ Spark Structured Streaming  │
│                             │
│ - Parse                     │
│ - Validate                  │
│ - Deduplicate               │
│ - Watermark                 │
│ - Event-time window         │
│ - Stateful aggregation      │
│ - Enrichment                │
└──────────────┬──────────────┘
               │
               │ Raw append data
               ▼
┌─────────────────────────────┐
│ Bronze data lake            │
│ MinIO + partitioned Parquet │
└──────────────┬──────────────┘
               │
               │ Clean and normalized data
               ▼
┌─────────────────────────────┐
│ Silver Lakehouse            │
│ MinIO + Apache Iceberg      │
└──────────────┬──────────────┘
               │
               │ Aggregates / rebuild source
               ▼
┌─────────────────────────────┐
│ Gold serving marts          │
│ ClickHouse                  │
└──────────────┬──────────────┘
               │
               ▼
            Grafana

┌─────────────────────────────┐
│ Apache Airflow              │
│                             │
│ - Backfill                  │
│ - Data quality              │
│ - Compaction                │
│ - Reconciliation            │
│ - ClickHouse rebuild        │
└─────────────────────────────┘
```

Prometheus thu thập metrics từ các service và cung cấp dữ liệu cho Grafana.

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
- Spark Standalone cluster.
- Spark UI.
- Spark History Server.

### 6.5. Storage

- MinIO.
- S3-compatible API.
- Parquet.
- Apache Iceberg.
- Iceberg catalog.

### 6.6. Serving

- ClickHouse.
- MergeTree-family engines.
- Materialized views khi có use case.
- Grafana.

### 6.7. Orchestration

- Apache Airflow.

### 6.8. Monitoring

- Prometheus.
- Grafana.
- Structured application logs.

### 6.9. Testing và code quality

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

Cấu trúc định hướng:

```text
bluesky-pipeline/
├── docs/
│   ├── huong-dan-lam-viec-voi-codex.md
│   ├── tong-quan-du-an.md
│   └── jetstream-schema-notes.md
├── src/
│   └── bluesky_pipeline/
│       ├── event_envelope.py
│       ├── normalize_event.py
│       └── ...
├── scripts/
│   ├── jetstream_probe.py
│   ├── analyze_sample.py
│   ├── normalize_sample.py
│   └── ...
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
- `scripts/`: script chạy tay phục vụ discovery, probe hoặc thao tác local.
- `tests/`: test cho các module có logic đáng kiểm chứng.
- `docs/`: tài liệu thiết kế, phạm vi dự án, ghi chú schema và quyết định kỹ
  thuật.
- `data/`: dữ liệu local sinh ra khi chạy probe hoặc sample; không commit.
- `requirements.txt`: dependency Python cho milestone hiện tại.

Khi code tăng lên, package `src/bluesky_pipeline/` có thể được tách tiếp theo các
nhóm trách nhiệm như:

```text
src/bluesky_pipeline/
├── ingestion/
├── events/
├── normalization/
├── storage/
├── streaming/
├── quality/
└── config/
```

Ý nghĩa định hướng:

- `ingestion/`: kết nối nguồn, WebSocket, Kafka producer, retry và logging.
- `events/`: event envelope, schema contract và Kafka key.
- `normalization/`: parse và chuẩn hóa event theo collection.
- `storage/`: helper liên quan Bronze, Silver, Gold hoặc object storage.
- `streaming/`: Spark Structured Streaming jobs.
- `quality/`: validation, data quality checks và reconciliation.
- `config/`: đọc cấu hình từ biến môi trường hoặc file config không chứa secret.

Các thư mục này chỉ được tạo khi có nhu cầu thực tế. Không tạo trước toàn bộ cấu
trúc nếu milestone hiện tại chưa dùng đến.

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
```

### 8.8. ClickHouse

ClickHouse là analytical serving database.

Nó phục vụ:

- Dashboard.
- Near-real-time metrics.
- Low-latency analytical queries.

### 8.9. Airflow

Airflow orchestration các batch workflow hữu hạn.

Airflow không chạy continuous consumer.

### 8.10. Prometheus và Grafana

Prometheus lưu technical metrics.

Grafana hiển thị:

- Business dashboard.
- Platform dashboard.
- SLI/SLO dashboard nếu được triển khai.

---

## 8. Phạm vi dữ liệu

### 8.1. Phạm vi nguồn hoàn chỉnh

Project tập trung vào các event công khai chính của Bluesky Jetstream để bao phủ
ba nhóm tín hiệu:

- Content activity.
- Engagement activity.
- Network activity.

Các collection thuộc scope hoàn chỉnh:

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

### 8.2. Thứ tự triển khai

Triển khai theo thứ tự để tránh xử lý quá nhiều schema cùng lúc:

1. Probe `app.bsky.feed.post` để xác nhận kết nối và shape event cơ bản.
2. Probe thêm `app.bsky.feed.like`, `app.bsky.feed.repost` và
   `app.bsky.graph.follow`.
3. Ingest raw multi-collection event vào Kafka/Bronze.
4. Normalize từng collection trong Spark theo use case.

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

---

## 9. Event envelope

Gateway phải giữ payload nguồn gần như nguyên vẹn và bổ sung metadata ingest.

Ví dụ logic:

```json
{
  "schema_version": 1,
  "source": "bluesky_jetstream",
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
- Timestamp dùng UTC.
- Không hard-code schema business quá sớm.

---

## 10. Thiết kế Kafka ban đầu

### 10.1. Topic MVP

```text
bluesky.raw.events.v1
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

Layout dự kiến:

```text
s3://bluesky-lake/bronze/events/
    ingest_date=YYYY-MM-DD/
        ingest_hour=HH/
```

Không partition theo:

- DID.
- Hashtag.
- Post URI.

### 14.2. Silver layer

Mục tiêu:

- Clean.
- Normalize.
- Validate.
- Deduplicate.
- Pseudonymize identifiers.
- Xử lý create/update/delete.

Bảng dự kiến:

```text
silver_posts
silver_deleted_records
silver_hashtags
silver_shared_domains
```

Các trường có thể gồm:

```text
post_uri
author_did_hash
created_at
processed_at
language
text_length
hashtag_count
link_count
is_reply
is_quote
record_status
```

Việc lưu toàn bộ post text trong Silver cần được đánh giá theo use case và retention.

### 14.3. Gold serving layer

Mục tiêu:

- Business analytics.
- Dashboard.
- API metrics.
- ClickHouse serving.

Trong project này, Gold là các bảng aggregate hoặc mart phục vụ truy vấn nhanh
trong ClickHouse. Gold không phải là một tầng Iceberg trong MinIO.

Bảng dự kiến:

```text
gold_event_volume_1m
gold_trending_hashtags_5m
gold_top_domains_15m
gold_language_activity_hourly
gold_post_engagement_velocity
gold_activity_spike_alerts
gold_pipeline_quality_metrics
```

Không cần triển khai toàn bộ bảng trong MVP đầu tiên.

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

## 16. ClickHouse serving model

ClickHouse ưu tiên lưu:

- Window aggregates.
- Trending results.
- Platform metrics.
- Latest dashboard state.

Không ưu tiên lưu toàn bộ raw event.

Nguyên tắc:

```text
Bronze Parquet  = raw history và replay
Silver Iceberg  = analytical source of truth
ClickHouse Gold = rebuildable serving layer
```

Nếu ClickHouse mất dữ liệu:

1. Xác định khoảng thời gian bị ảnh hưởng.
2. Đọc dữ liệu Silver từ Iceberg.
3. Tính lại aggregate Gold cho khoảng thời gian đó.
4. Load lại ClickHouse.
5. Chạy reconciliation.
6. Xác nhận dashboard hoạt động.

---

## 17. Airflow workflows dự kiến

### 17.1. Historical backfill

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

### 17.2. Silver Iceberg maintenance

```text
compact_small_files
→ expire_old_snapshots
→ remove_orphan_files
→ publish_maintenance_metrics
```

Chỉ triển khai các operation Iceberg đã được hiểu và thử nghiệm an toàn.

### 17.3. Data quality

```text
check_data_freshness
→ check_required_fields
→ compare_layer_counts
→ check_duplicate_rate
→ publish_quality_report
```

### 17.4. ClickHouse rebuild

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
- Trino khi chưa có nhu cầu federated query.
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
- ClickHouse Gold: serving layer.
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

- Event volume.
- Trending hashtags.
- Shared domains.
- Data quality metrics.
- Window aggregation.

### Milestone 7 — Apache Iceberg

Mục tiêu:

- Quản lý Silver tables bằng Iceberg.
- Snapshot.
- Time travel.
- Schema evolution.
- Compaction.
- Snapshot expiration.

### Milestone 8 — ClickHouse và Grafana

Mục tiêu:

- Serving tables.
- Incremental load.
- Dashboard.
- Rebuild ClickHouse Gold từ Silver.
- Query tuning cơ bản.

### Milestone 9 — Airflow

Mục tiêu:

- Backfill DAG.
- Data quality DAG.
- Maintenance DAG.
- ClickHouse rebuild DAG.

### Milestone 10 — Observability và failure testing

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

- Analytical SQL models trên ClickHouse.
- Tests.
- Documentation.
- Metric definitions.

### Trino

Chỉ dùng khi cần:

- Query Iceberg.
- Federated query giữa Iceberg, PostgreSQL và ClickHouse.
- Ad hoc analyst access.

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

Repository cần có:

- Docker Compose có thể chạy lại.
- Hướng dẫn setup.
- Architecture diagram.
- Data flow diagram.
- Event schema.
- Topic design.
- Bronze/Silver storage layout.
- ClickHouse schema.
- Airflow DAGs.
- Grafana dashboards.
- Unit tests.
- Integration tests.
- Failure experiment results.
- Performance benchmark results.
- Design decisions.
- Trade-offs và limitations.
- Demo video hoặc screenshots.

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
