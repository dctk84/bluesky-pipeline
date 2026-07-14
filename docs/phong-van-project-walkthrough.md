# Project walkthrough khi phỏng vấn

Tài liệu này giúp trình bày project `bluesky-pipeline` trong phỏng vấn Data
Engineer. Mục tiêu là nói rõ bài toán, kiến trúc, trade-off và bài học kỹ thuật
mà không sa vào kể từng command đã chạy.

## 1. Bản tóm tắt 1-2 phút

Project này là một streaming analytics lakehouse xử lý dữ liệu public events từ
Bluesky Jetstream. Em xây pipeline để ingest dữ liệu liên tục qua WebSocket, đưa
vào Kafka làm event buffer, xử lý bằng Spark Structured Streaming, lưu dữ liệu
trên MinIO theo mô hình Bronze/Silver/Gold, dùng Iceberg cho Silver và Gold
modeled, dùng Trino cho ad-hoc lakehouse query, rồi dùng ClickHouse và Grafana để
phục vụ dashboard.

Kiến trúc có hai nhánh serving. Nhánh hot path đọc Kafka bằng Spark streaming và
ghi realtime marts vào ClickHouse để dashboard cập nhật nhanh. Nhánh lakehouse
path ghi Bronze/Silver liên tục, sau đó refresh Gold modeled và Gold analytics
marts theo incremental batch để có các chỉ số phân tích sâu hơn như post
performance, content quality, conversation analytics, actor activity và network
growth.

Điểm em muốn chứng minh là không chỉ ingest được streaming data, mà còn biết tách
rõ raw data, clean lakehouse table, modeled layer, serving marts, dashboard,
checkpoint, reconciliation và các trade-off giữa local demo với production.

## 2. Bản trình bày 3-5 phút

### Bài toán

Bluesky phát sinh nhiều loại event liên tục như post, reply, like, repost, follow
và delete. Nếu chỉ xử lý batch đơn giản thì project không chứng minh được các kỹ
năng streaming, event log, distributed processing, lakehouse và observability.

Vì vậy project này tập trung vào một bài toán streaming analytics: vừa quan sát
hoạt động gần realtime, vừa lưu lại dữ liệu có thể rebuild và phân tích sâu.

### Kiến trúc tổng thể

Luồng chính:

```text
Bluesky Jetstream
-> Python ingestion gateway
-> Kafka
-> Spark Structured Streaming
-> Bronze Parquet trên MinIO
-> Silver Iceberg
-> Gold modeled Iceberg
-> Gold analytics serving marts
-> ClickHouse
-> Grafana
```

Ngoài ra có một realtime fast path:

```text
Kafka
-> Spark Structured Streaming
-> ClickHouse realtime marts
-> Grafana realtime dashboard
```

### Vì sao chọn từng công nghệ

- Kafka: tách ingestion gateway khỏi processing, có buffer, retention ngắn hạn và
  khả năng replay theo offset.
- Spark Structured Streaming: xử lý stream theo micro-batch, parse schema,
  normalize, aggregate, checkpoint và ghi Bronze/Silver/ClickHouse.
- MinIO: mô phỏng S3-compatible object storage trong local.
- Iceberg: cung cấp table semantics, snapshot, schema evolution và query ổn định
  cho Silver/Gold lakehouse.
- Trino: query engine cho ad-hoc analytics trên Iceberg mà không cần viết Spark
  job cho mọi câu hỏi.
- ClickHouse: serving database cho dashboard vì query aggregate nhanh và phù hợp
  với Grafana.
- Grafana: trình bày business metrics và operational metrics.

### Hot path và lakehouse path

Hot path ưu tiên freshness. Nó tính realtime marts theo phút như event volume,
content activity, engagement, network activity và stream batch health. Mục tiêu
là trả lời: pipeline vừa nhận và xử lý gì?

Lakehouse path ưu tiên correctness, rebuildability và phân tích sâu. Bronze giữ
raw history, Silver chuẩn hóa event-level data, Gold modeled tạo fact/dim, còn
Gold analytics serving marts tạo các metric phục vụ dashboard như post
performance, conversation summary, actor activity và network growth.

### Gold incremental load

Ban đầu Gold có thể full rebuild để dễ kiểm chứng. Sau đó project tiến hóa sang
incremental refresh:

- Gold facts đọc dữ liệu Silver mới theo `received_at`.
- Gold dimensions merge/update theo business key.
- Serving marts chỉ replace affected windows hoặc affected entities.
- ClickHouse serving tables không truncate toàn bộ trong incremental path.
- Reconciliation kiểm tra ClickHouse khớp với source/staging theo scope phù hợp.

Điểm quan trọng khi phỏng vấn: Gold trong lakehouse path vẫn là batch hoặc
incremental batch, nhưng project vẫn là streaming lakehouse vì ingestion,
Kafka/Bronze/Silver streaming và realtime fast path chạy liên tục.

### Dashboard chứng minh điều gì

Realtime dashboard chứng minh hot path có dữ liệu gần realtime và có health
metrics cơ bản như freshness, input rows per batch, batch duration và rows
written.

Gold Analytics dashboard chứng minh lakehouse path có thể tạo insight sâu hơn:
content quality, post performance, conversation analytics, actor behavior,
observed network growth và data quality.

Ảnh dashboard đã được lưu trong `docs/assets/` và nhúng trong `README.md`.

## 3. Known limitations cần chủ động nói

- Project chạy local/WSL, Spark đang dùng `local[2]`, chưa phải Spark cluster
  production.
- Airflow chưa triển khai trong MVP; Gold refresh đang chạy thủ công theo phase.
- Demo local chạy hai phase: live streaming trước, Gold analytics refresh sau.
- Gold incremental state đang lưu local JSON, chưa phải durable metadata store.
- Chưa tuyên bố exactly-once end-to-end; hiện có checkpoint, deterministic keys
  và reconciliation, nhưng serving writes cần được nhìn theo hướng at-least-once
  hoặc idempotent-by-design tùy mart.
- Network growth là observed activity từ stream đã ingest, không phải follower
  count toàn cục của Bluesky.
- Dashboard operational đủ cho demo local, nhưng production cần Prometheus,
  consumer lag, alerting, resource metrics và runbook.

## 4. Edge case đáng nhớ

### Event time khác ingestion time

Dashboard Gold từng xuất hiện window năm 2025 dù dữ liệu được ingest năm 2026.
Nguyên nhân là Gold analytics dùng source event time, trong khi ingestion/load
time là thời điểm pipeline nhận hoặc load dữ liệu. Đây là ví dụ tốt để giải thích
event time, ingestion time, processing time và freshness.

### Timestamp timezone trong Spark/ClickHouse/Grafana

Hot path dashboard từng lệch 7 giờ. Bài học là không nên sửa bằng cách cộng/trừ
timezone trong từng query Grafana. Pipeline phải thống nhất timestamp semantics,
dùng `received_at` cho operational hot path và format timestamp rõ ràng trước khi
insert vào ClickHouse.

### Delete event thiếu context

Follow delete có thể không có `target_actor_did` nếu project chưa observe được
follow create tương ứng. Thay vì drop hoặc gán sai, dashboard giữ data quality
panel cho unresolved follow delete targets.

### Local resource oversubscription

Khi cố chạy hot path và Gold path song song trên WSL, nhiều Spark applications và
containers có thể làm WSL/Docker restart. Quyết định cuối là demo local theo hai
phase độc lập, còn production sẽ cần resource isolation và orchestration.

## 5. Câu hỏi phỏng vấn và câu trả lời mẫu

### Vì sao cần Kafka?

Kafka giúp tách nguồn Jetstream khỏi Spark processing. Gateway chỉ cần publish
events vào topic, còn Spark có thể consume với checkpoint riêng. Kafka cũng cho
phép buffer khi downstream chậm, replay trong retention window và nhiều consumer
group độc lập cho hot path/lakehouse path.

### Vì sao không để Grafana query trực tiếp Iceberg?

Iceberg/Trino phù hợp cho ad-hoc analytics và source of truth, nhưng dashboard
refresh liên tục. Nếu Grafana cứ JOIN fact/dim và GROUP BY trên lakehouse mỗi vài
giây thì chi phí cao. ClickHouse serving marts lưu metric đã aggregate để query
nhanh và ổn định hơn.

### Gold modeled khác Gold serving như thế nào?

Gold modeled là fact/dim hoặc semantic layer trên Iceberg, dùng làm source phân
tích có thể rebuild và query bằng Trino. Gold serving marts là bảng aggregate
phục vụ dashboard trong ClickHouse. ClickHouse không phải source of truth duy
nhất; nếu mất có thể rebuild từ Gold modeled hoặc Silver.

### Project có phải streaming không nếu Gold chạy batch?

Có. Streaming không có nghĩa mọi tầng đều phải xử lý từng event theo realtime.
Ingestion, Kafka, Bronze/Silver streaming và hot path đều continuous. Gold
analytics là incremental batch vì ưu tiên correctness, late data, reconciliation
và rebuildability.

### Có đảm bảo exactly-once không?

Không tuyên bố exactly-once end-to-end. Spark checkpoint giúp restart từ offset
và giảm duplicate trong phạm vi streaming job, nhưng cần xét cả source, Kafka,
Spark, object storage, ClickHouse writes và retry. Project hiện trình bày trung
thực là at-least-once với deterministic keys, reconciliation và khả năng rebuild.

### Nếu triển khai production thì cần thay đổi gì?

Các điểm chính:

- Chạy Spark trên cluster/resource manager thay vì local mode.
- Dùng external database cho Hive Metastore thay vì Derby embedded.
- Đưa Gold incremental state vào durable metadata store hoặc orchestrator.
- Thêm Airflow cho batch/incremental orchestration.
- Thêm Prometheus metrics, alerting, consumer lag và runbook.
- Thêm CI/integration tests với container services.
- Thiết kế idempotent sink rõ hơn cho ClickHouse serving writes.

## 6. Kết thúc khi trình bày

Câu kết nên nhấn mạnh:

Project này không cố giả vờ là production-scale trên máy local. Điểm chính là em
đã xây được một streaming analytics lakehouse có data contracts, hai serving
paths, incremental Gold refresh, dashboard, reconciliation và tài liệu hóa các
trade-off. Nếu đưa lên production, em biết những phần nào cần thay đổi: compute
cluster, orchestration, durable metadata, monitoring, alerting và stronger
delivery semantics.
