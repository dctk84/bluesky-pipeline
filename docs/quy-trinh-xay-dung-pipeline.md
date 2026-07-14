# Quy trình xây dựng pipeline

Tài liệu này tổng hợp các mốc chính đã thực sự hoàn thành trong project
`bluesky-pipeline`.

Mục tiêu của tài liệu là phục vụ học tập và ôn phỏng vấn Data Engineer, không phải
nhật ký thao tác chi tiết. Vì vậy tài liệu chỉ giữ lại các bước có ý nghĩa về
kiến trúc, data contract, tầng xử lý hoặc khả năng kiểm chứng của pipeline. Các
bước thử nghiệm nhỏ, refactor cơ học hoặc prototype đã được thay thế được gom vào
bài học của bước lớn tương ứng.

## Mục lục

1. [Xác định mục tiêu và kiến trúc tổng thể](#bước-1-xác-định-mục-tiêu-và-kiến-trúc-tổng-thể)
2. [Thiết lập nền tảng repository và cấu hình dùng chung](#bước-2-thiết-lập-nền-tảng-repository-và-cấu-hình-dùng-chung)
3. [Khám phá dữ liệu Bluesky Jetstream](#bước-3-khám-phá-dữ-liệu-bluesky-jetstream)
4. [Thiết kế event envelope và normalization contract](#bước-4-thiết-kế-event-envelope-và-normalization-contract)
5. [Dựng Kafka local và kiểm chứng publish/consume](#bước-5-dựng-kafka-local-và-kiểm-chứng-publishconsume)
6. [Xây dựng live ingestion gateway](#bước-6-xây-dựng-live-ingestion-gateway)
7. [Kết nối Spark Structured Streaming với Kafka](#bước-7-kết-nối-spark-structured-streaming-với-kafka)
8. [Xây dựng Bronze raw data lake trên MinIO](#bước-8-xây-dựng-bronze-raw-data-lake-trên-minio)
9. [Thiết kế và build Silver v1](#bước-9-thiết-kế-và-build-silver-v1)
10. [Chuẩn hóa metadata và schema dùng chung](#bước-10-chuẩn-hóa-metadata-và-schema-dùng-chung)
11. [Xây dựng ClickHouse serving layer cho Gold aggregates](#bước-11-xây-dựng-clickhouse-serving-layer-cho-gold-aggregates)
12. [Migrate Silver sang Apache Iceberg](#bước-12-migrate-silver-sang-apache-iceberg)
13. [Chuẩn hóa Gold aggregate refresh từ Silver Iceberg](#bước-13-chuẩn-hóa-gold-aggregate-refresh-từ-silver-iceberg)
14. [Bổ sung reconciliation và checkpoint chất lượng](#bước-14-bổ-sung-reconciliation-và-checkpoint-chất-lượng)
15. [Chốt hai serving paths: realtime và lakehouse](#bước-15-chốt-hai-serving-paths-realtime-và-lakehouse)
16. [Xây dựng realtime fast path vào ClickHouse](#bước-16-xây-dựng-realtime-fast-path-vào-clickhouse)
17. [Hoàn thiện Grafana realtime dashboard và operational health](#bước-17-hoàn-thiện-grafana-realtime-dashboard-và-operational-health)
18. [Chốt trạng thái sau realtime và lakehouse baseline](#bước-18-chốt-trạng-thái-sau-realtime-và-lakehouse-baseline)
19. [Bổ sung live demo runner và cleanup dữ liệu local](#bước-19-bổ-sung-live-demo-runner-và-cleanup-dữ-liệu-local)
20. [Bổ sung Trino Query Engine cho Lakehouse](#bước-20-bổ-sung-trino-query-engine-cho-lakehouse)
21. [Thiết kế contract Gold modeled v1](#bước-21-thiết-kế-contract-gold-modeled-v1)
22. [Viết transformation Gold modeled v1](#bước-22-viết-transformation-gold-modeled-v1)
23. [Build Gold modeled v1 trên Iceberg](#bước-23-build-gold-modeled-v1-trên-iceberg)
24. [Kiểm chứng Gold modeled v1 bằng Trino](#bước-24-kiểm-chứng-gold-modeled-v1-bằng-trino)
25. [Thiết kế Gold analytics metrics v1](#bước-25-thiết-kế-gold-analytics-metrics-v1)
26. [Implement Gold post performance serving mart](#bước-26-implement-gold-post-performance-serving-mart)
27. [Kết nối SQL client tới Trino để query lakehouse](#bước-27-kết-nối-sql-client-tới-trino-để-query-lakehouse)
28. [Implement Gold content quality hourly serving mart](#bước-28-implement-gold-content-quality-hourly-serving-mart)
29. [Implement Gold thread conversation summary serving mart](#bước-29-implement-gold-thread-conversation-summary-serving-mart)
30. [Implement Gold actor activity daily serving mart](#bước-30-implement-gold-actor-activity-daily-serving-mart)
31. [Implement Gold network growth daily serving mart](#bước-31-implement-gold-network-growth-daily-serving-mart)
32. [Dựng Grafana Gold Analytics Dashboard](#bước-32-dựng-grafana-gold-analytics-dashboard)
33. [Thiết kế contract incremental refresh cho Gold](#bước-33-thiết-kế-contract-incremental-refresh-cho-gold)
34. [Incremental hóa Gold content quality hourly mart](#bước-34-incremental-hóa-gold-content-quality-hourly-mart)
35. [Incremental hóa các Gold analytics marts còn lại](#bước-35-incremental-hóa-các-gold-analytics-marts-còn-lại)
36. [Đưa incremental Gold vào lakehouse E2E path](#bước-36-đưa-incremental-gold-vào-lakehouse-e2e-path)
37. [Cố định Hive Metastore Derby DB trong volume persist](#bước-37-cố-định-hive-metastore-derby-db-trong-volume-persist)

## Bước 1: Xác định mục tiêu và kiến trúc tổng thể

**Mục tiêu**

Xác định project là một streaming analytics lakehouse từ Bluesky Jetstream, dùng
Kafka làm event backbone, Spark làm compute layer, MinIO/Iceberg làm lakehouse và
ClickHouse/Grafana làm serving/dashboard layer.

**Vì sao cần thực hiện**

Một project Data Engineering dễ bị phình scope nếu bắt đầu bằng công nghệ thay vì
bài toán. Bước này giúp xác định rõ project cần chứng minh các năng lực mà project
batch Data Warehouse trước đó chưa có: streaming ingestion, event log, distributed
processing, lakehouse, analytical serving và observability.

**Kết quả sau khi hoàn thành**

Repository có tài liệu nền tảng mô tả mục tiêu, phạm vi MVP, tech stack, milestone
và nguyên tắc thiết kế. Kiến trúc tổng thể cũng phân biệt rõ local learning
environment với hệ thống production-scale.

**Các file liên quan**

- `AGENTS.md`
- `docs/huong-dan-lam-viec-voi-codex.md`
- `docs/tong-quan-du-an.md`
- `README.md`

**Kiến thức cần ghi nhớ**

- Kiến trúc tổng thể là định hướng dài hạn, không phải yêu cầu triển khai đồng
  thời tất cả thành phần.
- Mỗi milestone nên tạo ra một luồng dữ liệu chạy được và kiểm chứng được.
- Khi trình bày project, cần nói rõ đâu là thành phần đã chạy local, đâu là giới
  hạn hiện tại và đâu là hướng mở rộng.

## Bước 2: Thiết lập nền tảng repository và cấu hình dùng chung

**Mục tiêu**

Thiết lập cấu trúc Python project, tách code pipeline dùng lại, scripts thao tác
local, tests, docs và dữ liệu local.

**Vì sao cần thực hiện**

Data project sẽ tăng nhanh số lượng file. Nếu không tách vai trò từ sớm, logic
pipeline, script thử nghiệm, dữ liệu local và tài liệu sẽ bị lẫn vào nhau, gây khó
debug và khó trình bày.

**Kết quả sau khi hoàn thành**

Project có package chính dưới `src/bluesky_pipeline/`, scripts thao tác local dưới
`scripts/`, tài liệu dưới `docs/`, tests dưới `tests/` và dữ liệu local không
commit dưới `data/`. Các metadata dùng chung như Kafka topic, Bronze/Silver/Gold
paths, ClickHouse table names, Spark/S3 config và transformation logic được tách
dần vào module dùng chung khi chúng trở thành contract giữa nhiều bước.

**Các file liên quan**

- `.gitignore`
- `requirements.txt`
- `src/bluesky_pipeline/config/kafka.py`
- `src/bluesky_pipeline/config/spark.py`
- `src/bluesky_pipeline/schemas/bronze_tables.py`
- `src/bluesky_pipeline/transforms/silver_transformations.py`
- `src/bluesky_pipeline/schemas/gold_tables.py`

**Kiến thức cần ghi nhớ**

- `src/` nên chứa logic hoặc contract có thể dùng lại; `scripts/` phù hợp cho
  entrypoint local, probe và checkpoint.
- Không hard-code lặp lại các contract như topic, table, path hoặc checkpoint ở
  nhiều nơi.
- Refactor metadata nhỏ không phải milestone riêng, nhưng là điều kiện để project
  không bị rối khi số lượng job tăng lên.

## Bước 3: Khám phá dữ liệu Bluesky Jetstream

**Mục tiêu**

Kết nối tới Bluesky Jetstream, thu sample event thật và quan sát các collection
trong scope hiện tại.

**Vì sao cần thực hiện**

Nguồn streaming thường có schema bán cấu trúc, field thiếu và shape khác nhau theo
loại event. Trước khi thiết kế Kafka message, Bronze layout hoặc Silver schema,
cần quan sát dữ liệu thật thay vì giả định schema lý tưởng.

**Kết quả sau khi hoàn thành**

Project có sample/probe để quan sát các collection chính:

- `app.bsky.feed.post`
- `app.bsky.feed.like`
- `app.bsky.feed.repost`
- `app.bsky.graph.follow`

Tài liệu schema notes ghi lại các quan sát quan trọng như delete event thiếu
`record`, `record.subject` có shape khác nhau giữa like/repost/follow, và post có
thể là reply nếu có `record.reply`.

**Các file liên quan**

- `scripts/discovery/jetstream_probe.py`
- `scripts/discovery/analyze_sample.py`
- `docs/jetstream-schema-notes.md`
- `data/probe/jetstream_sample.jsonl`

**Kiến thức cần ghi nhớ**

- Schema profiling là bước thiết kế, không chỉ là thao tác debug.
- Delete event cần xử lý missing field cẩn thận.
- Cùng tên field như `subject` có thể mang nghĩa và kiểu dữ liệu khác nhau tùy
  collection.

## Bước 4: Thiết kế event envelope và normalization contract

**Mục tiêu**

Bọc raw Jetstream event vào event envelope nội bộ và chuẩn bị logic normalize
sample thành record phẳng để downstream dễ xử lý.

**Vì sao cần thực hiện**

Raw event từ nguồn bên ngoài không nên được publish trực tiếp mà không có metadata
nội bộ. Event envelope giúp pipeline giữ thông tin như source, thời điểm nhận,
event kind, collection, operation, repository DID và raw payload. Normalization
giúp kiểm chứng cách trích các field quan trọng trước khi đưa vào Spark/Silver.

**Kết quả sau khi hoàn thành**

Project có event envelope contract và test cho các case create/delete/missing
field. Sau này envelope được bổ sung `event_kind` để phân biệt commit events với
các event không có collection/operation, giúp Bronze tách đúng event family.

**Các file liên quan**

- `src/bluesky_pipeline/transforms/event_envelope.py`
- `src/bluesky_pipeline/transforms/normalize_event.py`
- `scripts/discovery/normalize_sample.py`
- `tests/test_event_envelope.py`
- `docs/jetstream-schema-notes.md`

**Kiến thức cần ghi nhớ**

- Event envelope là contract giữa ingestion và downstream, nên thay đổi envelope
  phải có test hoặc checkpoint.
- Giữ raw payload giúp audit, replay và reprocess khi normalization thay đổi.
- Envelope không tạo ra exactly-once guarantee; guarantee phải xét toàn bộ
  end-to-end path.

## Bước 5: Dựng Kafka local và kiểm chứng publish/consume

**Mục tiêu**

Dựng Kafka local, tạo raw event topic và kiểm chứng producer/consumer bằng sample
event.

**Vì sao cần thực hiện**

Kafka là buffer/event log giữa ingestion gateway và compute layer. Trước khi nối
live Jetstream, cần kiểm chứng topic, bootstrap server, message key/value và khả
năng replay/consume bằng sample nhỏ.

**Kết quả sau khi hoàn thành**

Docker Compose chạy Kafka local. Project có script publish một event và publish
batch sample vào topic `bluesky.raw.events.v2`. Message key dùng
`repository_did` để giữ ordering tương đối theo repository.

**Các file liên quan**

- `docker-compose.yml`
- `src/bluesky_pipeline/config/kafka.py`
- `scripts/ingestion/publish_sample_to_kafka.py`
- `scripts/ingestion/publish_sample_batch_to_kafka.py`
- `data/probe/jetstream_sample.jsonl`

**Kiến thức cần ghi nhớ**

- Kafka trong local không phải production Kafka cluster.
- Replication factor 1 phù hợp local nhưng không chịu lỗi broker.
- Producer có buffer nội bộ, nên cần flush khi muốn đảm bảo message đã được gửi
  trước khi process kết thúc.

## Bước 6: Xây dựng live ingestion gateway

**Mục tiêu**

Kết nối live tới Bluesky Jetstream, bọc event bằng envelope và publish vào Kafka
raw topic.

**Vì sao cần thực hiện**

Đây là lát cắt streaming ingestion đầu tiên của project:

`Bluesky Jetstream -> Python ingestion gateway -> Kafka`.

Gateway cần giữ trách nhiệm mỏng: kết nối nguồn, retry/reconnect, thêm metadata
ingestion và publish event. Business transform sâu nên để downstream xử lý.

**Kết quả sau khi hoàn thành**

Ingestion gateway đọc các collection trong scope, publish envelope JSON vào Kafka,
có cấu hình qua environment variables, structured logging, bounded retry và
graceful shutdown cơ bản.

**Các file liên quan**

- `src/bluesky_pipeline/ingestion_gateway.py`
- `src/bluesky_pipeline/transforms/event_envelope.py`
- `src/bluesky_pipeline/config/kafka.py`
- `docker-compose.yml`

**Kiến thức cần ghi nhớ**

- Gateway chạy dài hạn phải xử lý reconnect và flush producer khi dừng.
- WebSocket public có thể bị đóng không sạch; retry là hành vi bình thường nếu có
  giới hạn.
- Không hard-code credential hoặc endpoint nhạy cảm trong code.

## Bước 7: Kết nối Spark Structured Streaming với Kafka

**Mục tiêu**

Kiểm chứng Spark đọc được raw events từ Kafka, parse event envelope JSON và tạo
DataFrame có các field cần thiết.

**Vì sao cần thực hiện**

Sau khi Kafka đã nhận event, tầng compute phải chứng minh đọc được cùng topic đó.
Đây là cầu nối giữa message broker và các tầng Bronze/Silver/Gold.

**Kết quả sau khi hoàn thành**

Project có Spark entrypoint đọc Kafka raw topic, cast Kafka value thành string,
parse event envelope bằng schema Spark và in/kiểm tra dữ liệu. Kafka connector và
Spark session được cấu hình dùng chung.

**Các file liên quan**

- `scripts/ingestion/spark_read_kafka_raw.py`
- `src/bluesky_pipeline/config/spark.py`
- `src/bluesky_pipeline/schemas/bronze_schemas.py`
- `src/bluesky_pipeline/config/kafka.py`

**Kiến thức cần ghi nhớ**

- Spark Structured Streaming đọc Kafka theo micro-batch, không phải từng event
  cập nhật UI ngay lập tức.
- Schema Spark nên được tách ra module dùng chung khi nhiều job cùng parse cùng
  contract.
- Cần phân biệt Kafka timestamp, source event time và ingestion time.

## Bước 8: Xây dựng Bronze raw data lake trên MinIO

**Mục tiêu**

Ghi raw event envelope từ Kafka vào Bronze Parquet trên MinIO, giữ raw JSON gốc và
partition dữ liệu để dễ quan sát/replay.

**Vì sao cần thực hiện**

Bronze là tầng lưu dữ liệu gần nguồn, phục vụ audit, replay và reprocess khi logic
Silver thay đổi. Giai đoạn đầu dùng Parquet partitioned để đơn giản, dễ inspect
trên MinIO và phù hợp append-only.

**Kết quả sau khi hoàn thành**

Bronze được ghi lên MinIO theo event family, bao gồm commit events và các nhóm
khác. Layout có partition theo `event_kind`, `collection` và thời gian ingest khi
cần. Script đọc Bronze kiểm chứng được counts, schema và các event family.

**Các file liên quan**

- `scripts/ingestion/spark_read_kafka_raw.py`
- `scripts/discovery/read_bronze_parquet.py`
- `src/bluesky_pipeline/schemas/bronze_tables.py`
- `src/bluesky_pipeline/config/spark.py`
- `docker-compose.yml`

**Kiến thức cần ghi nhớ**

- Bronze nên giữ raw payload để không mất khả năng audit/replay.
- Không phải mọi event đều có `collection`; `event_kind` giúp tránh gom nhầm
  non-commit events vào partition `null`.
- Bronze chưa cần Iceberg ngay nếu mục tiêu chính là append và quan sát raw data.

## Bước 9: Thiết kế và build Silver v1

**Mục tiêu**

Chuẩn hóa Bronze commit events thành các Silver datasets có schema rõ ràng:
posts, engagements, follows và deleted records.

**Vì sao cần thực hiện**

Bronze giữ dữ liệu gần nguồn nên chưa tối ưu cho phân tích trực tiếp. Silver có
thể vẫn giữ cùng grain event-level với Bronze, nhưng dữ liệu được parse, chuẩn
hóa kiểu dữ liệu, tách theo domain và đặt semantics rõ ràng hơn. Từ nền Silver
này, downstream có thể build Gold modeled layer hoặc Gold aggregate/serving marts
ổn định.

**Kết quả sau khi hoàn thành**

Silver v1 có các datasets:

- `silver_posts`
- `silver_engagements`
- `silver_follows`
- `silver_deleted_records`

Silver v1 ban đầu được thử bằng Parquet prototype, sau đó được chuẩn hóa thành
Iceberg source of truth. `docs/silver-schema-v1.md` ghi lại schema và ý nghĩa
từng bảng.

**Các file liên quan**

- `docs/silver-schema-v1.md`
- `scripts/discovery/profile_bronze_commit_events.py`
- `src/bluesky_pipeline/transforms/silver_transformations.py`
- `scripts/lakehouse/build_iceberg_silver_v1.py`
- `scripts/lakehouse/check_iceberg_silver_v1.py`

**Kiến thức cần ghi nhớ**

- Silver là nơi chuẩn hóa nghĩa của field, không chỉ đổi định dạng lưu trữ.
- Bronze và Silver không bắt buộc khác nhau về grain; điểm khác chính là chất
  lượng dữ liệu, schema, typing và semantics.
- Reply có thể được nhận diện từ `record.reply.root.uri`.
- Delete event cần bảng riêng vì thường thiếu `record` đầy đủ nhưng vẫn quan trọng
  cho phân tích churn/moderation.

## Bước 10: Chuẩn hóa code dùng chung và cấu trúc repo

**Mục tiêu**

Tách các schema, transformation logic, table names, Kafka config và helper HTTP
vào module dùng chung; đồng thời tổ chức lại `scripts/` theo vai trò để repo dễ
đọc hơn.

**Vì sao cần thực hiện**

Khi số lượng script tăng lên, nếu mỗi script tự hard-code path/table/schema thì
repo rất dễ lệch contract. Chuẩn hóa metadata giúp các build/check/load scripts
dùng cùng một nguồn sự thật.

Ngoài ra, cần phân biệt rõ hai loại code:

- `src/bluesky_pipeline/`: package Python chính, chứa logic pipeline có thể dùng
  lại như schema, config, transformation, table contract và helper client.
- `scripts/`: entrypoint để chạy local, demo, debug hoặc kiểm tra từng phần của
  pipeline.

**Kết quả sau khi hoàn thành**

Project có module dùng chung cho Bronze schema, Bronze paths, Silver
transformations, Gold table contracts, Iceberg config, ClickHouse helper và Kafka
config. Các script Silver/Gold được refactor để dùng contract chung thay vì lặp
hard-code.

Thư mục `scripts/` được tách theo vai trò:

- `scripts/discovery/`: probe, phân tích sample và smoke test.
- `scripts/ingestion/`: publish Kafka sample và ghi Bronze.
- `scripts/lakehouse/`: build/check Silver Iceberg và lakehouse path runner.
- `scripts/gold/`: build/load/reconcile Gold serving.
- `scripts/realtime/`: realtime fast path và realtime metrics check.
- `scripts/platform/`: setup các object phục vụ platform, ví dụ ClickHouse tables.

**Các file liên quan**

- `src/bluesky_pipeline/schemas/bronze_schemas.py`
- `src/bluesky_pipeline/schemas/bronze_tables.py`
- `src/bluesky_pipeline/transforms/silver_transformations.py`
- `src/bluesky_pipeline/schemas/gold_tables.py`
- `src/bluesky_pipeline/config/iceberg.py`
- `src/bluesky_pipeline/clients/clickhouse.py`
- `src/bluesky_pipeline/config/kafka.py`
- `docs/script-inventory.md`

**Kiến thức cần ghi nhớ**

- Metadata có tính contract nên đưa vào module dùng chung ngay khi được nhiều file
  sử dụng.
- Refactor không phải mục tiêu riêng, nhưng là chi phí cần trả để pipeline tiếp
  tục mở rộng an toàn.
- Helper ClickHouse nên hiển thị lỗi HTTP body để debug DDL/INSERT rõ nguyên nhân.
- `scripts/` nên mỏng và thiên về orchestration; logic reusable nên nằm trong
  `src/bluesky_pipeline/`.
- `src/` là container source code; `src/bluesky_pipeline/` mới là package Python
  chính của project.

## Bước 11: Xây dựng ClickHouse serving layer cho Gold aggregates

**Mục tiêu**

Dựng ClickHouse local, tạo serving tables và load các Gold aggregates đầu tiên
phục vụ query/dashboard.

**Vì sao cần thực hiện**

Silver phù hợp làm dữ liệu chuẩn hóa/lakehouse, nhưng dashboard không nên phải
lặp lại các phép `JOIN`, `GROUP BY` hoặc tính toán metric nặng ở mỗi lần refresh.
ClickHouse đóng vai trò serving/metric store cho các bảng đã được chuẩn bị sẵn để
Grafana query nhanh.

**Kết quả sau khi hoàn thành**

ClickHouse có database `bluesky` và các bảng aggregate/serving đầu tiên như:

- `gold_event_volume_by_type`
- `gold_post_engagement_summary`

Project có script tạo DDL, load dữ liệu vào ClickHouse, checkpoint
reconciliation và Grafana datasource/panels cho Gold serving v1.

**Các file liên quan**

- `scripts/platform/create_clickhouse_gold_tables.py`
- `scripts/gold/build/build_event_volume_from_iceberg.py`
- `scripts/gold/build/build_post_engagement_summary_from_iceberg.py`
- `scripts/gold/load/load_event_volume_to_clickhouse.py`
- `scripts/gold/load/load_post_engagement_summary_to_clickhouse.py`
- `scripts/gold/check/check_event_volume_reconciliation.py`
- `scripts/gold/check/check_post_engagement_reconciliation.py`
- `scripts/gold/check/check_serving_v1.py`
- `docker-compose.yml`

**Kiến thức cần ghi nhớ**

- ClickHouse là serving/metric store, không phải toàn bộ tầng Gold và không phải
  source of truth duy nhất.
- Các serving tables trong ClickHouse phải rebuild được từ Gold modeled layer
  hoặc từ Silver khi Gold modeled chưa hoàn chỉnh.
- Gold aggregated/serving tồn tại để giảm tải dashboard: thay vì tính metric từ
  dữ liệu chi tiết mỗi lần Grafana refresh, pipeline tính trước các bảng metric
  phù hợp cho truy vấn lặp lại.
- Dashboard JSON chưa cần commit ở giai đoạn thử nghiệm; khi dashboard hoàn chỉnh
  có thể screenshot/link hoặc export sau.

## Bước 12: Chuẩn hóa Silver bằng Apache Iceberg

**Mục tiêu**

Build Silver v1 trực tiếp từ Bronze thành Apache Iceberg trên MinIO.

**Vì sao cần thực hiện**

Parquet prototype phù hợp để học và kiểm chứng nhanh ở giai đoạn đầu, nhưng
Silver chính thức cần table semantics, snapshot, schema evolution và khả năng
quản lý dữ liệu rõ ràng hơn. Iceberg phù hợp với vai trò Silver source of truth.

**Kết quả sau khi hoàn thành**

Project smoke test được Iceberg table trên MinIO, build được Silver Iceberg v1
trực tiếp từ Bronze thông qua `silver_transformations.py`, và reconcile Iceberg
với expected metrics được tính lại từ Bronze. Các script Silver Parquet prototype
được loại bỏ để luồng chính không bị nhầm lẫn.

**Các file liên quan**

- `scripts/discovery/smoke_test_iceberg_minio.py`
- `scripts/lakehouse/build_iceberg_silver_v1.py`
- `scripts/lakehouse/check_iceberg_silver_v1.py`
- `src/bluesky_pipeline/transforms/silver_transformations.py`
- `src/bluesky_pipeline/config/iceberg.py`
- `src/bluesky_pipeline/config/spark.py`

**Kiến thức cần ghi nhớ**

- Iceberg nên bắt đầu từ Silver, nơi cần table semantics rõ hơn Bronze.
- Transformation logic nên nằm trong module dùng chung để batch build, check và
  future orchestration không phải copy cùng một logic.
- Không nên giữ mã prototype song song quá lâu khi đã có luồng chính thức.

## Bước 13: Chuẩn hóa Gold aggregate refresh từ Silver Iceberg

**Mục tiêu**

Build các Gold aggregates đầu tiên từ Silver Iceberg và load vào ClickHouse bằng
entrypoint refresh chính thức.

**Vì sao cần thực hiện**

Sau khi Silver Iceberg trở thành source of truth, Gold serving không nên phụ thuộc
vào Parquet prototype. Ở thời điểm này, luồng đã triển khai là:

`Silver Iceberg -> Gold aggregate/serving mart -> ClickHouse`.

Đây là bước thực dụng để có dashboard và reconciliation chạy được trước. Về mặt
kiến trúc lakehouse đầy đủ, Gold không chỉ là aggregate metric. Target lâu dài là:

`Silver Iceberg -> Gold modeled tables -> Trino`.

Từ Gold modeled, pipeline tiếp tục tạo serving marts cho dashboard:

`Gold modeled tables -> Gold aggregate/serving marts -> ClickHouse`.

Gold modeled tables có thể là fact/dim hoặc semantic marts phục vụ phân tích sâu.
Trino là query engine để query trực tiếp Gold modeled trên Iceberg. Gold
aggregate/serving marts là lớp tính trước metric từ dữ liệu đã model để phục vụ
Grafana và các truy vấn lặp lại với độ trễ thấp hơn.

Trong lakehouse path, Spark xử lý dữ liệu ở các đoạn chính:

- `Bronze -> Spark -> Silver Iceberg`
- `Silver Iceberg -> Spark -> Gold modeled tables`
- `Gold modeled tables -> Trino -> ad-hoc analytics`
- `Gold modeled tables -> Spark -> Gold aggregate/serving marts -> ClickHouse`

ClickHouse chỉ là serving layer cho lakehouse marts, không phải nơi xử lý dữ liệu
gốc chính.

**Kết quả sau khi hoàn thành**

Project có scripts build aggregate event volume và post engagement summary từ Silver
Iceberg, load vào ClickHouse và entrypoint `refresh_serving_from_iceberg.py`
để chạy refresh Gold serving v1. Checkpoint cuối của Gold serving đối chiếu
ClickHouse với Silver Iceberg source of truth, không còn quay lại Silver Parquet
prototype. Project cũng có entrypoint `run_lakehouse_path.py` để chạy
toàn bộ lakehouse path theo thứ tự: build Silver Iceberg, check
Silver Iceberg, refresh Gold aggregate/serving và check ClickHouse serving marts.
Gold modeled layer chưa được tách thành bảng riêng ở bước này; đây là phần cần
hoàn thiện tiếp khi chuyển từ metric đơn giản sang data modeling đầy đủ.

**Các file liên quan**

- `scripts/gold/build/build_event_volume_from_iceberg.py`
- `scripts/gold/build/build_post_engagement_summary_from_iceberg.py`
- `scripts/gold/refresh/refresh_serving_from_iceberg.py`
- `scripts/lakehouse/run_lakehouse_path.py`
- `scripts/gold/check/check_serving_v1.py`
- `src/bluesky_pipeline/schemas/gold_tables.py`

**Kiến thức cần ghi nhớ**

- Tên script nên phản ánh đúng vai trò; `refresh` phù hợp hơn `rebuild` khi đây là
  luồng vận hành chính chứ không chỉ là thao tác sửa lỗi.
- Gold aggregate staging trên MinIO là output trung gian; ClickHouse là
  serving/metric store cho query/dashboard.
- Gold modeled layer là hướng mở rộng cần thiết khi project chuyển từ metric đơn
  giản sang phân tích chuyên sâu bằng fact/dim hoặc semantic marts.
- Gold lakehouse path ưu tiên khả năng rebuild/reconcile hơn latency thấp.
- Khi đã chốt Silver Iceberg là source of truth, reconciliation cuối cùng phải
  đọc từ Iceberg để phản ánh đúng kiến trúc chính thức.
- Một entrypoint end-to-end giúp demo và kiểm tra luồng nhiều lớp dễ hơn, nhưng
  vẫn nên giữ các script con để debug từng tầng khi có lỗi.

## Bước 14: Bổ sung reconciliation và checkpoint chất lượng

**Mục tiêu**

Tạo các checkpoint CLI để kiểm chứng Silver, Gold staging và ClickHouse serving
khớp nhau.

**Vì sao cần thực hiện**

Data pipeline cần kiểm tra hành vi, không chỉ kiểm tra service đang chạy.
Reconciliation giúp phát hiện lệch count giữa Silver source of truth, Gold output
và ClickHouse serving layer.

Cơ chế checkpoint trong project là integration/data quality check: script đọc dữ
liệu thật từ Bronze, Iceberg hoặc ClickHouse; tính expected metrics từ source of
truth; query actual metrics ở output layer; sau đó so sánh expected với actual.
Nếu có mismatch, script dừng bằng exception. Đây không phải unit test dùng mock,
và cũng chưa phải full automated end-to-end test tự khởi động toàn bộ pipeline.

**Kết quả sau khi hoàn thành**

Project có checkpoint cho Silver v1, Iceberg Silver v1, Gold event volume, Gold
post engagement, ClickHouse serving marts và Gold serving tổng hợp. Các script
này phục vụ debug local và demo pipeline. Với Gold serving v1, expected metrics
được tính từ
Silver Iceberg hoặc Gold staging được build từ Silver Iceberg trước khi so sánh
với ClickHouse. Project cũng có checkpoint tổng hợp
`check_lakehouse_path.py` để kiểm tra lakehouse path hiện
có mà không build hoặc refresh lại dữ liệu.

**Các file liên quan**

- `scripts/lakehouse/check_iceberg_silver_v1.py`
- `scripts/gold/check/check_event_volume_reconciliation.py`
- `scripts/gold/check/check_post_engagement_reconciliation.py`
- `scripts/gold/check/check_serving_v1.py`
- `scripts/lakehouse/check_lakehouse_path.py`

**Kiến thức cần ghi nhớ**

- Reconciliation nên so sánh theo business metric, không chỉ so sánh row count
  tổng.
- Checkpoint pipeline kiểm chứng output thật của từng tầng, nên hữu ích để bắt
  lỗi cấu hình, schema, load thiếu dữ liệu hoặc lệch transformation.
- CLI checkpoint rất hữu ích trước khi có orchestration hoặc dashboard hoàn chỉnh.
- Nên tách lệnh `run` có ghi dữ liệu với lệnh `check` chỉ kiểm tra trạng thái
  hiện có, để demo và debug rõ ràng hơn.
- Không nên để checkpoint chính phụ thuộc vào prototype cũ sau khi pipeline đã
  chuyển sang bảng Iceberg chính thức.
- Không nên tuyên bố dữ liệu đúng nếu chưa có output kiểm chứng.

## Bước 15: Chốt hai serving paths: realtime và lakehouse

**Mục tiêu**

Chốt kiến trúc dashboard có hai path song song:

- Fast path gần thời gian thực.
- Lakehouse path có khả năng rebuild.

**Vì sao cần thực hiện**

Sơ đồ tuyến tính `Kafka -> Bronze -> Silver -> Gold -> Grafana` dễ gây hiểu nhầm
rằng mọi dashboard đều phải đi qua Silver trước. Với realtime metrics cần latency
thấp, hệ thống dùng path riêng từ Kafka sang ClickHouse. Silver Iceberg vẫn giữ
vai trò source of truth cho dữ liệu lịch sử, backfill và rebuild.

**Kết quả sau khi hoàn thành**

`docs/tong-quan-du-an.md` mô tả rõ:

- Fast path: `Kafka -> Spark Structured Streaming -> ClickHouse realtime marts -> Grafana`.
- Lakehouse path: `Kafka -> Bronze -> Silver Iceberg`, trong đó Bronze -> Silver
  chạy theo streaming để dữ liệu sạch được cập nhật liên tục.
- Gold lakehouse path mục tiêu:
  `Silver Iceberg -> Gold modeled tables -> Trino`.
- Serving path từ lakehouse:
  `Gold modeled tables -> Gold aggregate/serving marts -> ClickHouse -> Grafana`.
  Trong đó Gold modeled và Gold aggregate/serving có thể schedule chậm hơn bằng
  Airflow vì ưu tiên data modeling, aggregate, rebuild và reconciliation.
- Observability path: logs/metrics/checkpoints phục vụ vận hành.

Kiến trúc được mô tả là lambda-like streaming lakehouse architecture: có fast path
cho realtime metrics, có lakehouse path cho backfill, correction và
rebuild, nhưng không phải Lambda Architecture cổ điển với hai codebase hoàn toàn
tách biệt.

**Các file liên quan**

- `docs/tong-quan-du-an.md`
- `docs/quy-trinh-xay-dung-pipeline.md`
- `scripts/realtime/stream_metrics_to_clickhouse.py`
- `scripts/gold/refresh/refresh_serving_from_iceberg.py`

**Kiến thức cần ghi nhớ**

- Fast path không thay thế Silver Iceberg.
- ClickHouse realtime marts là serving tables, không phải source of truth duy
  nhất.
- Lakehouse path không có nghĩa mọi tầng đều chậm: Bronze và Silver là
  các tầng streaming/continuous, còn Gold có thể refresh chậm hơn.
- Near-real-time luôn có độ trễ từ Spark trigger, ClickHouse insert và Grafana
  refresh.
- Đây không còn nên gọi là Kappa-like architecture: project có lakehouse path
  riêng với Bronze/Silver/Gold Iceberg và Trino query layer. Kafka vẫn là event
  backbone chung, nhưng source of truth phân tích nằm ở lakehouse.
- Hai consumer group cùng đọc Kafka có thể lệch tạm thời; cần kiểm soát bằng lag,
  freshness, reconciliation và khả năng rebuild ClickHouse từ Iceberg.

## Bước 16: Xây dựng realtime fast path vào ClickHouse

**Mục tiêu**

Xây dựng Spark streaming job đọc Kafka, aggregate realtime metrics theo phút và
ghi vào ClickHouse realtime marts.

**Vì sao cần thực hiện**

Lakehouse path phục vụ baseline/rebuild, nhưng dashboard realtime cần dữ liệu cập
nhật nhanh hơn mà không chờ lakehouse modeling hoặc Gold aggregate refresh.

**Kết quả sau khi hoàn thành**

Project có entrypoint `scripts/realtime/stream_metrics_to_clickhouse.py` ghi các
realtime marts:

- `gold_event_volume_1m_stream`
- `gold_content_activity_1m_stream`
- `gold_engagement_1m_stream`
- `gold_network_activity_1m_stream`
- `gold_realtime_stream_batches`

Spark job dùng `foreachBatch`, aggregate trong từng micro-batch và ghi
ClickHouse qua HTTP JSONEachRow. Job cũng có `maxOffsetsPerTrigger` cấu hình qua
`SPARK_KAFKA_MAX_OFFSETS_PER_TRIGGER` để tránh một batch phình quá lớn khi Kafka
có backlog.

**Các file liên quan**

- `scripts/realtime/stream_metrics_to_clickhouse.py`
- `scripts/platform/create_clickhouse_gold_tables.py`
- `scripts/realtime/check_clickhouse_metrics.py`
- `src/bluesky_pipeline/schemas/gold_tables.py`
- `src/bluesky_pipeline/config/kafka.py`

**Kiến thức cần ghi nhớ**

- `foreachBatch` nhận DataFrame tĩnh của micro-batch, nên aggregation bên trong
  hàm là batch aggregation bình thường.
- Hiện tại collect aggregate về Driver chấp nhận được vì metric cardinality thấp.
  Nếu mở rộng sang top user, hashtag hoặc domain, cần chuyển sang connector/JDBC
  hoặc `foreachPartition`.
- `input_rows = count()` phục vụ batch health nhưng là một action bổ sung; workload
  lớn hơn có thể lấy từ streaming progress metrics.
- Semantics hiện tại là at-least-once; chưa tuyên bố exactly-once end-to-end.
- Khi Spark retry một micro-batch, ClickHouse có thể nhận lại cùng dữ liệu nếu
  sink không idempotent. Bảng metric nên có khóa logic như
  `window_start + metric_name + dimension` hoặc cơ chế reconciliation/deduplicate
  rõ ràng.

## Bước 17: Hoàn thiện Grafana realtime dashboard và operational health

**Mục tiêu**

Hiển thị business realtime metrics và operational health trên Grafana.

**Vì sao cần thực hiện**

Dashboard realtime không chỉ cần trả lời “người dùng đang làm gì” mà còn cần trả
lời “pipeline có đang chạy tốt không”. Vì vậy cần cả business panels và health
panels.

**Kết quả sau khi hoàn thành**

Grafana có các nhóm panel:

- Event volume theo event type.
- Content activity: post, original post, reply, post update, post delete.
- Engagement: like, repost, reply.
- Network activity: follow, unfollow, net follow.
- Freshness: lần ghi ClickHouse gần nhất.
- Batch health: batch duration, input rows, rows inserted by metric group,
  seconds since last batch.

Các query time series đã được chỉnh để group theo business key và sort tăng dần
theo thời gian, tránh lỗi Grafana không xử lý được dữ liệu chưa sorted.

**Các file liên quan**

- `docker-compose.yml`
- `scripts/realtime/check_clickhouse_metrics.py`
- `src/bluesky_pipeline/schemas/gold_tables.py`

**Kiến thức cần ghi nhớ**

- Grafana realtime panel vẫn query định kỳ xuống ClickHouse, không stream từng
  event trực tiếp vào trình duyệt.
- `spark_batch_id` chỉ dùng debug; business query không nên group theo batch id.
- Time picker/timezone là nguyên nhân thường gặp khi query có dữ liệu trong
  ClickHouse nhưng Grafana báo no data.
- Grafana time series cần output sort tăng dần theo cột time.

## Bước 18: Chốt trạng thái sau realtime và lakehouse baseline

**Mục tiêu**

Tóm tắt trạng thái của project tại mốc đã có realtime fast path và lakehouse
baseline, trước khi chuyển sang hoàn thiện Gold modeled, Gold analytics marts và
incremental refresh.

**Vì sao cần thực hiện**

Khi project đi qua nhiều prototype, tài liệu cần phản ánh kiến trúc hiện tại thay
vì giữ nguyên mọi bước nhỏ trong quá khứ. Điều này giúp người học ôn tập và trình
bày dự án theo mạch rõ ràng hơn.

**Kết quả sau khi hoàn thành**

Tại mốc này, project có hai path đã chạy được ở local:

- Realtime fast path:
  `Jetstream -> Gateway -> Kafka -> Spark Streaming -> ClickHouse realtime marts -> Grafana`.
- Lakehouse path:
  `Kafka -> Bronze -> Silver Iceberg`, với Bronze -> Silver chạy streaming trong
  live pipeline.
- Gold lakehouse path:
  `Silver Iceberg -> Gold modeled tables -> Trino`.
- Serving path từ lakehouse:
  `Gold modeled tables -> Gold aggregate/serving marts -> ClickHouse lakehouse marts -> Grafana`.

Realtime path đã có business metrics và operational health. Lakehouse path đã có
Silver Iceberg, Gold aggregate refresh và reconciliation. Phần còn cần hoàn thiện
tiếp là tách Gold modeled layer rõ ràng hơn, sau đó mới chuẩn hóa orchestration,
runbook và monitoring nếu milestone yêu cầu.

**Các file liên quan**

- `docs/tong-quan-du-an.md`
- `docs/quy-trinh-xay-dung-pipeline.md`
- `README.md`
- `scripts/realtime/stream_metrics_to_clickhouse.py`
- `scripts/gold/refresh/refresh_serving_from_iceberg.py`
- `scripts/realtime/check_clickhouse_metrics.py`
- `scripts/gold/check/check_serving_v1.py`

**Kiến thức cần ghi nhớ**

- Tài liệu quy trình nên ghi mốc kiến trúc, không ghi mọi command đã chạy.
- Prototype có giá trị học tập, nhưng khi bị thay thế bởi path chính thức thì nên
  được gom vào bài học thay vì giữ thành bước riêng.
- Fast path tối ưu latency; lakehouse path tối ưu độ tin cậy, rebuild và
  reconciliation.
- Với lakehouse analytics, Gold nên được hiểu thành hai vai trò: modeled layer để
  biểu diễn dữ liệu nghiệp vụ và aggregated/serving layer để tăng tốc dashboard.
- Trino là query engine cho Gold modeled Iceberg; ClickHouse là serving/metric
  store cho dashboard.
- README/runbook nên hoàn thiện gần cuối project, khi command và entrypoint đã ổn
  định.

## Bước 19: Bổ sung live demo runner và cleanup dữ liệu local

**Mục tiêu**

Tạo entrypoint chạy live pipeline end-to-end và cơ chế dọn dữ liệu ingest local
an toàn sau các lần demo/thử nghiệm.

**Vì sao cần thực hiện**

Sau khi từng path đã chạy được riêng lẻ, project cần một cách kiểm chứng thực tế:
nguồn live đi vào gateway, Kafka, Spark, ClickHouse và hiển thị trên Grafana mà
không cần bắn event thủ công. Đồng thời, vì đây là project cá nhân chạy local,
dữ liệu ingest lịch sử không cần giữ lâu dài và có thể làm phình MinIO,
ClickHouse, Kafka hoặc checkpoint.

**Kết quả sau khi hoàn thành**

Project có live demo runner để chạy đồng thời ingestion gateway, Bronze writer,
realtime metrics stream và Bronze-to-Silver streaming job. Gateway hỗ trợ live
mode, retry không giới hạn khi cấu hình `MAX_RETRIES=0`, và log tách rõ tổng số
lỗi kết nối với số lần retry liên tiếp để tránh hiểu nhầm lỗi đang tích tụ.

Project cũng có cleanup utility mặc định chạy dry-run trước, chỉ xóa thật khi
truyền flag xác nhận. Cleanup dọn Bronze/Silver/Gold/checkpoints trên MinIO,
drop metadata Iceberg trong Hive Metastore, truncate ClickHouse serving tables và
có tùy chọn purge Kafka raw topic. Cleanup cũng xóa local incremental state trong
`data/state/*.json` để lần chạy mới không dùng lại watermark/marker cũ của lần
trước. Khi cần chạy lại demo từ trạng thái chỉ có dữ liệu mới của lần chạy hiện
tại, cleanup được chạy với cả `--confirm-delete` và `--include-kafka-topic` để
xóa/recreate raw Kafka topic, drop Silver/Gold Iceberg metadata, xóa MinIO paths,
truncate ClickHouse tables và reset incremental state.

Live pipeline đã được chạy lại từ trạng thái sạch sau cleanup: dữ liệu mới đi từ
Jetstream vào Kafka, Spark ghi Bronze, realtime marts và đẩy Bronze mới sang
Silver Iceberg bằng streaming job; Grafana cập nhật theo time range hiện tại,
checkpoint realtime pass và runner dừng được bằng `Ctrl+C`.

**Các file liên quan**

- `scripts/e2e/run_live_pipeline.py`
- `src/bluesky_pipeline/ingestion_gateway.py`
- `scripts/platform/cleanup_ingested_data.py`
- `data/state/`
- `docs/script-inventory.md`

**Kiến thức cần ghi nhớ**

- Live demo runner là entrypoint vận hành local, không thay thế orchestration như
  Airflow trong các workflow batch/backfill có điểm bắt đầu và kết thúc rõ ràng.
- Bronze -> Silver nên chạy theo streaming trong live pipeline; Gold lakehouse
  refresh nên tách riêng và schedule bằng Airflow ở milestone orchestration.
- Cleanup dữ liệu ingest nên có dry-run và flag xác nhận vì đây là thao tác phá
  hủy dữ liệu.
- Muốn reset hoàn toàn cho một lần demo mới thì phải purge cả Kafka raw topic;
  nếu chỉ xóa MinIO/ClickHouse/Iceberg mà giữ Kafka log, Spark consumer có thể
  đọc lại event cũ tùy checkpoint/offset.
- Với incremental jobs, cleanup dữ liệu cũng cần reset local state marker; nếu
  giữ `last_successful_run_at` cũ sau khi xóa data, lần chạy mới có thể bỏ qua
  dữ liệu cần xử lý.
- Khi cleanup Iceberg trong Hive catalog, cần drop table/namespace trong
  metastore trước khi xóa data files trên object storage để tránh metadata trỏ
  tới file không còn tồn tại.
- Truncate ClickHouse giữ lại schema để Grafana dashboard không mất query/table
  contract.
- Xóa dữ liệu trong Docker volume không nhất thiết làm file disk image của WSL
  giảm ngay; compact WSL là thao tác ở tầng hệ điều hành, không phải logic
  pipeline.

## Bước 20: Bổ sung Trino Query Engine cho Lakehouse

**Mục tiêu**

Dựng Trino và Hive Metastore để query trực tiếp các bảng Iceberg bằng SQL, bắt
đầu bằng checkpoint query Silver v1.

**Vì sao cần thực hiện**

Trước bước này, Iceberg đã lưu được Silver nhưng việc kiểm tra và phân tích vẫn
phụ thuộc vào Spark job. Spark phù hợp cho compute, streaming transform, backfill
và build bảng; còn Trino phù hợp cho query SQL tương tác trên lakehouse. Bổ sung
Trino giúp kiến trúc lakehouse đầy đủ hơn: Iceberg là table format, Hive Metastore
là catalog metadata dùng chung, Trino là query engine, ClickHouse vẫn là serving
layer cho dashboard có độ trễ thấp.

**Kết quả sau khi hoàn thành**

Project có thêm service Hive Metastore và Trino trong Docker Compose. Spark
Iceberg được chuyển sang Hive catalog để các bảng Iceberg được đăng ký vào
metastore dùng chung, thay vì chỉ tồn tại dưới dạng Hadoop catalog riêng của
Spark. Trino kết nối được tới catalog lakehouse và query được các bảng Silver v1
trên MinIO.

Checkpoint Trino đã pass với các bảng Silver v1, nghĩa là luồng lakehouse hiện có
không chỉ build được bằng Spark mà còn query được bằng SQL qua query engine độc
lập.

**Các file liên quan**

- `docker-compose.yml`
- `config/hive/core-site.xml`
- `config/trino/catalog/lakehouse.properties`
- `src/bluesky_pipeline/config/iceberg.py`
- `src/bluesky_pipeline/config/spark.py`
- `scripts/lakehouse/check_trino_silver_v1.py`
- `docs/tong-quan-du-an.md`
- `docs/gold-data-model-v1.md`
- `docs/script-inventory.md`
- `README.md`

**Kiến thức cần ghi nhớ**

- Trino query Iceberg cần một catalog metadata dùng chung. Nếu chỉ dùng Hadoop
  catalog riêng trong Spark, Trino không tự biết các bảng đó tồn tại.
- Hive Metastore phải có cấu hình S3A để tạo namespace/table location trên MinIO.
- Khi đổi Iceberg catalog từ Hadoop sang Hive catalog, các bảng Iceberg cần được
  build lại để đăng ký metadata vào metastore mới.
- Trong kiến trúc này, Spark là compute engine; Trino là query engine cho
  lakehouse SQL/ad-hoc analytics; ClickHouse là serving/metric store cho Grafana.
- Lỗi tương thích giữa client Iceberg/Spark và Hive Metastore có thể xuất hiện ở
  tầng RPC/metastore. Trong project này, Hive Metastore 3.1.3 phù hợp hơn Hive 4
  cho stack local hiện tại.

## Bước 21: Thiết kế contract Gold modeled v1

**Mục tiêu**

Xác định các bảng Gold modeled đầu tiên cho lakehouse path và đưa tên namespace,
table vào metadata dùng chung của project.

**Vì sao cần thực hiện**

Trước đó, Gold trong project chủ yếu là các bảng aggregate/serving để Grafana
query nhanh. Cách này phù hợp cho fast path và các metric đơn giản, nhưng chưa đủ
để phục vụ phân tích chuyên sâu. Lakehouse path cần một lớp Gold modeled rõ ràng,
ví dụ fact/dim hoặc semantic marts, để Trino query trực tiếp và để các bảng
aggregate/serving trong ClickHouse có nguồn rebuild ổn định.

**Kết quả sau khi hoàn thành**

Project có tài liệu thiết kế Gold modeled v1 và contract Iceberg dùng chung cho
namespace `gold_v1` với các bảng:

- `gold_dim_actors`
- `gold_dim_posts`
- `gold_fact_content_events`
- `gold_fact_engagement_events`
- `gold_fact_network_events`

Bước này mới chốt contract và metadata. Transformation build dữ liệu từ Silver
Iceberg sang Gold modeled Iceberg sẽ được triển khai ở bước tiếp theo.

**Các file liên quan**

- `docs/gold-data-model-v1.md`
- `src/bluesky_pipeline/config/iceberg.py`

**Kiến thức cần ghi nhớ**

- Gold modeled không đồng nghĩa với aggregate metric. Gold modeled là lớp mô hình
  phân tích có business semantics rõ ràng.
- Gold aggregate/serving trong ClickHouse nên được xem là output phục vụ dashboard
  và phải rebuild được từ Gold modeled khi model đã hoàn chỉnh.
- Table/namespace contract nên đặt trong module dùng chung trước khi viết nhiều
  script build/check để tránh hard-code lệch nhau.

## Bước 22: Viết transformation Gold modeled v1

**Mục tiêu**

Viết logic dùng chung để chuyển các bảng Silver event-level thành các bảng Gold
modeled v1.

**Vì sao cần thực hiện**

Gold modeled là lớp data modeling của lakehouse path. Nếu viết logic trực tiếp
trong từng script build/check, các rule nghiệp vụ như xác định actor, chọn trạng
thái post mới nhất, dựng content event type hoặc phân loại follow/delete sẽ dễ bị
lặp và lệch nhau. Đưa transformation vào module dùng chung giúp build, check,
reconciliation và orchestration sau này dùng cùng một logic.

**Kết quả sau khi hoàn thành**

Project có module transformation tạo 5 DataFrame Gold modeled từ mapping Silver
DataFrame:

- `gold_dim_actors`
- `gold_dim_posts`
- `gold_fact_content_events`
- `gold_fact_engagement_events`
- `gold_fact_network_events`

Module này chưa ghi dữ liệu. Việc ghi ra Iceberg được tách sang script build ở
bước tiếp theo.

**Các file liên quan**

- `src/bluesky_pipeline/transforms/gold_transformations.py`
- `src/bluesky_pipeline/config/iceberg.py`
- `docs/gold-data-model-v1.md`

**Kiến thức cần ghi nhớ**

- Transformation dùng chung nên trả về DataFrame, không tự ghi output, để dễ tái
  sử dụng trong build/check/test.
- Dimension như `gold_dim_posts` cần chọn trạng thái mới nhất theo key, còn fact
  tables giữ grain event-level.
- Delete event thường thiếu record body, nên Gold v1 phải join lại state đã biết
  khi cần suy ra `is_reply` hoặc `target_actor_did`.
- Các event id trong fact có thể dựng deterministic từ business key, event type
  và `jetstream_time_us`, nhưng đây chưa phải exactly-once guarantee end-to-end.

## Bước 23: Build Gold modeled v1 trên Iceberg

**Mục tiêu**

Materialize các DataFrame Gold modeled v1 thành bảng Iceberg trong namespace
`gold_v1`.

**Vì sao cần thực hiện**

Transformation chỉ mô tả logic dữ liệu trong code. Để lakehouse path thật sự có
lớp Gold modeled, pipeline cần ghi các bảng này vào Iceberg catalog để Spark,
Trino và các job downstream có thể đọc lại như một dataset ổn định.

**Kết quả sau khi hoàn thành**

Project build được 5 bảng Gold modeled Iceberg từ Silver Iceberg:

- `gold_dim_actors`
- `gold_dim_posts`
- `gold_fact_content_events`
- `gold_fact_engagement_events`
- `gold_fact_network_events`

Script build đã chạy thành công và đọc lại được row count từ Iceberg catalog cho
từng bảng. Điều này xác nhận Gold modeled v1 không chỉ tồn tại trong logic
transformation, mà đã được materialize thành lakehouse tables.

**Các file liên quan**

- `scripts/lakehouse/build_gold_modeled_v1.py`
- `src/bluesky_pipeline/transforms/gold_transformations.py`
- `src/bluesky_pipeline/config/iceberg.py`
- `docs/script-inventory.md`

**Kiến thức cần ghi nhớ**

- Build script nên đọc Silver Iceberg qua catalog, apply transformation dùng
  chung rồi ghi Gold Iceberg, không đọc trực tiếp từ Bronze.
- Trong môi trường local, `DROP TABLE` rồi `CREATE` giúp rebuild model dễ hiểu
  khi schema còn thay đổi. Production thường cần chiến lược incremental,
  partition overwrite hoặc snapshot management rõ ràng hơn.
- Sau khi materialize Gold modeled, bước kiểm chứng tiếp theo không chỉ là Spark
  đọc lại count, mà còn cần Trino query được namespace/table để xác nhận query
  engine layer hoạt động với Gold.
- Fact event id nên đại diện cho từng event quan sát được, không chỉ business
  entity. Với network fact, `follow_create` cần kết hợp `follow_uri`,
  `network_event_type` và `jetstream_time_us` để tránh trùng key khi cùng follow
  record xuất hiện nhiều lần trong observed stream.

## Bước 24: Kiểm chứng Gold modeled v1 bằng Trino

**Mục tiêu**

Kiểm tra Trino query được namespace `gold_v1` và các bảng Gold modeled Iceberg
vừa build.

**Vì sao cần thực hiện**

Spark build thành công chỉ chứng minh compute engine ghi được bảng Iceberg. Với
kiến trúc lakehouse, Gold modeled còn phải query được qua query engine độc lập để
phục vụ ad-hoc SQL và phân tích chuyên sâu. Vì vậy cần checkpoint Trino cho Gold,
tương tự checkpoint đã làm với Silver.

**Kết quả sau khi hoàn thành**

Trino nhìn thấy namespace `gold_v1`, liệt kê được 5 bảng Gold modeled, đọc được
row count và kiểm tra key chính của từng bảng không null, không duplicate:

- `gold_dim_actors.actor_did`
- `gold_dim_posts.post_uri`
- `gold_fact_content_events.content_event_id`
- `gold_fact_engagement_events.engagement_event_id`
- `gold_fact_network_events.network_event_id`

Checkpoint kết thúc với `Trino Gold modeled v1 check passed`, xác nhận luồng
`Silver Iceberg -> Spark build Gold modeled -> Trino query Gold modeled` đã chạy
được.

**Các file liên quan**

- `scripts/lakehouse/check_trino_gold_modeled_v1.py`
- `scripts/lakehouse/build_gold_modeled_v1.py`
- `src/bluesky_pipeline/config/iceberg.py`
- `docs/script-inventory.md`

**Kiến thức cần ghi nhớ**

- Query engine checkpoint nên kiểm tra cả metadata visibility và đọc dữ liệu thật.
- Row count chỉ là kiểm tra cơ bản; key null/duplicate giúp bắt lỗi data modeling
  quan trọng hơn.
- Output Trino CLI có thể là CSV có quote, nên script checkpoint cần parse đúng
  định dạng thay vì tự split chuỗi thủ công.
- Khi Gold modeled đã query được bằng Trino, bước tiếp theo là refactor các bảng
  Gold aggregate/serving để đọc từ Gold modeled thay vì đọc trực tiếp từ Silver.

## Bước 25: Thiết kế Gold analytics metrics v1

**Mục tiêu**

Chốt bộ metric Gold aggregate/serving có giá trị phân tích cho lakehouse path
trước khi implement bảng ClickHouse mới.

**Vì sao cần thực hiện**

Nếu chỉ refactor các bảng aggregate cũ như event volume hoặc post engagement
summary, Gold aggregate sẽ không khác nhiều so với fast path. Lakehouse path cần
các metric trả lời được câu hỏi sâu hơn về content quality, engagement quality,
conversation dynamics, actor behavior và network activity. Vì vậy cần thiết kế
metric contract trước khi viết transformation và DDL.

**Kết quả sau khi hoàn thành**

Project có tài liệu `docs/gold-analytics-metrics-v1.md` mô tả các câu hỏi phân
tích, grain, source Gold modeled, cột đề xuất và checkpoint cho các serving marts:

- `gold_post_performance`
- `gold_content_quality_hourly`
- `gold_thread_conversation_summary`
- `gold_actor_activity_daily`
- `gold_network_growth_daily`

Tài liệu cũng chốt thứ tự implement, trong đó `gold_post_performance` là bảng nên
làm đầu tiên vì tạo insight rõ nhất và có thể thay thế bảng
`gold_post_engagement_summary` cũ.

**Các file liên quan**

- `docs/gold-analytics-metrics-v1.md`
- `docs/gold-data-model-v1.md`
- `docs/tong-quan-du-an.md`
- `README.md`

**Kiến thức cần ghi nhớ**

- Gold aggregate/serving không nên chỉ là event count; nó nên trả lời câu hỏi
  business/analytics cụ thể.
- Fast path tối ưu freshness; lakehouse aggregate tối ưu insight, khả năng
  rebuild và metric consistency.
- Trước khi build dashboard, cần xác định grain, metric formula và checkpoint để
  tránh mỗi panel tự định nghĩa metric một kiểu.

## Bước 26: Implement Gold post performance serving mart

**Mục tiêu**

Build serving mart phân tích performance từng post từ Gold modeled Iceberg, load
vào ClickHouse và đưa vào checkpoint tổng hợp của lakehouse path.

**Vì sao cần thực hiện**

`gold_post_engagement_summary` cũ chỉ đếm like/repost đơn giản. Để Gold aggregate
có giá trị phân tích hơn fast path, cần một bảng trả lời sâu hơn: post nào nhận
engagement tốt, engagement đến nhanh hay chậm, reply khác original post ra sao,
repost/like ratio thế nào và post đã bị delete hay chưa.

**Kết quả sau khi hoàn thành**

Project có serving mart `gold_post_performance` được build từ:

- `gold_dim_posts`
- `gold_fact_engagement_events`

Bảng này được ghi ra staging Parquet trên MinIO, load vào ClickHouse table
`bluesky.gold_post_performance`, rồi reconcile giữa staging và ClickHouse. Luồng
`run_lakehouse_path.py` đã chạy pass, xác nhận lakehouse path hiện có thể build
Silver, build/check Gold modeled, refresh Gold serving và kiểm tra serving marts.

**Các file liên quan**

- `src/bluesky_pipeline/transforms/gold_analytics_transformations.py`
- `src/bluesky_pipeline/schemas/gold_tables.py`
- `scripts/gold/build/build_post_performance_from_gold_modeled.py`
- `scripts/gold/load/load_post_performance_to_clickhouse.py`
- `scripts/gold/check/check_post_performance_reconciliation.py`
- `scripts/gold/refresh/refresh_serving_from_iceberg.py`
- `scripts/gold/check/check_serving_v1.py`
- `scripts/lakehouse/run_lakehouse_path.py`
- `scripts/lakehouse/check_lakehouse_path.py`
- `scripts/platform/create_clickhouse_gold_tables.py`
- `docs/gold-analytics-metrics-v1.md`

**Kiến thức cần ghi nhớ**

- Serving mart phân tích nên đọc từ Gold modeled để giữ đúng kiến trúc
  `Silver -> Gold modeled -> Gold serving`.
- `gold_post_performance` giữ cả post chỉ xuất hiện như target của engagement để
  không làm rơi metric; các metadata như author hoặc text có thể null nếu post
  create chưa được observe.
- ClickHouse là nơi phục vụ dashboard cho mart này; nếu muốn query mart tương tự
  bằng Trino, cần materialize mart đó thành Iceberg hoặc cấu hình thêm connector
  phù hợp.
- Reconciliation giữa staging và ClickHouse giúp đảm bảo quá trình load serving
  không làm lệch các metric quan trọng.

## Bước 27: Kết nối SQL client tới Trino để query lakehouse

**Mục tiêu**

Kết nối một SQL client như DBeaver tới Trino để query trực tiếp các bảng Iceberg
trong lakehouse.

**Vì sao cần thực hiện**

Checkpoint script xác nhận Trino đọc được dữ liệu, nhưng trong thực tế analyst
hoặc data engineer thường cần giao diện SQL để khám phá dữ liệu, viết ad-hoc
query và kiểm tra model. Bước này chứng minh query engine layer có thể dùng được
qua SQL client, không chỉ qua CLI/script.

**Kết quả sau khi hoàn thành**

DBeaver kết nối được tới Trino local qua JDBC và query được catalog
`lakehouse`, schema `gold_v1`. Người dùng có thể chạy SQL trực tiếp lên các bảng
Gold modeled Iceberg như `gold_dim_posts` hoặc `gold_fact_engagement_events`.

**Các file liên quan**

- `docker-compose.yml`
- `config/trino/catalog/lakehouse.properties`
- `docs/tong-quan-du-an.md`

**Kiến thức cần ghi nhớ**

- Trino Web UI chủ yếu dùng để xem query history, stage/task và lỗi query; SQL
  client như DBeaver/DataGrip tiện hơn để viết ad-hoc SQL.
- Với Trino local chưa bật authentication, không nhập password trong DBeaver.
  Nếu gửi username kèm password qua HTTP, Trino JDBC có thể báo lỗi cần TLS/SSL.
- Trino hiện query lakehouse Iceberg tables. Các bảng ClickHouse serving như
  `bluesky.gold_post_performance` vẫn query qua ClickHouse/Grafana trừ khi cấu
  hình thêm connector hoặc materialize mart đó thành Iceberg.

## Bước 28: Implement Gold content quality hourly serving mart

**Mục tiêu**

Build serving mart phân tích content lifecycle và content quality theo giờ từ
Gold modeled Iceberg, load vào ClickHouse và đưa vào checkpoint tổng hợp.

**Vì sao cần thực hiện**

`gold_post_performance` trả lời câu hỏi ở grain từng post. Cần thêm một mart theo
thời gian để phân tích xu hướng content: mỗi giờ có bao nhiêu original post,
reply, update, delete; tỷ lệ reply/original ra sao; tỷ lệ update/delete có bất
thường không; text length trung bình của content mới thay đổi như thế nào.

**Kết quả sau khi hoàn thành**

Project có serving mart `gold_content_quality_hourly` được build từ:

- `gold_fact_content_events`
- `gold_dim_posts`

Bảng này được ghi ra staging Parquet trên MinIO, load vào ClickHouse table
`bluesky.gold_content_quality_hourly`, rồi reconcile giữa staging và ClickHouse.
`run_lakehouse_path.py` đã chạy pass, xác nhận mart này nằm trong luồng tổng hợp
lakehouse path.

**Các file liên quan**

- `src/bluesky_pipeline/transforms/gold_analytics_transformations.py`
- `src/bluesky_pipeline/schemas/gold_tables.py`
- `scripts/gold/build/build_content_quality_hourly_from_gold_modeled.py`
- `scripts/gold/load/load_content_quality_hourly_to_clickhouse.py`
- `scripts/gold/check/check_content_quality_hourly_reconciliation.py`
- `scripts/gold/refresh/refresh_serving_from_iceberg.py`
- `scripts/gold/check/check_serving_v1.py`
- `scripts/platform/create_clickhouse_gold_tables.py`
- `scripts/platform/cleanup_ingested_data.py`
- `docs/gold-analytics-metrics-v1.md`

**Kiến thức cần ghi nhớ**

- Mart theo thời gian nên có grain rõ ràng, ở đây là một dòng cho mỗi giờ
  `window_start`.
- Ratio như `reply_ratio`, `delete_ratio` và `update_ratio` giúp dashboard phân
  tích chất lượng/hành vi nội dung tốt hơn chỉ đếm event.
- Reconciliation nên ưu tiên các count metrics ổn định; các metric float như
  ratio/average có thể kiểm tra riêng nếu cần để tránh sai khác nhỏ do kiểu số.

## Bước 29: Implement Gold thread conversation summary serving mart

**Mục tiêu**

Build serving mart phân tích conversation theo từng root thread từ Gold modeled
Iceberg, load vào ClickHouse và đưa vào checkpoint tổng hợp của lakehouse path.

**Vì sao cần thực hiện**

Content volume và content quality theo giờ chưa trả lời được câu hỏi thread nào
tạo nhiều thảo luận nhất. Bluesky có nhiều nội dung dạng reply, nên cần một mart
riêng ở grain `reply_root_uri` để phân tích conversation dynamics: số lượng reply,
số actor tham gia, thời điểm reply đầu/cuối, thời lượng conversation và số reply
bị delete.

**Kết quả sau khi hoàn thành**

Project có serving mart `gold_thread_conversation_summary` được build từ:

- `gold_dim_posts`
- `gold_fact_content_events`

Bảng này được ghi ra staging Parquet trên MinIO, load vào ClickHouse table
`bluesky.gold_thread_conversation_summary`, rồi reconcile giữa staging và
ClickHouse. `run_lakehouse_path.py` đã chạy pass với các metric chính đều `OK`:

- `row_count`
- `reply_count`
- `reply_author_count`
- `deleted_reply_count`

Điều này xác nhận mart conversation đã nằm trong luồng
`Gold modeled Iceberg -> Gold analytics mart -> ClickHouse serving -> reconciliation`.

**Các file liên quan**

- `src/bluesky_pipeline/transforms/gold_analytics_transformations.py`
- `src/bluesky_pipeline/schemas/gold_tables.py`
- `scripts/gold/build/build_thread_conversation_summary_from_gold_modeled.py`
- `scripts/gold/load/load_thread_conversation_summary_to_clickhouse.py`
- `scripts/gold/check/check_thread_conversation_summary_reconciliation.py`
- `scripts/gold/refresh/refresh_serving_from_iceberg.py`
- `scripts/gold/check/check_serving_v1.py`
- `scripts/platform/create_clickhouse_gold_tables.py`
- `scripts/platform/cleanup_ingested_data.py`
- `docs/gold-analytics-metrics-v1.md`
- `docs/script-inventory.md`

**Kiến thức cần ghi nhớ**

- Thread/conversation mart có grain khác post mart: một dòng đại diện cho một
  `reply_root_uri`, không phải một dòng cho mỗi post.
- Root post có thể không nằm trong dữ liệu observe được, nên metadata của root
  như `root_author_did` hoặc `root_post_created_at` có thể null; vẫn giữ thread
  để không làm rơi reply metrics.
- Delete reply chỉ đếm được đầy đủ khi delete event còn lookup được
  `reply_root_uri` từ trạng thái post đã observe. Đây là giới hạn dữ liệu hợp lý
  cần hiểu khi giải thích metric.
- Với mart phục vụ ClickHouse, reconciliation nên tập trung vào các metric count
  ổn định trước. Các metric thời gian và average có thể kiểm tra riêng khi cần
  dashboard hoặc phân tích sâu hơn.

## Bước 30: Implement Gold actor activity daily serving mart

**Mục tiêu**

Build serving mart phân tích hành vi actor theo ngày từ Gold modeled Iceberg,
load vào ClickHouse và đưa vào checkpoint tổng hợp của lakehouse path.

**Vì sao cần thực hiện**

Các mart trước phân tích post, content lifecycle và conversation. Để trả lời câu
hỏi về hành vi người dùng quan sát được, cần một mart ở grain
`activity_date + actor_did`: actor nào tạo nhiều content, actor nào đi tương tác,
actor nào nhận nhiều engagement và actor nào có hoạt động mạng xã hội qua follow.

**Kết quả sau khi hoàn thành**

Project có serving mart `gold_actor_activity_daily` được build từ Gold modeled:

- `gold_dim_actors`
- `gold_dim_posts`
- `gold_fact_content_events`
- `gold_fact_engagement_events`
- `gold_fact_network_events`

Bảng này được ghi ra staging Parquet trên MinIO, load vào ClickHouse table
`bluesky.gold_actor_activity_daily`, rồi reconcile giữa staging và ClickHouse.
`run_lakehouse_path.py` đã chạy pass, xác nhận mart actor activity nằm trong
luồng tổng hợp Gold serving.

**Các file liên quan**

- `src/bluesky_pipeline/transforms/gold_analytics_transformations.py`
- `src/bluesky_pipeline/schemas/gold_tables.py`
- `scripts/gold/build/build_actor_activity_daily_from_gold_modeled.py`
- `scripts/gold/load/load_actor_activity_daily_to_clickhouse.py`
- `scripts/gold/check/check_actor_activity_daily_reconciliation.py`
- `scripts/gold/refresh/refresh_serving_from_iceberg.py`
- `scripts/gold/check/check_serving_v1.py`
- `scripts/platform/create_clickhouse_gold_tables.py`
- `docs/gold-analytics-metrics-v1.md`

**Kiến thức cần ghi nhớ**

- Actor activity mart kết hợp nhiều vai trò của cùng một DID: creator, engager,
  actor nhận engagement và network builder.
- `received_*` metrics cần join engagement target post về author trong
  `gold_dim_posts`, vì actor đi tương tác và actor nhận tương tác là hai vai trò
  khác nhau.
- `activity_score` là score v1 có trọng số đơn giản để phục vụ dashboard/leaderboard,
  không phải ranking model phức tạp.

## Bước 31: Implement Gold network growth daily serving mart

**Mục tiêu**

Build serving mart phân tích observed network growth theo ngày từ Gold modeled
Iceberg, load vào ClickHouse và đưa vào checkpoint tổng hợp của lakehouse path.

**Vì sao cần thực hiện**

Sau khi đã có mart về content, conversation và actor behavior, cần một mart riêng
cho social graph dynamics. `gold_network_growth_daily` trả lời actor nào được
follow nhiều nhất trong dữ liệu quan sát được, follow/unfollow tạo ra net growth
như thế nào và có bao nhiêu follower unique trong ngày.

**Kết quả sau khi hoàn thành**

Project có serving mart `gold_network_growth_daily` được build từ
`gold_fact_network_events`, ghi ra staging Parquet trên MinIO, load vào
ClickHouse table `bluesky.gold_network_growth_daily`, rồi reconcile giữa staging
và ClickHouse. `run_lakehouse_path.py` đã chạy pass, đồng thời hoàn thiện đủ bộ
Gold analytics serving marts v1 đã thiết kế.

**Các file liên quan**

- `src/bluesky_pipeline/transforms/gold_analytics_transformations.py`
- `src/bluesky_pipeline/schemas/gold_tables.py`
- `scripts/gold/build/build_network_growth_daily_from_gold_modeled.py`
- `scripts/gold/load/load_network_growth_daily_to_clickhouse.py`
- `scripts/gold/check/check_network_growth_daily_reconciliation.py`
- `scripts/gold/refresh/refresh_serving_from_iceberg.py`
- `scripts/gold/check/check_serving_v1.py`
- `scripts/platform/create_clickhouse_gold_tables.py`
- `docs/gold-analytics-metrics-v1.md`

**Kiến thức cần ghi nhớ**

- Network growth trong project là observed growth từ stream đã ingest, không phải
  follower count toàn cục của Bluesky.
- `target_actor_did` có thể null nếu delete event không lookup được follow target;
  ClickHouse sorting key cần xử lý nullable column rõ ràng.
- Khi một bộ mart đã đủ contract ban đầu, nên chuyển sang dashboard/checkpoint và
  tài liệu hóa cách sử dụng thay vì tiếp tục thêm metric không có câu hỏi phân
  tích rõ ràng.

## Bước 32: Dựng Grafana Gold Analytics Dashboard

**Mục tiêu**

Dựng dashboard Grafana cho bộ Gold analytics serving marts v1 trên ClickHouse để
trình bày các insight từ lakehouse path.

**Vì sao cần thực hiện**

Sau khi Gold analytics marts đã được build, load và reconcile, cần một dashboard
giúp người xem hiểu dữ liệu mà không phải tự query SQL. Dashboard này là
presentation layer cho các mart đã được Spark build từ Gold modeled Iceberg và
ClickHouse phục vụ với latency thấp.

**Kết quả sau khi hoàn thành**

Grafana có dashboard `Bluesky Gold Analytics` đọc từ ClickHouse datasource, gồm
các nhóm:

- Gold serving freshness.
- Post performance và content quality.
- Conversation analytics.
- Actor activity.
- Network growth.
- Data quality cho unresolved follow delete targets.

Dashboard dùng wide-format query cho time series để legend rõ ràng, tách count và
ratio theo panel khi scale khác nhau, và giữ DQ panel cho các giới hạn dữ liệu
quan sát được.

**Các file liên quan**

- `docs/gold-analytics-metrics-v1.md`
- `docs/edge-cases-va-bai-hoc-phong-van.md`
- `src/bluesky_pipeline/schemas/gold_tables.py`
- `src/bluesky_pipeline/transforms/gold_analytics_transformations.py`
- `scripts/platform/create_clickhouse_gold_tables.py`
- `scripts/gold/refresh/refresh_serving_from_iceberg.py`
- `scripts/gold/check/check_serving_v1.py`
- `docker-compose.yml`

**Kiến thức cần ghi nhớ**

- Grafana là presentation layer; source of truth vẫn là lakehouse/Gold modeled và
  ClickHouse chỉ là serving mart có thể rebuild.
- Dashboard query cũng cần data contract rõ ràng: tránh alias aggregate gây lỗi
  ClickHouse, cast kiểu khi trộn signed/unsigned metric, và dùng wide-format time
  series khi muốn legend sạch.
- Business metrics và data quality panels nên cùng tồn tại: dashboard không chỉ
  hiển thị insight mà còn giúp giải thích giới hạn dữ liệu như unresolved follow
  delete targets.
- `unknown` author/root actor có thể là giới hạn hợp lệ của observed stream,
  không nhất thiết là lỗi dashboard.

## Bước 33: Thiết kế contract incremental refresh cho Gold

**Mục tiêu**

Đặt nền tảng để tiến hóa Gold modeled và Gold analytics marts từ full rebuild
sang incremental batch/micro-batch.

**Vì sao cần thực hiện**

Full rebuild phù hợp với local MVP vì dễ hiểu và dễ kiểm chứng, nhưng không tối
ưu khi dữ liệu lớn dần. Để Gold xử lý thường xuyên dữ liệu mới từ Silver mà không
phải scan lại toàn bộ lịch sử, pipeline cần một contract chung mô tả refresh
window, lookback và cách xác định phạm vi dữ liệu cần xử lý.

**Kết quả sau khi hoàn thành**

Project có tài liệu thiết kế incremental Gold refresh và module dùng chung
`incremental_refresh.py` để tính refresh window gồm:

- `last_successful_run_at`
- `refresh_from`
- `refresh_to`
- `lookback_hours`

Module cũng có helper đọc/ghi local state file dạng JSON để lưu
`last_successful_run_at` trong môi trường học/local. Module đã được kiểm tra bằng
compile Python, ví dụ tính window có lookback 2 giờ và smoke test đọc/ghi state
file.

Project đã triển khai incremental refresh cho nhóm Gold facts và dimensions:

- `gold_fact_content_events`
- `gold_fact_engagement_events`
- `gold_fact_network_events`
- `gold_dim_posts`
- `gold_dim_actors`

Fact refresh dùng deterministic event id để anti-join với bảng Gold hiện có và
chỉ append rows mới. Dimension refresh xác định affected business keys từ Silver,
recompute state cho các key đó từ Silver source of truth, rồi dùng Iceberg
`MERGE INTO` để update/insert theo business key.

Trước khi dùng `MERGE INTO` trong pipeline chính, project có smoke test Iceberg
v2 để xác nhận stack Spark + Iceberg + Hive catalog hỗ trợ row-level merge. Facts
và dimensions đều có checkpoint riêng để kiểm tra row count, key không null và
key uniqueness. Các refresh script chỉ update local state sau khi write và
checkpoint pass.

**Các file liên quan**

- `docs/gold-incremental-refresh-design.md`
- `src/bluesky_pipeline/state/incremental_refresh.py`
- `scripts/gold/check/check_gold_dimensions_incremental.py`
- `scripts/gold/check/check_gold_facts_incremental.py`
- `scripts/discovery/smoke_test_iceberg_merge.py`
- `scripts/gold/refresh/refresh_gold_dimensions_incremental.py`
- `scripts/gold/refresh/refresh_gold_facts_incremental.py`
- `docs/quy-trinh-xay-dung-pipeline.md`

**Kiến thức cần ghi nhớ**

- Streaming project không có nghĩa mọi layer đều phải continuous streaming. Một
  kiến trúc thực tế có thể dùng streaming cho ingestion/Bronze/Silver và
  incremental batch cho Gold analytical layer.
- Incremental refresh cần watermark hoặc state của lần chạy thành công gần nhất.
- Lookback window giúp xử lý late data nhưng yêu cầu downstream phải idempotent
  hoặc có cơ chế merge/replace rõ ràng.
- Local JSON state phù hợp cho bước học đầu tiên, nhưng production nên chuyển
  sang metadata table, orchestration state hoặc một durable control store.
- Count-only hoặc dry-run incremental job không nên cập nhật watermark thành
  công, vì như vậy có thể khiến lần chạy thật bỏ qua dữ liệu chưa được ghi xuống
  Gold.
- Trước khi ghi incremental vào Gold Iceberg, nên build thử DataFrame và kiểm tra
  key uniqueness trên phạm vi incremental để phát hiện lỗi contract sớm.
- Với fact append-only, anti-join theo deterministic event id là một cách đơn giản
  để đạt idempotent append trong bước đầu, trước khi cần `MERGE INTO` phức tạp
  hơn.
- Refresh job và checkpoint nên tách được để checkpoint có thể chạy độc lập sau
  một lần refresh, sau một lần full rebuild hoặc trước demo.
- Dimension incremental khác fact incremental: cần xác định affected business keys
  trước, sau đó recompute state cho các key đó từ Silver source of truth thay vì
  append mù theo event mới.
- Affected scope cho dimension và affected scope cho downstream marts có thể khác
  nhau. Engagement mới không đổi `gold_dim_posts`, nhưng vẫn ảnh hưởng các mart
  phân tích performance của post.
- Trước khi dùng row-level operation như Iceberg `MERGE INTO` trong pipeline
  chính, nên smoke test capability của stack local để tránh nhầm lỗi logic với
  lỗi catalog/table format.
- Với dimension/state table, production-like incremental refresh nên dùng merge
  theo business key thay vì append-only hoặc overwrite toàn bảng.
- Dù incremental window không có row mới, refresh job vẫn nên chạy checkpoint trên
  bảng đích hiện có trước khi ghi state thành công để tránh che lấp lỗi dữ liệu
  còn tồn tại từ lần chạy trước.
- Contract refresh window nên được tách thành module dùng chung trước khi viết
  từng incremental job cụ thể.

## Bước 34: Incremental hóa Gold content quality hourly mart

**Mục tiêu**

Chuyển `gold_content_quality_hourly` từ full rebuild/load sang incremental
refresh theo affected hourly windows.

**Vì sao cần thực hiện**

Trước bước này, script build mart ghi đè toàn bộ Parquet staging và script load
ClickHouse `TRUNCATE` toàn bảng rồi insert lại. Cách này dễ hiểu trong MVP local
nhưng không tối ưu khi dữ liệu lớn, vì mỗi lần refresh đều phải tính và load lại
toàn bộ lịch sử.

`gold_content_quality_hourly` là mart phù hợp để làm incremental đầu tiên vì
grain tự nhiên là `window_start` theo giờ. Khi có fact content mới, pipeline chỉ
cần xác định những giờ bị ảnh hưởng, recompute lại đúng các giờ đó từ Gold modeled
source of truth và replace các window tương ứng trong ClickHouse.

**Kết quả sau khi hoàn thành**

Project có script `refresh_gold_content_quality_hourly_incremental.py` để:

- Đọc `gold_fact_content_events` trong refresh window theo `received_at`.
- Xác định affected `window_start` theo `event_time`.
- Recompute lại mart rows cho affected hours từ full Gold modeled Iceberg.
- `DELETE` đúng affected windows trong ClickHouse rồi insert lại rows mới.
- Chờ ClickHouse mutation hoàn tất trước khi insert để tránh duplicate tạm thời.
- Reconcile metrics cho affected windows.
- Chỉ update local state sau khi replace và reconciliation pass.

Initial refresh với `--ignore-state` đã replace 276 hourly windows, reconcile
khớp với các tổng metric:

- `row_count = 276`
- `total_content_events = 31906`
- `original_post_create_count = 15749`
- `reply_create_count = 14754`
- `post_delete_count = 1234`
- `reply_delete_count = 152`
- `post_update_count = 17`

Full reconciliation cũ giữa Parquet staging và ClickHouse vẫn pass sau incremental
replace. Lần chạy normal sau đó không có fact rows mới, `affected_window_count =
0`, `affected_rows_replaced = 0` và `state_updated: true`.

**Các file liên quan**

- `scripts/gold/refresh/refresh_gold_content_quality_hourly_incremental.py`
- `scripts/gold/build/build_content_quality_hourly_from_gold_modeled.py`
- `scripts/gold/load/load_content_quality_hourly_to_clickhouse.py`
- `scripts/gold/check/check_content_quality_hourly_reconciliation.py`
- `src/bluesky_pipeline/transforms/gold_analytics_transformations.py`
- `src/bluesky_pipeline/state/incremental_refresh.py`
- `src/bluesky_pipeline/schemas/gold_tables.py`

**Kiến thức cần ghi nhớ**

- Watermark để biết dữ liệu mới nên dựa trên ingestion/load marker như
  `received_at`, nhưng affected business window của mart time series phải dựa
  trên `event_time`.
- Late data có thể được ingest trong window hiện tại nhưng làm thay đổi metric
  của một giờ cũ, nên incremental mart phải replace affected windows theo event
  time thay vì chỉ append giờ mới nhất.
- Với ClickHouse `MergeTree`, `ALTER TABLE ... DELETE` là mutation bất đồng bộ.
  Incremental load cần chờ mutation hoàn tất trước khi insert lại rows cùng key
  để tránh dashboard đọc duplicate tạm thời.
- Full rebuild path vẫn nên được giữ như fallback/rebuild mechanism; incremental
  path tối ưu vận hành thường ngày nhưng không thay thế nhu cầu rebuild khi đổi
  logic metric.

## Bước 35: Incremental hóa các Gold analytics marts còn lại

**Mục tiêu**

Hoàn thiện incremental refresh cho các Gold analytics marts còn lại trong
ClickHouse, thay thế cách vận hành thường ngày kiểu `TRUNCATE` toàn bảng bằng
replace đúng affected keys hoặc affected partitions.

**Vì sao cần thực hiện**

Sau khi `gold_content_quality_hourly` đã pass, project cần chứng minh pattern
incremental có thể áp dụng cho nhiều loại grain khác nhau, không chỉ time series
theo giờ. Mỗi mart có một grain riêng nên affected scope cũng khác nhau:

- `gold_post_performance`: affected theo `post_uri`.
- `gold_thread_conversation_summary`: affected theo `reply_root_uri`.
- `gold_network_growth_daily`: affected theo `activity_date + target_actor_did`.
- `gold_actor_activity_daily`: affected theo `activity_date + actor_did`.

Nếu vẫn rebuild/load toàn bảng, dashboard local vẫn chạy được nhưng kiến trúc
không phản ánh tốt bài toán dữ liệu lớn. Incremental refresh giúp chỉ recompute
và replace phần metric thật sự bị ảnh hưởng.

**Kết quả sau khi hoàn thành**

Project có helper dùng chung `incremental_serving_utils.py` cho các thao tác lặp
lại trong Gold serving incremental:

- Lọc Gold fact rows theo `received_at` refresh window.
- Đọc Gold modeled Iceberg theo table contract.
- Build ClickHouse predicates theo single key hoặc tuple key.
- Chia keys thành chunk để tránh câu SQL quá dài.
- Gửi SQL dài qua HTTP body.
- Chạy `ALTER TABLE ... DELETE`, chờ mutation hoàn tất rồi insert rows mới.
- Tính expected metrics từ affected mart DataFrame và actual metrics từ
  ClickHouse để reconciliation theo affected scope.

Project có thêm các script incremental:

- `refresh_gold_post_performance_incremental.py`
- `refresh_gold_thread_conversation_summary_incremental.py`
- `refresh_gold_network_growth_daily_incremental.py`
- `refresh_gold_actor_activity_daily_incremental.py`

Tất cả các script đã được kiểm chứng với 3 bước:

- Chạy initial incremental bằng `--ignore-state`.
- Chạy reconciliation full hiện có để đảm bảo ClickHouse vẫn khớp với staging
  hoặc source of truth hiện tại.
- Chạy lại normal mode để kiểm tra state/no-op path.

Trong quá trình kiểm thử, `gold_actor_activity_daily` gặp lỗi Spark ambiguous
reference ở cột `actor_did` khi join engagement fact với post author. Script đã
được sửa bằng cách tách rõ `actor_did` của người đi engagement và
`received_actor_did` của author nhận engagement, sau đó alias lại thành
`actor_did` cho grain cuối cùng của mart. Sau khi sửa, toàn bộ script incremental
cho các marts còn lại đều pass.

**Các file liên quan**

- `scripts/gold/common/incremental_serving_utils.py`
- `scripts/gold/refresh/refresh_gold_post_performance_incremental.py`
- `scripts/gold/refresh/refresh_gold_thread_conversation_summary_incremental.py`
- `scripts/gold/refresh/refresh_gold_network_growth_daily_incremental.py`
- `scripts/gold/refresh/refresh_gold_actor_activity_daily_incremental.py`
- `scripts/gold/check/check_post_performance_reconciliation.py`
- `scripts/gold/check/check_thread_conversation_summary_reconciliation.py`
- `scripts/gold/check/check_network_growth_daily_reconciliation.py`
- `scripts/gold/check/check_actor_activity_daily_reconciliation.py`
- `src/bluesky_pipeline/transforms/gold_analytics_transformations.py`
- `src/bluesky_pipeline/schemas/gold_tables.py`

**Kiến thức cần ghi nhớ**

- Incremental serving mart phải bắt đầu từ grain. Chỉ khi biết grain mới xác định
  được affected keys, delete predicate và reconciliation scope.
- Mart entity như post performance hoặc thread summary nên replace theo business
  key; mart daily nên replace theo tuple gồm ngày và entity key.
- Với metric nhận tương tác, cùng một event có thể ảnh hưởng nhiều actor: người
  thực hiện engagement và người nhận engagement qua author của target post.
- Khi join nhiều DataFrame có cột cùng tên, phải alias theo vai trò nghiệp vụ
  trước khi select; cùng tên kỹ thuật không có nghĩa là cùng semantics.
- ClickHouse `MergeTree` mutation là bất đồng bộ, nên incremental job phải chờ
  delete mutation hoàn tất trước khi insert lại affected rows.
- Full rebuild scripts vẫn có giá trị làm fallback và reconciliation baseline,
  còn incremental scripts là đường vận hành thường ngày.

## Bước 36: Đưa incremental Gold vào lakehouse E2E path

**Mục tiêu**

Đưa các script incremental Gold đã kiểm chứng riêng lẻ vào một luồng E2E có thứ
tự rõ ràng, để vận hành Gold không còn phụ thuộc vào việc chạy tay từng mart.

**Vì sao cần thực hiện**

Chạy từng script riêng giúp debug component, nhưng một pipeline thực tế cần
entrypoint orchestration để đảm bảo thứ tự phụ thuộc, fail-fast khi một bước lỗi
và tạo một command dễ demo/phỏng vấn. Gold incremental phải chạy theo thứ tự:

```text
facts -> dimensions -> Gold modeled check -> serving marts
```

Facts cần refresh trước để các sự kiện mới có mặt trong Gold modeled. Dimensions
cần refresh sau đó để state của post/actor kịp cập nhật. Sau khi Gold modeled
pass checkpoint, các serving marts mới được recompute và replace affected keys
trong ClickHouse.

**Kết quả sau khi hoàn thành**

Project có Gold incremental orchestrator:

```text
scripts/gold/refresh/refresh_gold_incremental.py
```

Script này chạy:

- `refresh_gold_facts_incremental`
- `refresh_gold_dimensions_incremental`
- `check_trino_gold_modeled_v1`
- `refresh_gold_content_quality_hourly_incremental`
- `refresh_gold_post_performance_incremental`
- `refresh_gold_thread_conversation_summary_incremental`
- `refresh_gold_network_growth_daily_incremental`
- `refresh_gold_actor_activity_daily_incremental`

Project cũng có lakehouse incremental E2E entrypoint:

```text
scripts/lakehouse/run_lakehouse_path_incremental.py
```

Entrypoint này kiểm tra Silver Iceberg trước, sau đó gọi Gold incremental
orchestrator. Lần chạy E2E incremental đã pass với normal state/no-op path:

```text
Gold incremental refresh passed
Lakehouse incremental path passed
```

Full rebuild path cũ `scripts/lakehouse/run_lakehouse_path.py` vẫn được giữ lại
để bootstrap, rebuild hoặc recovery khi cần. Incremental path mới là đường vận
hành thường ngày sau khi Silver đã có dữ liệu mới.

**Các file liên quan**

- `scripts/gold/refresh/refresh_gold_incremental.py`
- `scripts/lakehouse/run_lakehouse_path_incremental.py`
- `scripts/lakehouse/run_lakehouse_path.py`
- `scripts/gold/refresh/refresh_gold_facts_incremental.py`
- `scripts/gold/refresh/refresh_gold_dimensions_incremental.py`
- `scripts/gold/refresh/refresh_gold_content_quality_hourly_incremental.py`
- `scripts/gold/refresh/refresh_gold_post_performance_incremental.py`
- `scripts/gold/refresh/refresh_gold_thread_conversation_summary_incremental.py`
- `scripts/gold/refresh/refresh_gold_network_growth_daily_incremental.py`
- `scripts/gold/refresh/refresh_gold_actor_activity_daily_incremental.py`

**Kiến thức cần ghi nhớ**

- Component scripts dùng để phát triển/debug; E2E orchestrator dùng để vận hành
  và demo luồng hoàn chỉnh.
- Full rebuild và incremental refresh nên cùng tồn tại: full rebuild phục vụ
  bootstrap/recovery/backfill lớn, incremental refresh phục vụ chạy định kỳ trên
  dữ liệu mới.
- Orchestration phải fail-fast: nếu facts, dimensions hoặc checkpoint Gold
  modeled lỗi thì không nên tiếp tục refresh serving marts.
- Với hệ thống production, state của các incremental jobs nên được đưa vào
  durable metadata/control table hoặc orchestrator state thay vì chỉ dùng local
  JSON file.

## Bước 37: Cố định Hive Metastore Derby DB trong volume persist

**Mục tiêu**

Sửa cấu hình Hive Metastore local để Derby metastore database được lưu đúng trong
Docker named volume, giúp service restart/recreate không làm mất catalog metadata
hoặc rơi vào trạng thái schema init dở dang.

**Vì sao cần thực hiện**

Lakehouse path phụ thuộc vào Hive Metastore để Spark, Iceberg và Trino cùng nhìn
thấy namespace/table metadata. Khi Metastore không chạy, các bước đọc Silver
Iceberg readiness, build Gold modeled và validate bằng Trino đều fail dù MinIO,
Trino hoặc ClickHouse vẫn đang chạy.

Trong image `apache/hive:3.1.3`, Derby connection URL mặc định là relative:

```text
jdbc:derby:;databaseName=metastore_db;create=true
```

Image chạy trong `/opt/hive`, nên database thật nằm ở `/opt/hive/metastore_db`.
Compose trước đó mount volume vào `/opt/hive/data`, khiến Derby DB không được
persist đúng path. Khi container bị recreate, Hive có thể gặp hai lỗi ngược nhau:

- Init lại schema thì fail vì object đã tồn tại một phần.
- Resume mode thì fail vì bảng `VERSION` không tồn tại.

**Kết quả sau khi hoàn thành**

Project có cấu hình `config/hive/hive-site.xml` để cố định Derby DB vào path nằm
trong volume persist:

```text
jdbc:derby:/opt/hive/data/metastore_db;create=true
```

`docker-compose.yml` mount named volume `hive-metastore-db` vào `/opt/hive/data`
và mount `hive-site.xml` vào `/opt/hive/conf/hive-site.xml`. Sau khi chỉnh quyền
volume cho user `hive`, chạy `schematool -initSchema` một lần thành công, rồi
start Metastore với `IS_RESUME=true`.

Hive Metastore đã lên lại, và Trino query metadata qua catalog `lakehouse` được:

```text
SHOW SCHEMAS
```

Metastore mới chỉ có các schema mặc định, vì vậy các bảng Silver/Gold Iceberg cần
được rebuild/đăng ký lại bằng pipeline sau khi cleanup dữ liệu cũ.

Sau khi cleanup dữ liệu ingest cũ, bao gồm Kafka topic, ClickHouse serving
tables, checkpoint/state files và object storage paths liên quan, hệ thống sẵn
sàng chạy lại clean E2E để bootstrap lại Silver/Gold metadata trên metastore mới.

**Các file liên quan**

- `docker-compose.yml`
- `config/hive/hive-site.xml`
- `config/hive/core-site.xml`
- `config/trino/catalog/lakehouse.properties`
- `scripts/lakehouse/check_silver_iceberg_readiness.py`
- `scripts/lakehouse/run_lakehouse_path_incremental.py`

**Kiến thức cần ghi nhớ**

- Hive Metastore là metadata catalog, không phải nơi chứa data files. Data files
  nằm trên MinIO, còn table/namespace metadata nằm trong metastore database.
- Embedded Derby trong container nhạy với working directory và mount path. Cần
  kiểm tra connection URL thật sự, không chỉ nhìn tên volume.
- `schematool -initSchema` chỉ nên chạy một lần trên DB trống; sau khi schema đã
  có, container nên chạy với resume mode.
- Derby embedded chỉ phù hợp cho môi trường local học tập. Trong production nên
  dùng external database như PostgreSQL/MySQL cho Hive Metastore để tránh vấn đề
  persistence, lock và recovery.
