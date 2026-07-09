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
18. [Trạng thái hiện tại và bài học thiết kế](#bước-18-trạng-thái-hiện-tại-và-bài-học-thiết-kế)
19. [Bổ sung live demo runner và cleanup dữ liệu local](#bước-19-bổ-sung-live-demo-runner-và-cleanup-dữ-liệu-local)

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
- `src/bluesky_pipeline/kafka_config.py`
- `src/bluesky_pipeline/spark_session.py`
- `src/bluesky_pipeline/bronze_tables.py`
- `src/bluesky_pipeline/silver_transformations.py`
- `src/bluesky_pipeline/gold_tables.py`

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

- `src/bluesky_pipeline/event_envelope.py`
- `src/bluesky_pipeline/normalize_event.py`
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
- `src/bluesky_pipeline/kafka_config.py`
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
- `src/bluesky_pipeline/event_envelope.py`
- `src/bluesky_pipeline/kafka_config.py`
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
- `src/bluesky_pipeline/spark_session.py`
- `src/bluesky_pipeline/bronze_schemas.py`
- `src/bluesky_pipeline/kafka_config.py`

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
- `src/bluesky_pipeline/bronze_tables.py`
- `src/bluesky_pipeline/spark_session.py`
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
- `src/bluesky_pipeline/silver_transformations.py`
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

- `src/bluesky_pipeline/bronze_schemas.py`
- `src/bluesky_pipeline/bronze_tables.py`
- `src/bluesky_pipeline/silver_transformations.py`
- `src/bluesky_pipeline/gold_tables.py`
- `src/bluesky_pipeline/iceberg_config.py`
- `src/bluesky_pipeline/clickhouse_client.py`
- `src/bluesky_pipeline/kafka_config.py`
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
- `scripts/gold/build_event_volume_from_iceberg.py`
- `scripts/gold/build_post_engagement_summary_from_iceberg.py`
- `scripts/gold/load_event_volume_to_clickhouse.py`
- `scripts/gold/load_post_engagement_summary_to_clickhouse.py`
- `scripts/gold/check_event_volume_reconciliation.py`
- `scripts/gold/check_post_engagement_reconciliation.py`
- `scripts/gold/check_serving_v1.py`
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
- `src/bluesky_pipeline/silver_transformations.py`
- `src/bluesky_pipeline/iceberg_config.py`
- `src/bluesky_pipeline/spark_session.py`

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

`Silver Iceberg -> Gold modeled tables -> Gold aggregate/serving marts -> ClickHouse`.

Gold modeled tables có thể là fact/dim hoặc semantic marts phục vụ phân tích sâu.
Gold aggregate/serving marts là lớp tính trước metric từ dữ liệu đã model để phục
vụ Grafana và các truy vấn lặp lại với độ trễ thấp hơn.

Trong lakehouse path, Spark xử lý dữ liệu ở các đoạn chính:

- `Bronze -> Spark -> Silver Iceberg`
- `Silver Iceberg -> Spark -> Gold modeled tables`
- `Gold modeled tables -> Spark -> Gold aggregate/serving marts -> ClickHouse`

ClickHouse chỉ là serving layer cho lakehouse marts, không phải nơi xử lý dữ liệu
gốc chính.

**Kết quả sau khi hoàn thành**

Project có scripts build aggregate event volume và post engagement summary từ Silver
Iceberg, load vào ClickHouse và entrypoint `refresh_gold_serving_from_iceberg.py`
để chạy refresh Gold serving v1. Checkpoint cuối của Gold serving đối chiếu
ClickHouse với Silver Iceberg source of truth, không còn quay lại Silver Parquet
prototype. Project cũng có entrypoint `run_lakehouse_path.py` để chạy
toàn bộ lakehouse path theo thứ tự: build Silver Iceberg, check
Silver Iceberg, refresh Gold aggregate/serving và check ClickHouse serving marts.
Gold modeled layer chưa được tách thành bảng riêng ở bước này; đây là phần cần
hoàn thiện tiếp khi chuyển từ metric đơn giản sang data modeling đầy đủ.

**Các file liên quan**

- `scripts/gold/build_event_volume_from_iceberg.py`
- `scripts/gold/build_post_engagement_summary_from_iceberg.py`
- `scripts/gold/refresh_serving_from_iceberg.py`
- `scripts/lakehouse/run_lakehouse_path.py`
- `scripts/gold/check_serving_v1.py`
- `src/bluesky_pipeline/gold_tables.py`

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
- `scripts/gold/check_event_volume_reconciliation.py`
- `scripts/gold/check_post_engagement_reconciliation.py`
- `scripts/gold/check_serving_v1.py`
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
  `Silver Iceberg -> Gold modeled tables -> Gold aggregate/serving marts -> ClickHouse -> Grafana`.
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
- `scripts/gold/refresh_serving_from_iceberg.py`

**Kiến thức cần ghi nhớ**

- Fast path không thay thế Silver Iceberg.
- ClickHouse realtime marts là serving tables, không phải source of truth duy
  nhất.
- Lakehouse path không có nghĩa mọi tầng đều chậm: Bronze và Silver là
  các tầng streaming/continuous, còn Gold có thể refresh chậm hơn.
- Near-real-time luôn có độ trễ từ Spark trigger, ClickHouse insert và Grafana
  refresh.
- Project có lai một phần tư duy Kappa vì Kafka là event backbone chung và Spark
  được dùng cho cả streaming lẫn batch, nhưng không phải Kappa thuần vì vẫn có
  Bronze/Silver Iceberg làm lakehouse source of truth.

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
- `src/bluesky_pipeline/gold_tables.py`
- `src/bluesky_pipeline/kafka_config.py`

**Kiến thức cần ghi nhớ**

- `foreachBatch` nhận DataFrame tĩnh của micro-batch, nên aggregation bên trong
  hàm là batch aggregation bình thường.
- Hiện tại collect aggregate về Driver chấp nhận được vì metric cardinality thấp.
  Nếu mở rộng sang top user, hashtag hoặc domain, cần chuyển sang connector/JDBC
  hoặc `foreachPartition`.
- `input_rows = count()` phục vụ batch health nhưng là một action bổ sung; workload
  lớn hơn có thể lấy từ streaming progress metrics.
- Semantics hiện tại là at-least-once; chưa tuyên bố exactly-once end-to-end.

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
- `src/bluesky_pipeline/gold_tables.py`

**Kiến thức cần ghi nhớ**

- Grafana realtime panel vẫn query định kỳ xuống ClickHouse, không stream từng
  event trực tiếp vào trình duyệt.
- `spark_batch_id` chỉ dùng debug; business query không nên group theo batch id.
- Time picker/timezone là nguyên nhân thường gặp khi query có dữ liệu trong
  ClickHouse nhưng Grafana báo no data.
- Grafana time series cần output sort tăng dần theo cột time.

## Bước 18: Trạng thái hiện tại và bài học thiết kế

**Mục tiêu**

Tóm tắt trạng thái hiện tại của project và các bài học thiết kế quan trọng trước
khi chuyển sang hoàn thiện lakehouse path.

**Vì sao cần thực hiện**

Khi project đi qua nhiều prototype, tài liệu cần phản ánh kiến trúc hiện tại thay
vì giữ nguyên mọi bước nhỏ trong quá khứ. Điều này giúp người học ôn tập và trình
bày dự án theo mạch rõ ràng hơn.

**Kết quả sau khi hoàn thành**

Project hiện có hai path đã chạy được ở local:

- Realtime fast path:
  `Jetstream -> Gateway -> Kafka -> Spark Streaming -> ClickHouse realtime marts -> Grafana`.
- Lakehouse path:
  `Kafka -> Bronze -> Silver Iceberg`, với Bronze -> Silver chạy streaming trong
  live pipeline.
- Gold lakehouse path:
  `Silver Iceberg -> Gold modeled tables -> Gold aggregate/serving marts -> ClickHouse lakehouse marts -> Grafana`.

Realtime path đã có business metrics và operational health. Lakehouse path đã có
Silver Iceberg, Gold aggregate refresh và reconciliation. Phần còn cần hoàn thiện
tiếp là tách Gold modeled layer rõ ràng hơn, sau đó mới chuẩn hóa orchestration,
runbook và monitoring nếu milestone yêu cầu.

**Các file liên quan**

- `docs/tong-quan-du-an.md`
- `docs/quy-trinh-xay-dung-pipeline.md`
- `README.md`
- `scripts/realtime/stream_metrics_to_clickhouse.py`
- `scripts/gold/refresh_serving_from_iceberg.py`
- `scripts/realtime/check_clickhouse_metrics.py`
- `scripts/gold/check_serving_v1.py`

**Kiến thức cần ghi nhớ**

- Tài liệu quy trình nên ghi mốc kiến trúc, không ghi mọi command đã chạy.
- Prototype có giá trị học tập, nhưng khi bị thay thế bởi path chính thức thì nên
  được gom vào bài học thay vì giữ thành bước riêng.
- Fast path tối ưu latency; lakehouse path tối ưu độ tin cậy, rebuild và
  reconciliation.
- Với lakehouse analytics, Gold nên được hiểu thành hai vai trò: modeled layer để
  biểu diễn dữ liệu nghiệp vụ và aggregated/serving layer để tăng tốc dashboard.
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
truncate ClickHouse serving tables và có tùy chọn purge Kafka raw topic.

Live pipeline đã được chạy lại từ trạng thái sạch sau cleanup: dữ liệu mới đi từ
Jetstream vào Kafka, Spark ghi Bronze, realtime marts và đẩy Bronze mới sang
Silver Iceberg bằng streaming job; Grafana cập nhật theo time range hiện tại,
checkpoint realtime pass và runner dừng được bằng `Ctrl+C`.

**Các file liên quan**

- `scripts/e2e/run_live_pipeline.py`
- `src/bluesky_pipeline/ingestion_gateway.py`
- `scripts/platform/cleanup_ingested_data.py`
- `docs/script-inventory.md`

**Kiến thức cần ghi nhớ**

- Live demo runner là entrypoint vận hành local, không thay thế orchestration như
  Airflow trong các workflow batch/backfill có điểm bắt đầu và kết thúc rõ ràng.
- Bronze -> Silver nên chạy theo streaming trong live pipeline; Gold lakehouse
  refresh nên tách riêng và schedule bằng Airflow ở milestone orchestration.
- Cleanup dữ liệu ingest nên có dry-run và flag xác nhận vì đây là thao tác phá
  hủy dữ liệu.
- Truncate ClickHouse giữ lại schema để Grafana dashboard không mất query/table
  contract.
- Xóa dữ liệu trong Docker volume không nhất thiết làm file disk image của WSL
  giảm ngay; compact WSL là thao tác ở tầng hệ điều hành, không phải logic
  pipeline.
