# Quy trình xây dựng pipeline

Tài liệu này tổng hợp các bước chính đã thực sự hoàn thành trong project
`bluesky-pipeline`.

Mục tiêu của tài liệu là phục vụ học tập và ôn phỏng vấn Data Engineer, không phải
nhật ký thao tác chi tiết. Nội dung chỉ ghi lại các mốc đã có file hoặc cấu hình
tồn tại trong repository.

## Mục lục

1. [Xác định mục tiêu, phạm vi và nguyên tắc làm việc](#bước-1-xác-định-mục-tiêu-phạm-vi-và-nguyên-tắc-làm-việc)
2. [Thiết lập cấu trúc repository và môi trường Python cơ bản](#bước-2-thiết-lập-cấu-trúc-repository-và-môi-trường-python-cơ-bản)
3. [Khám phá dữ liệu Bluesky Jetstream](#bước-3-khám-phá-dữ-liệu-bluesky-jetstream)
4. [Tạo event envelope cho raw event](#bước-4-tạo-event-envelope-cho-raw-event)
5. [Profile schema từ sample Jetstream](#bước-5-profile-schema-từ-sample-jetstream)
6. [Normalize event sample thành record phẳng](#bước-6-normalize-event-sample-thành-record-phẳng)
7. [Dựng Kafka local và raw event topic](#bước-7-dựng-kafka-local-và-raw-event-topic)
8. [Publish sample event vào Kafka](#bước-8-publish-sample-event-vào-kafka)
9. [Xây dựng live ingestion gateway ban đầu](#bước-9-xây-dựng-live-ingestion-gateway-ban-đầu)
10. [Bổ sung cấu hình, logging và reliability cơ bản cho gateway](#bước-10-bổ-sung-cấu-hình-logging-và-reliability-cơ-bản-cho-gateway)
11. [Duy trì tài liệu kỹ thuật theo tiến độ pipeline](#bước-11-duy-trì-tài-liệu-kỹ-thuật-theo-tiến-độ-pipeline)
12. [Kiểm chứng Spark đọc raw event từ Kafka](#bước-12-kiểm-chứng-spark-đọc-raw-event-từ-kafka)
13. [Parse event envelope JSON trong Spark](#bước-13-parse-event-envelope-json-trong-spark)
14. [Ghi Bronze Parquet local bằng Spark streaming](#bước-14-ghi-bronze-parquet-local-bằng-spark-streaming)
15. [Đọc lại Bronze Parquet để kiểm chứng dữ liệu usable](#bước-15-đọc-lại-bronze-parquet-để-kiểm-chứng-dữ-liệu-usable)
16. [Partition Bronze local theo collection](#bước-16-partition-bronze-local-theo-collection)
17. [Giữ raw JSON gốc trong Bronze local](#bước-17-giữ-raw-json-gốc-trong-bronze-local)
18. [Partition Bronze local theo thời gian ingest](#bước-18-partition-bronze-local-theo-thời-gian-ingest)
19. [Dựng MinIO local làm S3-compatible object storage](#bước-19-dựng-minio-local-làm-s3-compatible-object-storage)
20. [Tạo bucket Bronze trên MinIO](#bước-20-tạo-bucket-bronze-trên-minio)
21. [Ghi Bronze Parquet lên MinIO bằng Spark S3A](#bước-21-ghi-bronze-parquet-lên-minio-bằng-spark-s3a)
22. [Đọc lại Bronze Parquet từ MinIO](#bước-22-đọc-lại-bronze-parquet-từ-minio)
23. [Tách cấu hình Spark S3A dùng chung](#bước-23-tách-cấu-hình-spark-s3a-dùng-chung)
24. [Bổ sung event kind vào event envelope](#bước-24-bổ-sung-event-kind-vào-event-envelope)
25. [Ghi Bronze raw events có event kind lên MinIO](#bước-25-ghi-bronze-raw-events-có-event-kind-lên-minio)
26. [Tách Bronze output theo event family](#bước-26-tách-bronze-output-theo-event-family)
27. [Đọc và kiểm chứng ba Bronze event family](#bước-27-đọc-và-kiểm-chứng-ba-bronze-event-family)
28. [Profile Bronze commit events để chuẩn bị Silver](#bước-28-profile-bronze-commit-events-để-chuẩn-bị-silver)
29. [Thiết kế Silver schema v1](#bước-29-thiết-kế-silver-schema-v1)
30. [Build Silver posts từ Bronze commit events](#bước-30-build-silver-posts-từ-bronze-commit-events)
31. [Đọc và kiểm chứng Silver posts](#bước-31-đọc-và-kiểm-chứng-silver-posts)
32. [Build Silver engagements từ Bronze commit events](#bước-32-build-silver-engagements-từ-bronze-commit-events)
33. [Đọc và kiểm chứng Silver engagements](#bước-33-đọc-và-kiểm-chứng-silver-engagements)
34. [Build Silver follows từ Bronze commit events](#bước-34-build-silver-follows-từ-bronze-commit-events)
35. [Đọc và kiểm chứng Silver follows](#bước-35-đọc-và-kiểm-chứng-silver-follows)
36. [Build Silver deleted records từ Bronze commit events](#bước-36-build-silver-deleted-records-từ-bronze-commit-events)
37. [Đọc và kiểm chứng Silver deleted records](#bước-37-đọc-và-kiểm-chứng-silver-deleted-records)
38. [Kiểm tra tổng quan Silver v1](#bước-38-kiểm-tra-tổng-quan-silver-v1)
39. [Build Gold event volume prototype từ Silver v1](#bước-39-build-gold-event-volume-prototype-từ-silver-v1)
40. [Đọc và kiểm chứng Gold event volume prototype](#bước-40-đọc-và-kiểm-chứng-gold-event-volume-prototype)
41. [Dựng ClickHouse local cho Gold serving layer](#bước-41-dựng-clickhouse-local-cho-gold-serving-layer)
42. [Tạo Gold serving table trong ClickHouse](#bước-42-tạo-gold-serving-table-trong-clickhouse)
43. [Load Gold event volume vào ClickHouse](#bước-43-load-gold-event-volume-vào-clickhouse)
44. [Tự động load Gold event volume vào ClickHouse](#bước-44-tự-động-load-gold-event-volume-vào-clickhouse)
45. [Kiểm tra Gold serving table trong ClickHouse](#bước-45-kiểm-tra-gold-serving-table-trong-clickhouse)
46. [Tách ClickHouse HTTP helper dùng chung](#bước-46-tách-clickhouse-http-helper-dùng-chung)

## Bước 1: Xác định mục tiêu, phạm vi và nguyên tắc làm việc

**Mục tiêu**

Xác định project sẽ xây dựng một streaming data pipeline từ Bluesky Jetstream,
dùng Kafka làm event backbone và chuẩn bị cho các tầng xử lý tiếp theo như Spark,
Data Lake, Iceberg và ClickHouse.

**Vì sao cần thực hiện**

Một data pipeline dễ bị phình scope nếu không xác định rõ mục tiêu, công nghệ và
ranh giới từng milestone. Bước này giúp phân biệt phần đã triển khai trong local
với kiến trúc dài hạn, tránh mô tả môi trường học tập như production-scale.

**Kết quả sau khi hoàn thành**

Repository có tài liệu chính thức mô tả mục tiêu dự án, phạm vi dữ liệu, kiến trúc
định hướng, tech stack, nguyên tắc thiết kế và cách làm việc với Codex.

**Các file liên quan**

- `AGENTS.md`
- `docs/tong-quan-du-an.md`
- `docs/huong-dan-lam-viec-voi-codex.md`
- `README.md`

**Kiến thức cần ghi nhớ**

- Data pipeline nên bắt đầu từ bài toán và phạm vi dữ liệu, không bắt đầu bằng
  việc dựng thật nhiều công nghệ.
- Kiến trúc tổng thể là định hướng dài hạn; mỗi milestone chỉ triển khai phần cần
  thiết để tạo ra một luồng dữ liệu có thể kiểm chứng.
- Trong phỏng vấn, cần nói rõ đâu là thành phần đã chạy được, đâu là thiết kế tiếp
  theo, và đâu là giới hạn của môi trường local.

## Bước 2: Thiết lập cấu trúc repository và môi trường Python cơ bản

**Mục tiêu**

Tạo nền tảng project Python có cấu trúc rõ ràng, tách code pipeline, script chạy
tay, test, docs và dữ liệu local.

**Vì sao cần thực hiện**

Một data project thực tế sẽ tăng nhanh số lượng file. Nếu không tách sớm vai trò
của từng nhóm file, logic pipeline, script thử nghiệm, dữ liệu local và tài liệu
sẽ bị lẫn vào nhau.

**Kết quả sau khi hoàn thành**

Repository có cấu trúc hiện tại:

- `src/bluesky_pipeline/` cho logic pipeline dùng lại.
- `scripts/` cho script discovery và thao tác local.
- `tests/` cho test.
- `docs/` cho tài liệu kỹ thuật.
- `data/` cho dữ liệu local không commit.

Project cũng có dependency Python cơ bản cho WebSocket, Kafka client và pytest.

**Các file liên quan**

- `.gitignore`
- `requirements.txt`
- `src/bluesky_pipeline/event_envelope.py`
- `src/bluesky_pipeline/normalize_event.py`
- `src/bluesky_pipeline/ingestion_gateway.py`
- `scripts/jetstream_probe.py`
- `scripts/analyze_sample.py`
- `scripts/normalize_sample.py`
- `tests/test_event_envelope.py`
- `docs/tong-quan-du-an.md`

**Kiến thức cần ghi nhớ**

- `src/` nên chứa logic có thể dùng lại, không chứa dữ liệu sample.
- `scripts/` phù hợp cho thao tác discovery, kiểm chứng local hoặc utility tạm.
- `data/` là dữ liệu sinh ra khi chạy, không nên commit.
- Cấu trúc repo nên tiến hóa theo độ phức tạp, không cần tạo trước mọi thư mục cho
  các milestone tương lai.

## Bước 3: Khám phá dữ liệu Bluesky Jetstream

**Mục tiêu**

Kết nối tới Bluesky Jetstream, thu thập sample event thật và xác định các
collection nằm trong scope hiện tại.

**Vì sao cần thực hiện**

Trước khi thiết kế Kafka topic, Bronze layout hoặc Spark schema, cần quan sát dữ
liệu thật để hiểu event shape, operation, field thiếu và khác biệt giữa các
collection.

**Kết quả sau khi hoàn thành**

Project có script probe đọc các collection:

- `app.bsky.feed.post`
- `app.bsky.feed.like`
- `app.bsky.feed.repost`
- `app.bsky.graph.follow`

Script ghi sample event envelope vào JSONL local và tài liệu schema đã ghi lại các
quan sát chính về collection, operation, record type và `subject` shape.

**Các file liên quan**

- `scripts/jetstream_probe.py`
- `data/probe/jetstream_sample.jsonl`
- `docs/jetstream-schema-notes.md`
- `requirements.txt`

**Kiến thức cần ghi nhớ**

- Với nguồn streaming bên ngoài, nên có bước discovery trước khi chốt schema.
- Không nên giả định mọi event đều có cùng shape; cùng một field như `subject` có
  thể là object, string hoặc null tùy collection.
- Delete event có thể thiếu `record`, nên mọi logic parse/normalize phải xử lý
  missing field.

## Bước 4: Tạo event envelope cho raw event

**Mục tiêu**

Bọc raw Jetstream event bằng một envelope nội bộ có metadata ổn định để dùng cho
ingestion và Kafka raw topic.

**Vì sao cần thực hiện**

Raw event từ nguồn bên ngoài không nên được publish trực tiếp mà không có metadata
nội bộ. Envelope giúp pipeline biết event đến từ đâu, nhận lúc nào, thuộc
collection nào, operation gì và vẫn giữ payload gốc để audit/replay.

**Kết quả sau khi hoàn thành**

Project có module tạo event envelope với các field như `schema_version`, `source`,
`received_at`, `collection`, `operation`, `repository_did`, `jetstream_time_us` và
`payload`. Test hiện tại kiểm tra các case create, delete, missing commit và event
rỗng.

**Các file liên quan**

- `src/bluesky_pipeline/event_envelope.py`
- `tests/test_event_envelope.py`
- `scripts/jetstream_probe.py`
- `docs/jetstream-schema-notes.md`

**Kiến thức cần ghi nhớ**

- Event envelope là contract dữ liệu nội bộ giữa ingestion và các tầng downstream.
- Giữ raw payload trong envelope giúp replay và reprocess khi logic normalize thay
  đổi.
- Không nên tuyên bố exactly-once chỉ vì có Kafka hoặc envelope; cần chứng minh
  toàn bộ end-to-end mới được nói đến guarantee đó.

## Bước 5: Profile schema từ sample Jetstream

**Mục tiêu**

Đọc sample JSONL đã thu thập để thống kê collection, operation, record type,
`subject` shape và danh sách field xuất hiện theo từng collection.

**Vì sao cần thực hiện**

Schema profiling giúp chuyển từ quan sát thủ công sang thống kê có hệ thống. Đây
là nền tảng để thiết kế normalization và Silver schema sau này.

**Kết quả sau khi hoàn thành**

Project có script phân tích sample và tài liệu ghi lại các kết quả quan sát như
like/post/repost/follow counts, delete event thiếu record và sự khác nhau của
`record.subject`.

**Các file liên quan**

- `scripts/analyze_sample.py`
- `data/probe/jetstream_sample.jsonl`
- `docs/jetstream-schema-notes.md`

**Kiến thức cần ghi nhớ**

- Schema của event streaming thường là semi-structured và thay đổi theo loại
  event.
- Việc profile dữ liệu thật giúp tránh thiết kế schema quá sớm hoặc quá lý tưởng.
- Với data pipeline, tài liệu schema observation là một phần quan trọng của thiết
  kế, không chỉ là ghi chú phụ.

## Bước 6: Normalize event sample thành record phẳng

**Mục tiêu**

Biến event envelope lồng nhau thành một record phẳng hơn để dễ quan sát và chuẩn
bị cho Spark normalization/Silver schema sau này.

**Vì sao cần thực hiện**

Kafka raw topic nên giữ dữ liệu gần nguồn, nhưng các tầng xử lý tiếp theo cần
record dễ đọc, dễ validate và dễ query hơn. Normalize sample giúp kiểm chứng cách
trích field quan trọng trước khi đưa vào Spark.

**Kết quả sau khi hoàn thành**

Project có module `normalize_event()` tạo record phẳng với các field như
`repository_did`, `collection`, `operation`, `rkey`, `cid`, `record_type`,
`record_created_at`, `text`, `subject_uri`, `subject_cid` và `raw_event`.

Script local tạo file normalized JSONL để kiểm tra kết quả.

**Các file liên quan**

- `src/bluesky_pipeline/normalize_event.py`
- `scripts/normalize_sample.py`
- `data/probe/jetstream_normalized_sample.jsonl`
- `docs/jetstream-schema-notes.md`

**Kiến thức cần ghi nhớ**

- Normalize không đồng nghĩa với bỏ raw data; raw event vẫn cần giữ để audit và
  replay.
- Một schema phẳng ban đầu có thể hữu ích cho discovery, nhưng chưa phải Silver
  schema cuối cùng.
- Field `subject_uri` hiện có thể là AT URI hoặc DID tùy collection; khi thiết kế
  Silver có thể cần tách nghĩa rõ hơn.

## Bước 7: Dựng Kafka local và raw event topic

**Mục tiêu**

Dựng Kafka local bằng Docker Compose và chuẩn bị raw topic để làm buffer giữa
Jetstream gateway và các consumer downstream.

**Vì sao cần thực hiện**

Kafka giúp tách ingestion khỏi compute engine, tạo event log ngắn hạn, hỗ trợ
partitioning, replay theo offset và consumer group. Đây là một năng lực cốt lõi
của streaming data platform.

**Kết quả sau khi hoàn thành**

Repository có Docker Compose chạy Kafka local bằng image `apache/kafka:3.7.0`.
Topic raw event được tài liệu hóa là `bluesky.raw.events.v1` với 3 partition và
replication factor 1 trong local.

**Các file liên quan**

- `docker-compose.yml`
- `requirements.txt`
- `docs/jetstream-schema-notes.md`

**Kiến thức cần ghi nhớ**

- Kafka trong local không phải production Kafka cluster.
- Replication factor 1 phù hợp local nhưng không có khả năng chịu lỗi broker.
- Phân biệt listener dùng trong Docker network và listener dùng từ host là điểm
  quan trọng khi debug Kafka local.
- Kafka là buffer/event backbone, không phải kho lưu trữ lịch sử dài hạn của
  project.

## Bước 8: Publish sample event vào Kafka

**Mục tiêu**

Kiểm chứng Python producer có thể publish event envelope từ sample JSONL vào Kafka
raw topic và consumer có thể đọc lại.

**Vì sao cần thực hiện**

Trước khi nối live Jetstream vào Kafka, cần kiểm chứng từng phần nhỏ: producer
client, Kafka connectivity, topic, message key và message value.

**Kết quả sau khi hoàn thành**

Project có script publish một event và script publish batch nhỏ từ
`data/probe/jetstream_sample.jsonl` vào topic `bluesky.raw.events.v1`. Tài liệu đã
ghi lại luồng kiểm chứng:

`sample JSONL -> Python Producer -> Kafka topic -> Kafka Console Consumer`.

**Các file liên quan**

- `scripts/publish_sample_to_kafka.py`
- `scripts/publish_sample_batch_to_kafka.py`
- `data/probe/jetstream_sample.jsonl`
- `docs/jetstream-schema-notes.md`
- `requirements.txt`

**Kiến thức cần ghi nhớ**

- Nên kiểm chứng Kafka bằng sample nhỏ trước khi nối live stream.
- Message key hiện tại là `repository_did` để giữ ordering tương đối trong phạm vi
  repository.
- Producer có buffer nội bộ, nên cần `flush()` khi muốn đảm bảo message đã được
  gửi trước khi process kết thúc.

## Bước 9: Xây dựng live ingestion gateway ban đầu

**Mục tiêu**

Kết nối live tới Bluesky Jetstream, bọc event bằng envelope và publish trực tiếp
vào Kafka raw topic.

**Vì sao cần thực hiện**

Đây là lát cắt streaming ingestion đầu tiên của project. Nó thay thế việc publish
từ sample local bằng luồng dữ liệu live:

`Jetstream WebSocket -> Python ingestion gateway -> Kafka raw topic`.

**Kết quả sau khi hoàn thành**

Project có ingestion gateway đọc 4 collection trong scope, tạo event envelope và
publish message vào `bluesky.raw.events.v1`. Gateway dùng `repository_did` làm
Kafka key và event envelope JSON làm message value.

**Các file liên quan**

- `src/bluesky_pipeline/ingestion_gateway.py`
- `src/bluesky_pipeline/event_envelope.py`
- `docker-compose.yml`
- `docs/jetstream-schema-notes.md`
- `requirements.txt`

**Kiến thức cần ghi nhớ**

- Gateway chỉ nên vận chuyển và thêm metadata ingestion, không nên làm business
  aggregation hoặc normalize sâu theo từng collection.
- Giữ gateway mỏng giúp tách trách nhiệm giữa ingestion, Kafka và Spark.
- Live streaming ingestion cần được kiểm chứng bằng hành vi thật: event đi vào
  Kafka và consumer đọc được.

## Bước 10: Bổ sung cấu hình, logging và reliability cơ bản cho gateway

**Mục tiêu**

Làm gateway tiến gần hơn tới process chạy dài hạn bằng cách đưa cấu hình ra
environment variable, thêm structured logging, retry hữu hạn và graceful shutdown
cơ bản.

**Vì sao cần thực hiện**

Streaming gateway không nên phụ thuộc vào giá trị hard-code trong code. Nó cũng
cần log rõ ràng, retry khi WebSocket lỗi tạm thời và flush producer trước khi
thoát để giảm rủi ro mất message đang nằm trong buffer.

**Kết quả sau khi hoàn thành**

Gateway hiện có các biến cấu hình như `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC`,
`MAX_EVENTS`, `MAX_RETRIES` và `RETRY_BACKOFF_SECONDS`. Gateway có logging cơ bản,
bounded retry khi WebSocket lỗi và graceful shutdown cơ bản. Khi process bị
interrupt hoặc task bị cancel, gateway đi qua nhánh kết thúc để flush Kafka
producer trước khi thoát.

**Các file liên quan**

- `src/bluesky_pipeline/ingestion_gateway.py`
- `docs/jetstream-schema-notes.md`
- `docs/tong-quan-du-an.md`

**Kiến thức cần ghi nhớ**

- Config theo môi trường nên đi qua environment variable; secret và credential
  không được hard-code.
- Retry cần có giới hạn để tránh loop vô hạn khi nguồn lỗi liên tục.
- Graceful shutdown trong producer-based app thường cần flush buffer trước khi
  thoát.
- `KeyboardInterrupt` và `asyncio.CancelledError` là tín hiệu dừng chủ động, nên
  cần được xử lý khác với lỗi kết nối hoặc lỗi Kafka thật.
- Structured logging giúp quan sát process tốt hơn `print`, nhưng chưa thay thế
  metrics/monitoring đầy đủ.

## Bước 11: Duy trì tài liệu kỹ thuật theo tiến độ pipeline

**Mục tiêu**

Duy trì tài liệu vừa phục vụ thiết kế, vừa phục vụ học tập và phỏng vấn.

**Vì sao cần thực hiện**

Data Engineering project không chỉ là code. Người làm cần giải thích được lý do
chọn kiến trúc, giới hạn hiện tại, dữ liệu đi qua từng tầng thế nào và bài học nào
có thể áp dụng cho dự án khác.

**Kết quả sau khi hoàn thành**

Repository có các tài liệu mô tả tổng quan dự án, quy tắc làm việc, ghi chú schema
Jetstream và quy trình xây dựng pipeline. Từ thời điểm tài liệu này được tạo, mỗi
bước chính đã được xác nhận hoàn thành cần được cập nhật vào tài liệu trước khi
chuyển sang bước tiếp theo.

**Các file liên quan**

- `AGENTS.md`
- `docs/huong-dan-lam-viec-voi-codex.md`
- `docs/tong-quan-du-an.md`
- `docs/jetstream-schema-notes.md`
- `docs/quy-trinh-xay-dung-pipeline.md`

**Kiến thức cần ghi nhớ**

- Tài liệu quy trình không phải nhật ký command; nó là bản tóm tắt các quyết định
  và mốc kiến trúc đã hoàn thành.
- Khi phỏng vấn, cách trình bày tốt là đi theo luồng dữ liệu và lý do kỹ thuật:
  nguồn dữ liệu, ingestion, Kafka, xử lý, lưu trữ, phục vụ truy vấn và vận hành.
- Chỉ ghi những gì đã hoàn thành và kiểm chứng; không biến kế hoạch tương lai
  thành thành quả hiện tại.

## Bước 12: Kiểm chứng Spark đọc raw event từ Kafka

**Mục tiêu**

Tạo lát cắt Spark Structured Streaming đầu tiên đọc trực tiếp từ Kafka raw topic
và in dữ liệu ra console để kiểm chứng kết nối.

**Vì sao cần thực hiện**

Sau khi ingestion gateway đã publish event vào Kafka, cần chứng minh tầng compute
có thể consume cùng topic đó. Đây là cầu nối đầu tiên giữa message broker và Spark,
trước khi parse schema, validate dữ liệu hoặc ghi Bronze.

**Kết quả sau khi hoàn thành**

Project có script Spark local đọc topic `bluesky.raw.events.v1`, cast Kafka
`key` và `value` từ binary sang string, rồi in các cột raw ra console. Script đã
chạy được sau khi cài Java, thêm `pyspark==3.5.1` và cấu hình Kafka connector
`spark-sql-kafka-0-10_2.12:3.5.1`.

**Các file liên quan**

- `scripts/spark_read_kafka_raw.py`
- `scripts/publish_sample_batch_to_kafka.py`
- `requirements.txt`
- `docker-compose.yml`

**Kiến thức cần ghi nhớ**

- PySpark cần Java để khởi động Spark engine; lỗi `JAVA_GATEWAY_EXITED` thường là
  dấu hiệu cần kiểm tra Java trước.
- Spark core không tự có Kafka data source; muốn dùng `.format("kafka")` cần thêm
  package `spark-sql-kafka-0-10`.
- Kafka message trong Spark có `key` và `value` dạng binary, nên thường cần cast
  sang string trước khi parse JSON.
- Checkpoint là bắt buộc với streaming query nghiêm túc, kể cả khi bước hiện tại
  mới in ra console để kiểm chứng.

## Bước 13: Parse event envelope JSON trong Spark

**Mục tiêu**

Chuyển `message_value` từ JSON string thành các cột có schema trong Spark
Structured Streaming.

**Vì sao cần thực hiện**

Kafka chỉ lưu message dạng byte. Để Spark có thể validate, transform, partition
và ghi xuống Bronze/Silver ở các bước sau, raw JSON cần được parse thành cột có
kiểu dữ liệu rõ ràng thay vì chỉ là một chuỗi dài.

**Kết quả sau khi hoàn thành**

Script Spark đọc topic `bluesky.raw.events.v1`, parse envelope JSON bằng
`from_json` và in ra console các cột như `schema_version`, `source`,
`received_at`, `collection`, `operation`, `repository_did` và
`jetstream_time_us`.

**Các file liên quan**

- `scripts/spark_read_kafka_raw.py`
- `requirements.txt`
- `docs/quy-trinh-xay-dung-pipeline.md`

**Kiến thức cần ghi nhớ**

- `from_json` cần schema tường minh để Spark biết cách chuyển JSON string thành
  struct column.
- Schema ở bước này mới là envelope schema tối thiểu, chưa phải schema đầy đủ cho
  từng collection bên trong `payload`.
- Parse thành cột giúp các bước sau dễ filter, partition, validate và ghi dữ liệu
  ra storage hơn nhiều so với xử lý một chuỗi JSON thô.
- Console sink chỉ dùng để kiểm chứng; sink thật của Bronze sẽ là Parquet.

## Bước 14: Ghi Bronze Parquet local bằng Spark streaming

**Mục tiêu**

Đổi Spark sink từ console sang Parquet để tạo tầng Bronze local đầu tiên.

**Vì sao cần thực hiện**

Console sink chỉ chứng minh Spark đọc và parse được dữ liệu. Pipeline cần một
storage sink để lưu lịch sử raw/envelope event phục vụ audit, replay và các bước
xử lý tiếp theo. Ở giai đoạn này, ghi local Parquet giúp kiểm chứng logic Spark
file sink trước khi đưa MinIO vào.

**Kết quả sau khi hoàn thành**

Spark Structured Streaming đọc topic `bluesky.raw.events.v1`, parse envelope JSON
và ghi dữ liệu ra `data/bronze/bluesky_raw_events` dưới dạng Parquet. Khi publish
sample batch vào Kafka, thư mục Bronze local sinh ra các file `.parquet` và metadata
của Spark file sink.

**Các file liên quan**

- `scripts/spark_read_kafka_raw.py`
- `scripts/publish_sample_batch_to_kafka.py`
- `data/bronze/bluesky_raw_events`
- `data/checkpoints/spark_read_kafka_raw`

**Kiến thức cần ghi nhớ**

- Bronze là tầng dữ liệu gần nguồn, ưu tiên append, audit và replay.
- Với Spark Structured Streaming file sink, output path và checkpoint path là một
  cặp trạng thái phải nhất quán với nhau.
- Không nên xóa riêng `_spark_metadata` hoặc riêng checkpoint trong lúc test. Nếu
  muốn chạy lại sạch ở local discovery, cần xóa cả output path và checkpoint path
  cùng lúc.
- Local Parquet chỉ là bước kiểm chứng sink; thiết kế chính thức của project sẽ
  đưa Bronze lên MinIO theo kiến trúc S3-compatible object storage.

## Bước 15: Đọc lại Bronze Parquet để kiểm chứng dữ liệu usable

**Mục tiêu**

Đọc ngược dữ liệu Bronze Parquet đã ghi để xác nhận output của Spark streaming có
thể được sử dụng bởi job Spark khác.

**Vì sao cần thực hiện**

Một pipeline không chỉ cần ghi file thành công, mà còn cần chứng minh dữ liệu ghi
ra có schema đọc được và có record thực tế. Bước này kiểm tra chất lượng tối thiểu
của sink trước khi tiếp tục tối ưu layout hoặc chuyển sang MinIO.

**Kết quả sau khi hoàn thành**

Project có script đọc `data/bronze/bluesky_raw_events`, in schema, hiển thị một số
record mẫu và in `bronze_count`. Kết quả chạy cho thấy dữ liệu Bronze local đọc lại
được bằng Spark.

**Các file liên quan**

- `scripts/read_bronze_parquet.py`
- `data/bronze/bluesky_raw_events`
- `scripts/spark_read_kafka_raw.py`

**Kiến thức cần ghi nhớ**

- Ghi được file chưa đủ; cần đọc lại để kiểm chứng schema và dữ liệu.
- Parquet là định dạng columnar phù hợp cho tầng Bronze vì Spark đọc/ghi hiệu quả
  và giữ được schema.
- Script đọc kiểm chứng nên tách khỏi streaming writer để tránh trộn trách nhiệm
  giữa ghi dữ liệu và quan sát dữ liệu.

## Bước 16: Partition Bronze local theo collection

**Mục tiêu**

Tổ chức dữ liệu Bronze Parquet local theo `collection` để dễ quan sát và đọc theo
từng loại event.

**Vì sao cần thực hiện**

Bluesky Jetstream có nhiều collection như post, like, repost và follow. Nếu tất cả
event nằm chung một layout phẳng, các job sau sẽ khó đọc chọn lọc theo loại event.
Partition theo `collection` là bước đơn giản nhưng có giá trị rõ ràng cho truy vấn,
debug và các transform downstream.

**Kết quả sau khi hoàn thành**

Bronze writer ghi dữ liệu xuống `data/bronze/bluesky_raw_events` với layout thư
mục dạng `collection=...`. Khi publish sample batch, output sinh ra các thư mục
như `collection=app.bsky.feed.like` và `collection=app.bsky.feed.post`.

**Các file liên quan**

- `scripts/spark_read_kafka_raw.py`
- `data/bronze/bluesky_raw_events`
- `data/checkpoints/spark_read_kafka_raw`

**Kiến thức cần ghi nhớ**

- Partition column nên là cột thường được dùng để filter hoặc phân tách luồng xử
  lý downstream.
- Partition quá ít thì chưa tận dụng được pruning; partition quá nhiều dễ tạo
  nhiều file/thư mục nhỏ. Ở bước này `collection` là lựa chọn hợp lý vì số lượng
  collection trong scope còn nhỏ.
- Khi thay đổi partition layout của file sink streaming, cần reset cả output path
  và checkpoint path trong môi trường local test.

## Bước 17: Giữ raw JSON gốc trong Bronze local

**Mục tiêu**

Bổ sung cột `message_value` vào Bronze Parquet local để mỗi record đã parse vẫn
giữ lại raw Kafka message gốc.

**Vì sao cần thực hiện**

Bronze là tầng gần nguồn, phục vụ audit, replay và reprocess. Nếu Bronze chỉ lưu
các cột metadata đã parse mà bỏ raw JSON, pipeline sẽ khó xử lý lại dữ liệu khi
schema downstream thay đổi hoặc logic parse ban đầu có lỗi.

**Kết quả sau khi hoàn thành**

Spark writer giữ lại `message_value` cùng với Kafka metadata và các field envelope
đã parse. Sau khi reset output/checkpoint local, chạy lại writer và publish sample
batch, script đọc Bronze hiển thị raw JSON trong output và in được
`bronze_count: 9405`.

**Các file liên quan**

- `scripts/spark_read_kafka_raw.py`
- `scripts/read_bronze_parquet.py`
- `scripts/publish_sample_batch_to_kafka.py`
- `data/bronze/bluesky_raw_events`
- `data/checkpoints/spark_read_kafka_raw`

**Kiến thức cần ghi nhớ**

- Bronze nên giữ dữ liệu gần nguồn nhất có thể để phục vụ audit và replay.
- Parse JSON thành cột giúp query và partition dễ hơn, nhưng không nên thay thế
  hoàn toàn raw payload ở tầng Bronze.
- Khi thay đổi schema output của Spark streaming file sink trong local test, cần
  reset đồng thời output path và checkpoint path để tránh lẫn schema cũ và mới.

## Bước 18: Partition Bronze local theo thời gian ingest

**Mục tiêu**

Bổ sung các cột `kafka_timestamp`, `ingest_date` và `ingest_hour`, sau đó tổ chức
Bronze Parquet local theo layout thời gian ingest kết hợp với `collection`.

**Vì sao cần thực hiện**

Bronze cần layout dễ đọc theo khoảng thời gian vì hầu hết thao tác audit, replay,
backfill và kiểm tra dữ liệu đều bắt đầu từ một khoảng ngày/giờ cụ thể. Partition
theo thời gian ingest là lựa chọn ổn định hơn so với các key có cardinality cao
như DID, post URI hoặc hashtag.

**Kết quả sau khi hoàn thành**

Spark writer ghi dữ liệu xuống `data/bronze/bluesky_raw_events` với layout dạng
`ingest_date=.../ingest_hour=.../collection=...`. Script đọc Bronze hiển thị dữ
liệu đã ghi, bao gồm raw JSON trong `message_value`, các field envelope đã parse
và các cột thời gian ingest.

**Các file liên quan**

- `scripts/spark_read_kafka_raw.py`
- `scripts/read_bronze_parquet.py`
- `scripts/publish_sample_batch_to_kafka.py`
- `data/bronze/bluesky_raw_events`
- `data/checkpoints/spark_read_kafka_raw`

**Kiến thức cần ghi nhớ**

- Partition theo ngày/giờ giúp các job sau đọc chọn lọc dữ liệu theo khoảng thời
  gian, giảm số file cần scan.
- `kafka_timestamp` là thời điểm Kafka ghi nhận message, khác với `received_at`
  của gateway và `jetstream_time_us` từ nguồn.
- Không nên partition Bronze theo key có quá nhiều giá trị như DID hoặc URI vì dễ
  tạo nhiều thư mục/file nhỏ và làm layout khó quản lý.

## Bước 19: Dựng MinIO local làm S3-compatible object storage

**Mục tiêu**

Bổ sung MinIO vào Docker Compose để project có object storage local, chuẩn bị cho
Bronze Data Lake theo kiến trúc S3-compatible.

**Vì sao cần thực hiện**

Bronze local filesystem chỉ phù hợp để kiểm chứng Spark file sink ban đầu. Data
Lake thực tế cần tách compute khỏi storage, lưu dữ liệu ở object storage và dùng
đường dẫn S3-compatible để Spark, các job backfill và các tầng downstream có thể
đọc lại dữ liệu ổn định hơn.

**Kết quả sau khi hoàn thành**

Docker Compose có service `minio` chạy bằng image version cụ thể, expose API port
`9000` và console port `9001`. Container `bluesky-minio` chạy thành công cùng với
Kafka trong môi trường local.

**Các file liên quan**

- `docker-compose.yml`
- `docs/quy-trinh-xay-dung-pipeline.md`

**Kiến thức cần ghi nhớ**

- MinIO là object storage local tương thích S3 API, không phải S3 thật trên cloud.
- Credential `minioadmin/minioadmin` chỉ dùng cho môi trường học local, không được
  xem là cấu hình production.
- Dựng MinIO mới là chuẩn bị hạ tầng storage; cần tạo bucket và cấu hình Spark S3A
  trước khi thật sự ghi Bronze lên MinIO.

## Bước 20: Tạo bucket Bronze trên MinIO

**Mục tiêu**

Tạo bucket `bluesky-lake` trong MinIO để làm namespace lưu dữ liệu Data Lake local.

**Vì sao cần thực hiện**

Spark không thể ghi dữ liệu vào object storage nếu bucket chưa tồn tại. Bucket là
đơn vị chứa object ở tầng S3-compatible, tương tự thư mục gốc của lake trong môi
trường local.

**Kết quả sau khi hoàn thành**

MinIO có bucket `bluesky-lake`, sẵn sàng nhận dữ liệu Bronze ở đường dẫn như
`s3a://bluesky-lake/bronze/bluesky_raw_events`.

**Các file liên quan**

- `docker-compose.yml`
- `docs/quy-trinh-xay-dung-pipeline.md`

**Kiến thức cần ghi nhớ**

- Bucket phải tồn tại trước khi Spark ghi object vào MinIO.
- `s3a://` là scheme Hadoop/Spark dùng để truy cập storage tương thích S3.
- Trong local, bucket MinIO thay thế vai trò của S3 bucket trên cloud nhưng không
  phải môi trường production.

## Bước 21: Ghi Bronze Parquet lên MinIO bằng Spark S3A

**Mục tiêu**

Chuyển Spark Bronze writer từ local filesystem sang MinIO bằng đường dẫn `s3a://`.

**Vì sao cần thực hiện**

Milestone Bronze Data Lake cần chứng minh Spark có thể ghi dữ liệu ra object
storage thay vì chỉ ghi vào ổ đĩa local. Đây là bước chuyển từ kiểm chứng file sink
cục bộ sang kiến trúc S3-compatible đúng định hướng của project.

**Kết quả sau khi hoàn thành**

Spark Structured Streaming ghi Bronze Parquet và checkpoint vào bucket
`bluesky-lake` trên MinIO. MinIO Console hiển thị object dưới các prefix như
`bronze/bluesky_raw_events/` và `checkpoints/spark_read_kafka_raw/`.

**Các file liên quan**

- `scripts/spark_read_kafka_raw.py`
- `docker-compose.yml`
- `docs/quy-trinh-xay-dung-pipeline.md`

**Kiến thức cần ghi nhớ**

- Spark dùng Hadoop S3A connector để đọc/ghi object storage tương thích S3.
- Với MinIO local, cần bật path-style access và trỏ endpoint về
  `http://localhost:9000`.
- Checkpoint cũng cần nằm trên storage ổn định tương ứng với output sink để Spark
  có thể quản lý tiến độ streaming query.

## Bước 22: Đọc lại Bronze Parquet từ MinIO

**Mục tiêu**

Đọc ngược dữ liệu Bronze Parquet từ MinIO để xác nhận object đã ghi có thể được
Spark job khác sử dụng.

**Vì sao cần thực hiện**

Ghi object thành công chưa đủ để coi Bronze usable. Pipeline cần chứng minh dữ
liệu trên MinIO có schema đọc được, có record thực tế và có thể dùng làm input cho
các bước downstream như validation, normalization hoặc replay.

**Kết quả sau khi hoàn thành**

Script đọc Bronze trỏ tới `s3a://bluesky-lake/bronze/bluesky_raw_events`, cấu hình
S3A connector cho MinIO local và in được schema cùng sample rows từ dữ liệu đã ghi.

**Các file liên quan**

- `scripts/read_bronze_parquet.py`
- `scripts/spark_read_kafka_raw.py`
- `docker-compose.yml`
- `docs/quy-trinh-xay-dung-pipeline.md`

**Kiến thức cần ghi nhớ**

- Mỗi job Spark đọc/ghi MinIO cần có cấu hình S3A endpoint, credential, path-style
  access và implementation class.
- Kiểm chứng đọc lại là bước bắt buộc để tránh nhầm giữa “ghi file/object thành
  công” và “dữ liệu downstream thật sự dùng được”.
- Bronze trên MinIO là source để các job Spark tiếp theo đọc lại, không phụ thuộc
  vào dữ liệu local trong `data/`.

## Bước 23: Tách cấu hình Spark S3A dùng chung

**Mục tiêu**

Tách logic tạo `SparkSession` có cấu hình S3A sang module dùng chung để writer và
reader không lặp lại cấu hình MinIO.

**Vì sao cần thực hiện**

Khi nhiều Spark script cùng đọc/ghi MinIO, việc lặp endpoint, credential, package
và S3A options ở từng file dễ gây sai lệch cấu hình. Module dùng chung giúp các
script Spark dùng cùng một cách kết nối object storage.

**Kết quả sau khi hoàn thành**

Project có module `src/bluesky_pipeline/spark_session.py` tạo SparkSession local
đã cấu hình S3A. `scripts/spark_read_kafka_raw.py` và
`scripts/read_bronze_parquet.py` dùng lại helper này. Script đọc Bronze từ MinIO
đã chạy thành công khi đặt `PYTHONPATH=src`.

**Các file liên quan**

- `src/bluesky_pipeline/spark_session.py`
- `scripts/spark_read_kafka_raw.py`
- `scripts/read_bronze_parquet.py`
- `docs/quy-trinh-xay-dung-pipeline.md`

**Kiến thức cần ghi nhớ**

- Cấu hình kết nối storage là phần dùng chung, nên tách ra khi nhiều job Spark
  cùng cần sử dụng.
- `PYTHONPATH=src` giúp Python tìm package local khi chạy script trực tiếp từ repo
  mà chưa cài project dưới dạng package.
- Tách helper chỉ nên làm sau khi có duplication thật và đã kiểm chứng hành vi
  trước đó chạy đúng.

## Bước 24: Bổ sung event kind vào event envelope

**Mục tiêu**

Bổ sung `event_kind` vào event envelope để phân biệt commit event với các
non-commit event như `identity` và `account`.

**Vì sao cần thực hiện**

Trước đó pipeline chủ yếu dựa vào `collection` và `operation`, nhưng hai field này
chỉ tồn tại với commit event. Khi đưa `identity/account` vào scope phân tích,
pipeline cần một field cấp envelope để phân loại event family trước khi ghi Bronze
hoặc normalize downstream.

**Kết quả sau khi hoàn thành**

`build_event_envelope()` lấy `event_kind` từ `raw_event["kind"]`. Test envelope đã
kiểm chứng create commit event có `event_kind = "commit"`, identity event không có
commit vẫn giữ `event_kind = "identity"`, và event rỗng có `event_kind = None`.

**Các file liên quan**

- `src/bluesky_pipeline/event_envelope.py`
- `tests/test_event_envelope.py`
- `docs/tong-quan-du-an.md`
- `docs/jetstream-schema-notes.md`

**Kiến thức cần ghi nhớ**

- `collection` là metadata của commit event, không phải mọi Jetstream event đều có
  field này.
- `event_kind` là field phân loại cấp event, phù hợp để route dữ liệu sang các
  Bronze path khác nhau.
- Khi mở rộng contract envelope, cần cập nhật test để khóa hành vi cho cả event
  commit và non-commit.

## Bước 25: Ghi Bronze raw events có event kind lên MinIO

**Mục tiêu**

Cập nhật Spark Bronze writer để parse và ghi `event_kind` xuống MinIO, giúp phân
biệt `commit`, `identity` và `account` ở tầng Bronze.

**Vì sao cần thực hiện**

Khi scope mở rộng sang account lifecycle events, `collection` không còn đủ để phân
loại mọi event. `event_kind` cho phép Bronze giữ cả commit và non-commit event mà
vẫn đọc được theo từng event family.

**Kết quả sau khi hoàn thành**

Spark writer đọc topic schema mới, parse được `event_kind` và ghi dữ liệu lên
MinIO với layout có các partition `event_kind=commit`, `event_kind=identity` và
`event_kind=account`.

**Các file liên quan**

- `src/bluesky_pipeline/event_envelope.py`
- `src/bluesky_pipeline/ingestion_gateway.py`
- `scripts/spark_read_kafka_raw.py`
- `scripts/publish_sample_batch_to_kafka.py`
- `docs/quy-trinh-xay-dung-pipeline.md`

**Kiến thức cần ghi nhớ**

- Khi schema của message envelope thay đổi, dữ liệu cũ trong Kafka topic có thể
  không có field mới. Với local learning, tạo topic version mới là cách rõ ràng để
  tránh lẫn schema cũ và mới.
- `event_kind` là partition có cardinality thấp, phù hợp để đọc chọn lọc theo
  nhóm event.
- Đây vẫn là bước chuyển tiếp; layout sạch hơn sẽ tách commit, identity và account
  sang các Bronze path riêng.

## Bước 26: Tách Bronze output theo event family

**Mục tiêu**

Tách Spark Bronze writer thành ba output path riêng cho commit, identity và account
events.

**Vì sao cần thực hiện**

Commit events có `collection`, `operation` và record payload, trong khi
identity/account events không có collection. Nếu ghi chung một path, Bronze dễ có
partition `collection=NULL` hoặc layout khó hiểu. Tách theo event family giúp mỗi
nhóm có schema và partition phù hợp hơn.

**Kết quả sau khi hoàn thành**

MinIO bucket `bluesky-lake` có ba prefix Bronze:

```text
bronze/bluesky_commit_events/
bronze/bluesky_identity_events/
bronze/bluesky_account_events/
```

Commit events được partition theo `ingest_date`, `ingest_hour` và `collection`.
Identity/account events được partition theo `ingest_date` và `ingest_hour`.

**Các file liên quan**

- `scripts/spark_read_kafka_raw.py`
- `src/bluesky_pipeline/event_envelope.py`
- `src/bluesky_pipeline/ingestion_gateway.py`
- `docs/tong-quan-du-an.md`
- `docs/jetstream-schema-notes.md`
- `docs/quy-trinh-xay-dung-pipeline.md`

**Kiến thức cần ghi nhớ**

- Không phải mọi event từ Jetstream đều là commit event có collection.
- Bronze layout nên phản ánh bản chất dữ liệu thay vì ép các event khác schema vào
  cùng một partition tree.
- Tách path theo event family giúp downstream đọc đúng nguồn dữ liệu cho từng bài
  toán phân tích.

## Bước 27: Đọc và kiểm chứng ba Bronze event family

**Mục tiêu**

Cập nhật script đọc Bronze để đọc riêng commit, identity và account events từ ba
path khác nhau trên MinIO.

**Vì sao cần thực hiện**

Sau khi tách writer thành nhiều output path, cần chứng minh từng path đều đọc lại
được bằng Spark. Đây là bước kiểm chứng rằng layout Bronze mới không chỉ sinh
object trên MinIO mà còn usable cho downstream jobs.

**Kết quả sau khi hoàn thành**

`scripts/read_bronze_parquet.py` đọc được ba path:

```text
s3a://bluesky-lake/bronze/bluesky_commit_events
s3a://bluesky-lake/bronze/bluesky_identity_events
s3a://bluesky-lake/bronze/bluesky_account_events
```

Script in schema, sample rows và count riêng cho từng event family. Với commit
events, script cũng in count theo `collection`.

**Các file liên quan**

- `scripts/read_bronze_parquet.py`
- `scripts/spark_read_kafka_raw.py`
- `src/bluesky_pipeline/spark_session.py`
- `docs/quy-trinh-xay-dung-pipeline.md`

**Kiến thức cần ghi nhớ**

- Mỗi output path mới cần có bước đọc ngược để kiểm chứng dữ liệu có thể dùng
  được.
- Reader kiểm chứng nên phản ánh đúng layout storage hiện tại thay vì đọc một path
  cũ đã bị thay thế.
- Count theo event family và collection là kiểm tra tối thiểu trước khi xây các
  bước Silver/analytics phía sau.

## Bước 28: Profile Bronze commit events để chuẩn bị Silver

**Mục tiêu**

Profile dữ liệu `bronze/bluesky_commit_events` để hiểu phân bố collection,
operation và các field nested quan trọng trước khi thiết kế Silver schema.

**Vì sao cần thực hiện**

Silver là contract dữ liệu sạch hơn Bronze, nên không nên chốt schema chỉ bằng
cảm giác hoặc nhìn vài JSON sample thủ công. Profile bằng Spark giúp xác nhận
field nào xuất hiện theo từng collection và operation, đặc biệt là khác biệt giữa
create/update/delete.

**Kết quả sau khi hoàn thành**

Project có script profile Bronze commit events. Kết quả hiện tại cho thấy dữ liệu
có đủ like, post, repost và follow; create event thường có `cid`, `record_type` và
`record_created_at`, còn delete event chủ yếu chỉ có `rkey`. Post có `text`, một
phần post là reply có `reply_root_uri`; like/repost có `subject_uri`; follow cần
parse riêng vì `record.subject` là string.

**Các file liên quan**

- `scripts/profile_bronze_commit_events.py`
- `scripts/spark_read_kafka_raw.py`
- `src/bluesky_pipeline/spark_session.py`
- `docs/quy-trinh-xay-dung-pipeline.md`

**Kiến thức cần ghi nhớ**

- Bronze profile là bước nối giữa raw storage và Silver design.
- Delete event không nên bị ép vào cùng bảng create/update nếu payload không đủ
  field; nên có bảng deleted records riêng.
- Cùng field `record.subject` có shape khác nhau giữa collection, nên Silver cần
  xử lý theo từng nhóm event.

## Bước 29: Thiết kế Silver schema v1

**Mục tiêu**

Ghi lại thiết kế Silver schema ban đầu cho commit events trước khi viết Spark job
tạo Silver.

**Vì sao cần thực hiện**

Silver schema là contract quan trọng giữa Bronze raw data và các bước analytics
phía sau. Việc ghi docs trước giúp xác định rõ bảng nào xử lý post, engagement,
follow và delete, đồng thời tránh đưa quá nhiều yêu cầu như deduplication,
watermark hoặc Iceberg vào bước đầu.

**Kết quả sau khi hoàn thành**

Repository có `docs/silver-schema-v1.md`, mô tả nguồn dữ liệu Bronze, các quan sát
từ profile, bốn bảng Silver dự kiến và những phần chưa thuộc scope Silver v1.

**Các file liên quan**

- `docs/silver-schema-v1.md`
- `docs/quy-trinh-xay-dung-pipeline.md`
- `scripts/profile_bronze_commit_events.py`

**Kiến thức cần ghi nhớ**

- Silver v1 nên bắt đầu bằng schema đơn giản và chạy được, chưa cần xử lý toàn bộ
  yêu cầu lakehouse nâng cao.
- Tách bảng theo bản chất event giúp downstream dễ query hơn: posts,
  engagements, follows và deleted records.
- Những phần như deduplication, quarantine, pseudonymization và Iceberg nên được
  bổ sung sau khi có job Silver đầu tiên được kiểm chứng.

## Bước 30: Build Silver posts từ Bronze commit events

**Mục tiêu**

Tạo Spark batch job đầu tiên để chuẩn hóa post create/update events từ Bronze
commit events thành bảng `silver_posts`.

**Vì sao cần thực hiện**

Bronze giữ raw JSON để audit và replay, nhưng downstream analytics cần bảng dễ
query hơn. `silver_posts` là lát cắt Silver đầu tiên, giúp kiểm chứng luồng
Bronze -> Silver trên MinIO trước khi triển khai thêm engagement, follow và delete
records.

**Kết quả sau khi hoàn thành**

Project có script `scripts/build_silver_posts.py` đọc
`s3a://bluesky-lake/bronze/bluesky_commit_events`, parse envelope JSON, lọc
`app.bsky.feed.post` với operation `create/update`, tạo các cột Silver v1 như
`post_uri`, `author_did`, `text_length`, `is_reply`, `reply_root_uri` và ghi ra
`s3a://bluesky-lake/silver/silver_posts`. Kết quả chạy hiện tại có
`silver_posts_count: 119`.

**Các file liên quan**

- `scripts/build_silver_posts.py`
- `docs/silver-schema-v1.md`
- `docs/quy-trinh-xay-dung-pipeline.md`

**Kiến thức cần ghi nhớ**

- Silver không thay thế Bronze; Silver là dữ liệu đã chuẩn hóa để query và làm
  nguồn cho analytics.
- Batch build ở bước này dùng `overwrite` để dễ chạy lại trong local learning,
  chưa phải chiến lược incremental/idempotent hoàn chỉnh.
- Chỉ xử lý post create/update trước giúp tạo một vertical slice nhỏ, thay vì cố
  gắng build toàn bộ Silver schema trong một lần.

## Bước 31: Đọc và kiểm chứng Silver posts

**Mục tiêu**

Đọc lại `silver_posts` từ MinIO để xác nhận bảng Silver đầu tiên có schema và dữ
liệu usable.

**Vì sao cần thực hiện**

Ghi Silver thành công chưa đủ; cần đọc lại bằng một Spark job khác để kiểm chứng
dữ liệu downstream có thể sử dụng. Các count theo `operation` và `is_reply` giúp
xác nhận transform cơ bản từ Bronze sang Silver hoạt động đúng.

**Kết quả sau khi hoàn thành**

Project có script `scripts/read_silver_posts.py` đọc
`s3a://bluesky-lake/silver/silver_posts`, in schema, count và một số thống kê cơ
bản. Kết quả hiện tại:

```text
silver_posts_count: 119
create: 118
update: 1
is_reply=false: 67
is_reply=true: 52
```

**Các file liên quan**

- `scripts/read_silver_posts.py`
- `scripts/build_silver_posts.py`
- `docs/quy-trinh-xay-dung-pipeline.md`

**Kiến thức cần ghi nhớ**

- Mọi bảng Silver mới cần có bước đọc ngược để kiểm chứng.
- Count theo field dẫn xuất như `is_reply` giúp kiểm tra transform logic, không
  chỉ kiểm tra file tồn tại.
- Sau khi có một bảng Silver usable, có thể tiếp tục mở rộng sang engagements,
  follows và deleted records theo cùng cách làm.

## Bước 32: Build Silver engagements từ Bronze commit events

**Mục tiêu**

Tạo bảng `silver_engagements` từ like/repost create events trong Bronze commit
events.

**Vì sao cần thực hiện**

Like và repost là nhóm engagement activity quan trọng cho các use case như
engagement volume, engagement velocity và rapid growth. Chuẩn hóa chúng vào một
bảng Silver chung giúp downstream query theo `engagement_type` thay vì phải đọc
raw JSON của từng collection.

**Kết quả sau khi hoàn thành**

Project có script `scripts/build_silver_engagements.py` đọc Bronze commit events,
lọc `app.bsky.feed.like` và `app.bsky.feed.repost` với operation `create`, tạo các
cột Silver như `engagement_uri`, `actor_did`, `engagement_type`, `subject_uri` và
`subject_cid`, rồi ghi ra `s3a://bluesky-lake/silver/silver_engagements`. Kết quả
chạy hiện tại có `silver_engagements_count: 981`.

**Các file liên quan**

- `scripts/build_silver_engagements.py`
- `docs/silver-schema-v1.md`
- `docs/quy-trinh-xay-dung-pipeline.md`

**Kiến thức cần ghi nhớ**

- Like và repost có schema gần nhau vì cùng dùng `record.subject` dạng object gồm
  `uri` và `cid`.
- `engagement_type` giúp gom hai collection vào một bảng Silver nhưng vẫn giữ khả
  năng phân tích riêng like/repost.
- Delete like/repost chưa đưa vào `silver_engagements`; chúng sẽ đi vào bảng
  `silver_deleted_records`.

## Bước 33: Đọc và kiểm chứng Silver engagements

**Mục tiêu**

Đọc lại `silver_engagements` từ MinIO để xác nhận bảng engagement đã chuẩn hóa có
thể dùng cho downstream analytics.

**Vì sao cần thực hiện**

Sau khi ghi Silver engagements, cần kiểm tra không chỉ count tổng mà cả phân bố
`engagement_type` và tính đầy đủ của `subject_uri`, vì đây là target post phục vụ
các bài toán engagement analytics.

**Kết quả sau khi hoàn thành**

Project có script `scripts/read_silver_engagements.py` đọc
`s3a://bluesky-lake/silver/silver_engagements` và in schema, count cùng thống kê
cơ bản. Kết quả hiện tại:

```text
silver_engagements_count: 981
like: 837
repost: 144
has_subject_uri=true: 981
```

**Các file liên quan**

- `scripts/read_silver_engagements.py`
- `scripts/build_silver_engagements.py`
- `docs/quy-trinh-xay-dung-pipeline.md`

**Kiến thức cần ghi nhớ**

- Với engagement events, `subject_uri` là field quan trọng vì nó trỏ tới post được
  like hoặc repost.
- Count theo `engagement_type` giúp kiểm tra mapping collection sang business
  type có đúng không.
- Kiểm chứng field completeness trước khi làm Gold giúp tránh xây analytics trên
  dữ liệu thiếu target.

## Bước 34: Build Silver follows từ Bronze commit events

**Mục tiêu**

Tạo bảng `silver_follows` từ follow create events trong Bronze commit events.

**Vì sao cần thực hiện**

Follow events đại diện cho network activity. Việc chuẩn hóa follow create thành
Silver giúp project có dữ liệu phục vụ các phân tích như follow volume, network
activity trend và active repositories theo follow activity.

**Kết quả sau khi hoàn thành**

Project có script `scripts/build_silver_follows.py` đọc Bronze commit events, lọc
`app.bsky.graph.follow` với operation `create`, parse `record.subject` dạng string
thành `target_actor_did`, rồi ghi ra `s3a://bluesky-lake/silver/silver_follows`.
Kết quả hiện tại có `silver_follows_count: 67` và toàn bộ record có
`target_actor_did`.

**Các file liên quan**

- `scripts/build_silver_follows.py`
- `docs/silver-schema-v1.md`
- `docs/quy-trinh-xay-dung-pipeline.md`

**Kiến thức cần ghi nhớ**

- Follow khác like/repost ở chỗ `record.subject` là string DID, không phải object
  có `uri` và `cid`.
- Cùng tên field trong raw JSON có thể có schema khác nhau theo collection, nên
  Silver job cần parse theo từng event family.
- `target_actor_did` là field chính để phân tích network activity.

## Bước 35: Đọc và kiểm chứng Silver follows

**Mục tiêu**

Đọc lại `silver_follows` từ MinIO để xác nhận bảng follow đã chuẩn hóa có thể dùng
cho downstream analytics.

**Vì sao cần thực hiện**

`silver_follows` chỉ có giá trị phân tích nếu field target của follow được parse
đúng. Kiểm tra `target_actor_did` giúp xác nhận Spark job đã xử lý đúng shape
string của `record.subject` trong follow events.

**Kết quả sau khi hoàn thành**

Project có script `scripts/read_silver_follows.py` đọc
`s3a://bluesky-lake/silver/silver_follows` và in schema, count cùng kiểm tra field
target. Kết quả hiện tại:

```text
silver_follows_count: 67
has_target_actor_did=true: 67
```

**Các file liên quan**

- `scripts/read_silver_follows.py`
- `scripts/build_silver_follows.py`
- `docs/quy-trinh-xay-dung-pipeline.md`

**Kiến thức cần ghi nhớ**

- Kiểm chứng field chính của từng bảng Silver quan trọng hơn chỉ kiểm tra tổng số
  dòng.
- Với network activity, `actor_did` và `target_actor_did` là cặp field cốt lõi.
- Cách build/read/check này có thể lặp lại cho các bảng Silver tiếp theo.

## Bước 36: Build Silver deleted records từ Bronze commit events

**Mục tiêu**

Tạo bảng `silver_deleted_records` từ delete events của các commit collection trong
scope.

**Vì sao cần thực hiện**

Delete events thường không có record payload đầy đủ, nên không phù hợp ghi chung
vào các bảng Silver create/update như posts, engagements hoặc follows. Một bảng
deleted records riêng giúp downstream biết record nào đã bị xóa và có thể xử lý
rebuild, reconciliation hoặc serving layer chính xác hơn.

**Kết quả sau khi hoàn thành**

Project có script `scripts/build_silver_deleted_records.py` đọc Bronze commit
events, lọc operation `delete`, dựng `record_uri` từ `repository_did`, `collection`
và `rkey`, rồi ghi ra `s3a://bluesky-lake/silver/silver_deleted_records`. Kết quả
hiện tại có `silver_deleted_records_count: 29` với phân bố:

```text
app.bsky.feed.post: 6
app.bsky.feed.like: 11
app.bsky.feed.repost: 2
app.bsky.graph.follow: 10
```

**Các file liên quan**

- `scripts/build_silver_deleted_records.py`
- `docs/silver-schema-v1.md`
- `docs/quy-trinh-xay-dung-pipeline.md`

**Kiến thức cần ghi nhớ**

- Delete event là một loại lifecycle event quan trọng, không nên bỏ qua chỉ vì
  thiếu record payload.
- Tách delete sang bảng riêng giúp các bảng create/update giữ schema rõ ràng hơn.
- `record_uri` là key tự nhiên giúp nhận diện record bị xóa trong từng collection.

## Bước 37: Đọc và kiểm chứng Silver deleted records

**Mục tiêu**

Đọc lại `silver_deleted_records` từ MinIO để xác nhận bảng delete lifecycle đã
chuẩn hóa có thể dùng cho downstream processing.

**Vì sao cần thực hiện**

Delete records ảnh hưởng tới tính đúng đắn của serving layer và rebuild sau này.
Kiểm tra `record_uri` giúp xác nhận mỗi delete event có định danh record bị xóa,
không chỉ có count tổng.

**Kết quả sau khi hoàn thành**

Project có script `scripts/read_silver_deleted_records.py` đọc
`s3a://bluesky-lake/silver/silver_deleted_records`, in schema, count theo
collection và kiểm tra `record_uri`. Kết quả hiện tại:

```text
silver_deleted_records_count: 29
app.bsky.feed.like: 11
app.bsky.feed.post: 6
app.bsky.feed.repost: 2
app.bsky.graph.follow: 10
has_record_uri=true: 29
```

**Các file liên quan**

- `scripts/read_silver_deleted_records.py`
- `scripts/build_silver_deleted_records.py`
- `docs/quy-trinh-xay-dung-pipeline.md`

**Kiến thức cần ghi nhớ**

- Delete records cần được kiểm chứng riêng vì chúng có payload nghèo hơn create
  events.
- `record_uri` là field quan trọng để downstream biết record nào cần loại bỏ hoặc
  đánh dấu deleted.
- Kiểm chứng delete lifecycle sớm giúp tránh xây dashboard chỉ dựa trên create
  events và bỏ qua thay đổi trạng thái dữ liệu.

## Bước 38: Kiểm tra tổng quan Silver v1

**Mục tiêu**

Tạo một script kiểm tra tổng quan toàn bộ các bảng Silver v1 đã build trên MinIO.

**Vì sao cần thực hiện**

Sau khi có nhiều bảng Silver riêng lẻ, cần một bước kiểm tra chung để xác nhận tất
cả bảng đều tồn tại, đọc được và có count khớp với các bước build trước đó. Đây là
mốc xác nhận Silver v1 đã hình thành một lớp dữ liệu usable trước khi chuyển sang
Gold analytics hoặc data quality.

**Kết quả sau khi hoàn thành**

Project có script `scripts/check_silver_v1.py` đọc bốn bảng Silver:

```text
silver_posts
silver_engagements
silver_follows
silver_deleted_records
```

Kết quả kiểm tra hiện tại:

```text
silver_posts_count: 119
silver_engagements_count: 981
silver_follows_count: 67
silver_deleted_records_count: 29
```

**Các file liên quan**

- `scripts/check_silver_v1.py`
- `scripts/read_silver_posts.py`
- `scripts/read_silver_engagements.py`
- `scripts/read_silver_follows.py`
- `scripts/read_silver_deleted_records.py`
- `docs/quy-trinh-xay-dung-pipeline.md`

**Kiến thức cần ghi nhớ**

- Khi số lượng bảng tăng lên, cần có script kiểm tra lớp dữ liệu ở cấp layer thay
  vì chỉ kiểm tra từng bảng rời rạc.
- Count tổng không thay thế data quality đầy đủ, nhưng là checkpoint tối thiểu để
  xác nhận Silver layer có dữ liệu usable.
- Silver v1 hiện vẫn là Parquet trên MinIO, chưa phải Iceberg và chưa có
  deduplication/quarantine/pseudonymization.

## Bước 39: Build Gold event volume prototype từ Silver v1

**Mục tiêu**

Tạo aggregate Gold prototype đầu tiên từ các bảng Silver v1 để đếm event volume
theo loại event business.

**Vì sao cần thực hiện**

Sau khi có Silver v1, project cần chứng minh dữ liệu đã chuẩn hóa có thể tạo ra
aggregate phục vụ analytics. Bước này nối Silver sang Gold ở mức prototype, trước
khi đưa Gold serving marts vào ClickHouse theo kiến trúc chính thức.

**Kết quả sau khi hoàn thành**

Project có script `scripts/build_gold_event_volume.py` đọc các bảng Silver
`silver_posts`, `silver_engagements`, `silver_follows` và
`silver_deleted_records`, tạo aggregate theo `event_type`, rồi ghi ra
`s3a://bluesky-lake/gold/gold_event_volume_by_type`.

**Các file liên quan**

- `scripts/build_gold_event_volume.py`
- `scripts/check_silver_v1.py`
- `docs/quy-trinh-xay-dung-pipeline.md`

**Kiến thức cần ghi nhớ**

- Đây là Gold prototype trên MinIO, chưa phải Gold serving layer chính thức bằng
  ClickHouse.
- Gold aggregate nên được rebuild từ Silver, không phụ thuộc trực tiếp vào raw
  Bronze.
- Bắt đầu bằng aggregate đơn giản giúp kiểm chứng luồng Silver -> Gold trước khi
  thêm ClickHouse và dashboard.

## Bước 40: Đọc và kiểm chứng Gold event volume prototype

**Mục tiêu**

Đọc lại Gold event volume prototype từ MinIO để xác nhận aggregate đã ghi có thể
được query bằng Spark.

**Vì sao cần thực hiện**

Tương tự Bronze và Silver, ghi Gold thành công chưa đủ; cần đọc lại để xác nhận
schema và giá trị aggregate. Bước này giúp kiểm tra các count tổng theo event type
trước khi chuyển Gold sang ClickHouse.

**Kết quả sau khi hoàn thành**

Project có script `scripts/read_gold_event_volume.py` đọc
`s3a://bluesky-lake/gold/gold_event_volume_by_type` và in aggregate:

```text
deleted_record: 29
follow: 67
like: 837
post: 119
repost: 144
```

**Các file liên quan**

- `scripts/read_gold_event_volume.py`
- `scripts/build_gold_event_volume.py`
- `docs/quy-trinh-xay-dung-pipeline.md`

**Kiến thức cần ghi nhớ**

- Gold prototype là bước kiểm chứng logic aggregate, không thay thế ClickHouse.
- Count trong Gold phải giải thích được từ các bảng Silver nguồn.
- Khi thêm ClickHouse, dữ liệu Gold serving marts phải có khả năng rebuild từ
  Silver hoặc Gold prototype tương ứng.

## Bước 41: Dựng ClickHouse local cho Gold serving layer

**Mục tiêu**

Bổ sung ClickHouse vào Docker Compose để bắt đầu triển khai Gold serving layer
theo kiến trúc chính thức.

**Vì sao cần thực hiện**

Gold prototype trên MinIO chỉ kiểm chứng logic aggregate. Theo kiến trúc dự án,
Gold serving marts cần nằm trong ClickHouse để phục vụ truy vấn phân tích độ trễ
thấp và dashboard sau này.

**Kết quả sau khi hoàn thành**

Docker Compose có service `clickhouse` dùng image version cụ thể
`clickhouse/clickhouse-server:24.8`, expose HTTP port `8123` và native port host
`9002`. ClickHouse chạy thành công với credential local `default/clickhouse`, và
HTTP query `SELECT 1` trả về `1`.

**Các file liên quan**

- `docker-compose.yml`
- `docs/quy-trinh-xay-dung-pipeline.md`

**Kiến thức cần ghi nhớ**

- ClickHouse là serving database cho Gold marts, không phải source of truth duy
  nhất của pipeline.
- Dữ liệu trong ClickHouse phải có thể rebuild từ Silver hoặc Gold prototype.
- Port native của ClickHouse được map ra `9002` trên host để tránh trùng với MinIO
  đang dùng port `9000`.

## Bước 42: Tạo Gold serving table trong ClickHouse

**Mục tiêu**

Tạo database và table đầu tiên trong ClickHouse để chứa Gold event volume serving
mart.

**Vì sao cần thực hiện**

ClickHouse cần schema table rõ ràng trước khi load dữ liệu aggregate. Bảng Gold
serving này là điểm bắt đầu để chuyển từ Gold prototype trên MinIO sang serving
layer có thể query nhanh và dùng cho dashboard.

**Kết quả sau khi hoàn thành**

ClickHouse có database `bluesky` và table
`bluesky.gold_event_volume_by_type` với các cột `event_type`, `event_count` và
`loaded_at`. Lệnh `SHOW TABLES FROM bluesky` trả về
`gold_event_volume_by_type`.

**Các file liên quan**

- `docker-compose.yml`
- `docs/quy-trinh-xay-dung-pipeline.md`

**Kiến thức cần ghi nhớ**

- Với ClickHouse HTTP interface, các query ghi như `CREATE DATABASE` hoặc
  `CREATE TABLE` cần gửi bằng method POST, không dùng GET readonly.
- `MergeTree` là engine cơ bản cho bảng lưu dữ liệu phân tích trong ClickHouse.
- Bảng Gold serving phải có thể nạp lại từ dữ liệu đã build ở Silver/Gold
  prototype.

## Bước 43: Load Gold event volume vào ClickHouse

**Mục tiêu**

Nạp dữ liệu aggregate `gold_event_volume_by_type` vào bảng Gold serving trong
ClickHouse và query kiểm chứng kết quả.

**Vì sao cần thực hiện**

Gold prototype trên MinIO chỉ chứng minh logic aggregate. Để đi đúng kiến trúc
serving, dữ liệu aggregate cần được đưa vào ClickHouse để phục vụ truy vấn nhanh
và dashboard sau này.

**Kết quả sau khi hoàn thành**

Dữ liệu event volume được insert vào
`bluesky.gold_event_volume_by_type`. Query từ ClickHouse trả về:

```text
deleted_record  29
follow          67
like            837
post            119
repost          144
```

**Các file liên quan**

- `docker-compose.yml`
- `scripts/build_gold_event_volume.py`
- `scripts/read_gold_event_volume.py`
- `docs/quy-trinh-xay-dung-pipeline.md`

**Kiến thức cần ghi nhớ**

- ClickHouse Gold serving mart là nơi phục vụ query/dashboard, không phải source
  of truth duy nhất.
- Dữ liệu ClickHouse phải có thể truncate và load lại từ Silver/Gold prototype.
- CSV thủ công chỉ là bước kiểm chứng ban đầu; bước tiếp theo nên tự động hóa load
  từ dữ liệu đã build.

## Bước 44: Tự động load Gold event volume vào ClickHouse

**Mục tiêu**

Tạo script tự động đọc Gold event volume prototype từ MinIO và load vào ClickHouse
serving table.

**Vì sao cần thực hiện**

Load thủ công bằng CSV chỉ phù hợp để kiểm chứng kết nối ban đầu. Pipeline cần một
bước có thể chạy lại để rebuild ClickHouse Gold serving mart từ dữ liệu đã build,
đúng nguyên tắc ClickHouse không phải source of truth duy nhất.

**Kết quả sau khi hoàn thành**

Project có script `scripts/load_gold_event_volume_to_clickhouse.py` đọc
`s3a://bluesky-lake/gold/gold_event_volume_by_type`, truncate bảng
`bluesky.gold_event_volume_by_type`, insert dữ liệu qua ClickHouse HTTP API và
query kiểm chứng kết quả:

```text
deleted_record  29
follow          67
like            837
post            119
repost          144
```

**Các file liên quan**

- `scripts/load_gold_event_volume_to_clickhouse.py`
- `scripts/build_gold_event_volume.py`
- `docker-compose.yml`
- `docs/quy-trinh-xay-dung-pipeline.md`

**Kiến thức cần ghi nhớ**

- Rebuild ClickHouse bằng truncate + insert từ Gold prototype là cách đơn giản cho
  local learning; production cần chiến lược idempotent và kiểm soát lỗi tốt hơn.
- Khi dùng `urllib`, không nên nhúng `user:password` trực tiếp vào URL nếu parser
  xử lý sai host; dùng HTTP Basic Auth header rõ ràng hơn.
- Script load tự động là bước đầu của workflow sau này có thể đưa vào Airflow.

## Bước 45: Kiểm tra Gold serving table trong ClickHouse

**Mục tiêu**

Tạo script riêng để query bảng Gold serving trong ClickHouse và xác nhận dữ liệu
đã load đúng.

**Vì sao cần thực hiện**

Load và verify nên được tách rõ. Script kiểm tra riêng giúp xác nhận serving table
đang có dữ liệu đúng mà không vô tình reload lại bảng. Đây cũng là bước nền cho
dashboard hoặc health check sau này.

**Kết quả sau khi hoàn thành**

Project có script `scripts/check_clickhouse_gold_event_volume.py` query
`bluesky.gold_event_volume_by_type` và in kết quả:

```text
deleted_record  29
follow          67
like            837
post            119
repost          144
```

**Các file liên quan**

- `scripts/check_clickhouse_gold_event_volume.py`
- `scripts/load_gold_event_volume_to_clickhouse.py`
- `docker-compose.yml`
- `docs/quy-trinh-xay-dung-pipeline.md`

**Kiến thức cần ghi nhớ**

- Tách load và check giúp debug dễ hơn: một script thay đổi state, một script chỉ
  đọc state.
- ClickHouse serving table là đích phục vụ truy vấn nhanh, còn dữ liệu có thể
  rebuild từ Silver/Gold prototype.
- Một kiểm tra nhỏ qua HTTP query là bước đầu trước khi có dashboard Grafana.

## Bước 46: Tách ClickHouse HTTP helper dùng chung

**Mục tiêu**

Tách logic gọi ClickHouse HTTP API sang helper dùng chung trong package
`bluesky_pipeline`.

**Vì sao cần thực hiện**

Sau khi có nhiều script cần query hoặc load ClickHouse, việc lặp URL, Basic Auth
và HTTP request ở từng script dễ gây sai lệch cấu hình. Helper dùng chung giúp các
script ClickHouse sử dụng cùng một cách kết nối.

**Kết quả sau khi hoàn thành**

Project có module `src/bluesky_pipeline/clickhouse_client.py` chứa
`execute_clickhouse()`. Hai script `scripts/check_clickhouse_gold_event_volume.py`
và `scripts/load_gold_event_volume_to_clickhouse.py` dùng helper này và đã chạy
thành công.

**Các file liên quan**

- `src/bluesky_pipeline/clickhouse_client.py`
- `scripts/check_clickhouse_gold_event_volume.py`
- `scripts/load_gold_event_volume_to_clickhouse.py`
- `docs/quy-trinh-xay-dung-pipeline.md`

**Kiến thức cần ghi nhớ**

- Khi một logic kết nối được dùng ở nhiều script, nên tách helper để giảm lặp và
  giảm rủi ro cấu hình lệch nhau.
- Config kết nối ClickHouse nên lấy từ environment variables với default local rõ
  ràng.
- Refactor helper cần được kiểm chứng bằng cả script đọc và script ghi/load.
