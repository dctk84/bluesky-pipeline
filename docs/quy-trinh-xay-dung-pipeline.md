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
