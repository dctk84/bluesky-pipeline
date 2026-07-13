# Edge cases và bài học phỏng vấn

Tài liệu này ghi lại các edge case, khám phá dữ liệu, lỗi đáng học và quyết định
kỹ thuật quan trọng trong quá trình xây dựng project `bluesky-pipeline`.

Mục tiêu của tài liệu không phải ghi nhật ký từng command. Mục tiêu là giúp người
học ôn lại những tình huống có giá trị khi phỏng vấn Data Engineer: vì sao lỗi
xảy ra, cách trace nguyên nhân, trade-off đã chọn và bài học có thể áp dụng lại
ở project khác.

## Khi nào cần cập nhật tài liệu này

Cập nhật tài liệu này khi gặp một tình huống có giá trị học tập lâu dài, ví dụ:

- Dữ liệu nguồn có schema hoặc behavior khác giả định ban đầu.
- Một metric/dashboard gây hiểu nhầm và cần phân biệt rõ event time, ingestion
  time, processing time hoặc load time.
- Một lỗi làm lộ ra vấn đề về schema evolution, data contract, idempotency,
  checkpoint, replay, duplicate hoặc late data.
- Một quyết định kỹ thuật có trade-off rõ ràng giữa đơn giản, đúng kiến trúc,
  latency, khả năng rebuild hoặc khả năng quan sát.
- Một giới hạn local khác với production và cần giải thích trung thực khi phỏng
  vấn.

Không cần cập nhật tài liệu này cho lỗi gõ sai command, typo nhỏ, lỗi import đơn
giản hoặc thao tác debug không tạo ra bài học lâu dài.

## Cấu trúc đề xuất cho mỗi case

Mỗi case nên ghi ngắn gọn theo cấu trúc:

```text
### Case N: Tên case

**Hiện tượng**

Mô tả điều quan sát được.

**Cách phát hiện**

Query, checkpoint, log hoặc dashboard nào giúp phát hiện.

**Nguyên nhân**

Giải thích nguyên nhân thật, phân biệt với triệu chứng.

**Cách xử lý hoặc quyết định**

Nêu hướng xử lý đã chọn, hoặc lý do giữ nguyên nếu đây là behavior hợp lệ.

**Bài học phỏng vấn**

Tóm tắt điều cần nhớ để giải thích với interviewer.

**File hoặc bảng liên quan**

Liệt kê đường dẫn file, bảng hoặc script liên quan.
```

## Case 1: Delete event thường thiếu `record`

**Hiện tượng**

Khi probe Bluesky Jetstream, các event `delete` thường không có `record` đầy đủ.
Trong sample ban đầu, các event delete khớp với nhóm `record_type = missing`.

**Cách phát hiện**

Project dùng script discovery để phân tích sample Jetstream và ghi lại trong:

- `scripts/discovery/analyze_sample.py`
- `docs/jetstream-schema-notes.md`

**Nguyên nhân**

Delete event chỉ cần thông tin định danh record bị xóa như repository DID,
collection và rkey. Nguồn không đảm bảo gửi lại toàn bộ record body khi record đã
bị xóa.

**Cách xử lý hoặc quyết định**

Silver v1 tách riêng bảng `silver_deleted_records` thay vì ép delete event vào
`silver_posts`, `silver_engagements` hoặc `silver_follows`.

**Bài học phỏng vấn**

Streaming pipeline không nên giả định mọi event cùng collection đều có cùng
payload shape. Delete/update/create có thể cần schema và logic riêng. Giữ delete
event là cần thiết để downstream hiểu lifecycle của dữ liệu.

**File hoặc bảng liên quan**

- `src/bluesky_pipeline/silver_transformations.py`
- `docs/silver-schema-v1.md`
- `lakehouse.silver_v1.silver_deleted_records`

## Case 2: Cùng field `subject` nhưng khác shape theo collection

**Hiện tượng**

Field `record.subject` có shape khác nhau:

- Like/repost: object có `uri` và `cid`.
- Follow: string DID của actor được follow.
- Post: thường không có `subject`.

**Cách phát hiện**

Schema profiling trên sample Jetstream cho thấy `record.subject` không thể parse
bằng một schema phẳng duy nhất cho mọi collection.

**Nguyên nhân**

Bluesky dùng cùng tên field trong nhiều record type, nhưng semantics khác nhau
theo collection.

**Cách xử lý hoặc quyết định**

Spark schema được tách theo record family:

- `ENGAGEMENT_RECORD_SCHEMA` cho like/repost.
- `FOLLOW_RECORD_SCHEMA` cho follow.
- `POST_RECORD_SCHEMA` cho post.

Silver cũng tách `silver_engagements` và `silver_follows`.

**Bài học phỏng vấn**

Schema-on-read không có nghĩa là parse tùy tiện. Với semi-structured event, cần
profile dữ liệu thật và normalize theo domain semantics, không chỉ theo tên field.

**File hoặc bảng liên quan**

- `src/bluesky_pipeline/bronze_schemas.py`
- `src/bluesky_pipeline/silver_transformations.py`
- `docs/jetstream-schema-notes.md`

## Case 3: Non-commit events không nên ép vào layout commit

**Hiện tượng**

Jetstream có các event `identity` và `account` không có `commit.collection`,
`commit.operation` hoặc `record`.

**Cách phát hiện**

Khi đọc Kafka/Bronze, project quan sát thấy event có `kind` ở cấp event nhưng
không có `commit`. Nếu ghi chung theo partition `collection`, các event này dễ
rơi vào partition null hoặc layout khó hiểu.

**Nguyên nhân**

Không phải mọi event từ Jetstream đều là repository commit. Identity/account là
nhóm event khác, phục vụ account lifecycle.

**Cách xử lý hoặc quyết định**

Event envelope bổ sung `event_kind`. Bronze writer tách output theo event family:

- `bronze/bluesky_commit_events`
- `bronze/bluesky_identity_events`
- `bronze/bluesky_account_events`

Silver v1 hiện chỉ xử lý commit events; account lifecycle để ngoài scope hiện
tại.

**Bài học phỏng vấn**

Raw lake layout phải phản ánh event family. Không nên dùng một partition strategy
cho các event không cùng schema chỉ vì chúng đến từ cùng WebSocket.

**File hoặc bảng liên quan**

- `src/bluesky_pipeline/event_envelope.py`
- `scripts/ingestion/spark_read_kafka_raw.py`
- `src/bluesky_pipeline/bronze_tables.py`

## Case 4: Window năm 2025 trong dashboard năm 2026

**Hiện tượng**

Panel `gold_content_quality_hourly` trong Grafana xuất hiện một `window_start`
ở `2025-10-06`, dù dữ liệu đang được load lại vào ClickHouse ngày `2026-07-11`.
Row này có:

```text
reply_create_count = 1
total_content_events = 1
reply_ratio = 1
avg_text_length = 299
```

**Cách phát hiện**

Query ClickHouse trong Grafana:

```sql
SELECT
    formatDateTime(window_start, '%F %T', 'UTC') AS window_start_utc,
    formatDateTime(window_start, '%F %T', 'Asia/Bangkok') AS window_start_bangkok,
    total_content_events,
    reply_create_count,
    reply_ratio,
    avg_text_length,
    loaded_at
FROM bluesky.gold_content_quality_hourly
WHERE window_start < toDateTime('2026-01-01 00:00:00', 'UTC')
ORDER BY window_start;
```

Trace tiếp bằng Trino trên Gold modeled:

```sql
SELECT
    content_event_id,
    post_uri,
    author_did,
    content_event_type,
    event_time,
    received_at,
    from_unixtime(jetstream_time_us / 1000000.0) AS jetstream_time,
    jetstream_time_us,
    ingest_date,
    ingest_hour,
    is_reply,
    reply_root_uri,
    reply_parent_uri
FROM lakehouse.gold_v1.gold_fact_content_events
WHERE event_time >= TIMESTAMP '2025-10-06 00:00:00'
  AND event_time < TIMESTAMP '2025-10-07 00:00:00'
ORDER BY event_time
LIMIT 20;
```

Kết quả cho thấy:

```text
content_event_type = reply_create
event_time         = 2025-10-06 00:08:42 +0700
received_at        = 2026-07-08T16:24:56.757260Z
jetstream_time     = 2026-07-08 23:24:57.259 +0700
ingest_date        = 2026-07-08
```

**Nguyên nhân**

Gold modeled tạo `event_time` bằng hàm `_event_time()`, ưu tiên
`record_created_at` trước `received_at`. Vì vậy nếu Bluesky emit hoặc pipeline
observe một record có `record.createdAt` cũ, metric sẽ được group về source event
time cũ, dù ingestion/load xảy ra năm 2026.

Đây không phải lỗi Grafana và không phải dữ liệu local cũ. Đây là khác biệt giữa:

- Event time: thời điểm record được tạo tại nguồn.
- Ingestion time: thời điểm gateway nhận event.
- Jetstream time: thời điểm Jetstream emit event.
- Load time: thời điểm mart được load vào ClickHouse.

**Cách xử lý hoặc quyết định**

Không xóa row 2025 một cách mù quáng. Dashboard chính nên dùng time filter
`$__timeFilter(window_start)` để người xem tập trung vào khoảng thời gian đang
chọn. Đồng thời nên giữ một data quality panel như `DQ - Old Source Event
Windows` để theo dõi các window cũ bất thường.

Nếu use case là operational freshness, dashboard nên dùng `loaded_at`,
`received_at`, `jetstream_time` hoặc metric freshness riêng thay vì chỉ nhìn
`event_time`.

**Bài học phỏng vấn**

Event-time analytics có thể tạo window nằm xa thời điểm ingest. Đây là hành vi
hợp lệ nếu business question hỏi "nội dung được tạo khi nào". Nhưng nếu câu hỏi
là "pipeline vừa ingest gì" hoặc "dashboard có fresh không", phải dùng ingestion
time/load time. Đây là ví dụ tốt để giải thích event time, ingestion time và data
freshness.

**File hoặc bảng liên quan**

- `src/bluesky_pipeline/gold_transformations.py`
- `src/bluesky_pipeline/gold_analytics_transformations.py`
- `lakehouse.gold_v1.gold_fact_content_events`
- `bluesky.gold_content_quality_hourly`

## Case 16: Cùng tên `actor_did` nhưng khác vai trò nghiệp vụ sau join

**Hiện tượng**

Khi chạy `refresh_gold_actor_activity_daily_incremental.py --ignore-state`,
script fail ở bước build affected keys cho actor daily:

```text
pyspark.errors.exceptions.captured.AnalysisException:
[AMBIGUOUS_REFERENCE] Reference `actor_did` is ambiguous
```

Lỗi xuất hiện sau khi join engagement fact với post author để tính phần actor
nhận engagement.

**Cách phát hiện**

Traceback chỉ tới hàm `build_received_engagement_keys()`. Ở đó script join:

```text
gold_fact_engagement_events.target_post_uri
    -> gold_dim_posts.post_uri
```

Sau join, DataFrame có hai cột cùng tên `actor_did`:

- `actor_did` từ engagement fact: actor đi like/repost.
- `actor_did` từ post dimension: author của post nhận engagement.

Spark không thể biết `select(col("actor_did"))` đang chọn cột nào.

**Nguyên nhân**

Đây không chỉ là lỗi kỹ thuật do trùng tên cột. Nguyên nhân thật là cùng một tên
`actor_did` đang đại diện cho hai vai trò nghiệp vụ khác nhau trong cùng một
metric:

- **engagement actor**: người thực hiện tương tác.
- **received actor**: người nhận tương tác vì họ là author của target post.

Với `gold_actor_activity_daily`, cả hai vai trò đều cần được tính nhưng ở các
metric khác nhau.

**Cách xử lý hoặc quyết định**

Trong incremental script, cột author của post được alias thành
`received_actor_did` trước khi join/select:

```text
author_did AS received_actor_did
```

Sau khi đã xác định đúng vai trò nhận engagement, script mới alias lại thành
`actor_did` cho grain cuối cùng `activity_date + actor_did` của mart.

**Bài học phỏng vấn**

Khi join fact và dimension trong analytics mart, không nên chỉ nhìn tên cột mà
phải nhìn semantics của cột trong từng ngữ cảnh. Một event có thể ảnh hưởng nhiều
entity khác nhau. Việc alias sớm theo vai trò nghiệp vụ giúp tránh ambiguous
columns và giúp metric dễ giải thích hơn.

**File hoặc bảng liên quan**

- `scripts/gold/refresh_gold_actor_activity_daily_incremental.py`
- `src/bluesky_pipeline/gold_analytics_transformations.py`
- `lakehouse.gold_v1.gold_fact_engagement_events`
- `lakehouse.gold_v1.gold_dim_posts`
- `bluesky.gold_actor_activity_daily`

## Case 5: ClickHouse và Trino không dùng cùng namespace/table path

**Hiện tượng**

Khi query `lakehouse.gold_v1.gold_fact_content_events` trong Grafana ClickHouse
datasource, ClickHouse báo syntax error. Khi query `bluesky.gold_content_quality_hourly`
trong Trino, Trino báo schema `bluesky` không tồn tại.

**Cách phát hiện**

Trong Grafana, datasource là `grafana-clickhouse-datasource`, nên SQL được gửi
tới ClickHouse. Trong DBeaver, connection Trino chỉ nhìn thấy catalog Iceberg
`lakehouse`, không tự nhìn thấy database ClickHouse `bluesky`.

**Nguyên nhân**

Project hiện tách rõ hai serving/query layer:

- Trino query Iceberg lakehouse tables, ví dụ `lakehouse.gold_v1.*`.
- ClickHouse query serving marts, ví dụ `bluesky.gold_*`.

Chưa cấu hình Trino ClickHouse connector, nên hai hệ thống không tự federation.

**Cách xử lý hoặc quyết định**

Chạy query trace Silver/Gold modeled trong DBeaver/Trino. Chạy query dashboard
serving mart trong Grafana/ClickHouse. Không trộn namespace giữa hai engine.

**Bài học phỏng vấn**

Lakehouse query engine và serving database có vai trò khác nhau. Tên bảng giống
business domain không có nghĩa chúng nằm trong cùng catalog. Khi debug pipeline
nhiều tầng, phải biết đang query engine nào và source of truth của tầng đó là gì.

**File hoặc bảng liên quan**

- `config/trino/catalog/lakehouse.properties`
- `docker-compose.yml`
- `lakehouse.gold_v1.gold_fact_content_events`
- `bluesky.gold_content_quality_hourly`

## Case 6: Spark streaming ghi ClickHouse hiện là at-least-once

**Hiện tượng**

Realtime fast path dùng Spark Structured Streaming `foreachBatch` để aggregate
micro-batch rồi insert vào ClickHouse. Nếu Spark retry một micro-batch, ClickHouse
có thể nhận lại cùng batch.

**Cách phát hiện**

Thiết kế realtime marts có `spark_batch_id` trong các bảng:

- `gold_event_volume_1m_stream`
- `gold_content_activity_1m_stream`
- `gold_engagement_1m_stream`
- `gold_network_activity_1m_stream`
- `gold_realtime_stream_batches`

Tài liệu tổng quan cũng ghi rõ chưa tuyên bố exactly-once end-to-end.

**Nguyên nhân**

Spark checkpoint giúp quản lý progress của streaming query, nhưng sink HTTP tự
viết vào ClickHouse không tự đảm bảo idempotency toàn cục. `foreachBatch` mặc
định nên được xem là at-least-once ở sink nếu không có khóa logic hoặc cơ chế
deduplicate rõ ràng.

**Cách xử lý hoặc quyết định**

Project giữ mô tả trung thực: at-least-once, có reconciliation/checkpoint và có
`spark_batch_id` để debug. Nếu cần tiến gần hơn tới idempotent write, bảng metric
cần khóa logic như:

```text
window_start + metric_name + dimension
```

hoặc cơ chế replace/deduplicate khi retry.

**Bài học phỏng vấn**

Không nên tuyên bố exactly-once chỉ vì Spark có checkpoint. Delivery semantics
phải xét toàn bộ đường đi: source, processing, sink, retry và cách sink xử lý
duplicate.

**File hoặc bảng liên quan**

- `scripts/realtime/stream_metrics_to_clickhouse.py`
- `src/bluesky_pipeline/gold_tables.py`
- `docs/tong-quan-du-an.md`

## Case 7: Local Spark hiện chưa phải Spark Standalone multi-worker

**Hiện tượng**

Tài liệu mục tiêu yêu cầu Spark Structured Streaming chạy ở chế độ multi-worker
Spark Standalone, nhưng helper hiện tại tạo SparkSession bằng:

```text
.master("local[*]")
```

**Cách phát hiện**

Đọc `src/bluesky_pipeline/spark_session.py` và đối chiếu với mục tiêu dài hạn
trong `docs/tong-quan-du-an.md`.

**Nguyên nhân**

Project đang đi theo vertical slice: ưu tiên dựng luồng dữ liệu chạy được local
trước, rồi mới nâng dần lên distributed Spark cluster và failure experiments.

**Cách xử lý hoặc quyết định**

Khi trình bày hiện trạng, cần nói rõ đây là local learning environment, chưa phải
production-scale hoặc HA. Bước Spark Standalone multi-worker thuộc phần mở rộng
để chứng minh distributed execution/failure testing.

**Bài học phỏng vấn**

Portfolio project nên trung thực về phạm vi đã chứng minh. `local[*]` giúp học
Spark API và hoàn thiện data flow, nhưng chưa chứng minh scheduling qua nhiều
worker, executor failure hoặc cluster resource management.

**File hoặc bảng liên quan**

- `src/bluesky_pipeline/spark_session.py`
- `docs/tong-quan-du-an.md`

## Case 8: Bronze Parquet schema mismatch cột `partition`

**Hiện tượng**

Log live pipeline từng ghi lỗi:

```text
Parquet column cannot be converted.
Column: [partition], Expected: bigint, Found: INT32.
```

**Cách phát hiện**

Đọc log local:

- `logs/live_pipeline/silver-stream.log`

**Nguyên nhân khả dĩ**

Bronze Parquet có file được ghi với physical type khác nhau cho cột Kafka
`partition`. Một số Spark schema khai báo `partition` là `IntegerType`, trong khi
luồng đọc hoặc schema inference có thể kỳ vọng kiểu khác. Đây là dạng lỗi data
contract/schema evolution ở tầng file.

**Cách xử lý hoặc quyết định**

Chưa ghi nhận đây là lỗi hiện tại nếu `check_lakehouse_path.py` đã pass. Tuy
nhiên, nếu lỗi xuất hiện lại khi chạy live Bronze -> Silver streaming, cần kiểm
tra schema Bronze hiện có, cleanup/rebuild dữ liệu local nếu phù hợp và chuẩn hóa
schema cột `partition` nhất quán giữa writer và reader.

**Bài học phỏng vấn**

Parquet lưu schema ở file level. Khi cùng một path có file được ghi bởi các
version schema khác nhau, Spark có thể fail khi đọc chung. Schema contract ở
Bronze cũng cần ổn định, dù Bronze là raw layer.

**File hoặc bảng liên quan**

- `src/bluesky_pipeline/bronze_schemas.py`
- `scripts/ingestion/spark_read_kafka_raw.py`
- `scripts/lakehouse/stream_silver_from_bronze.py`
- `logs/live_pipeline/silver-stream.log`

## Case 9: ClickHouse `ILLEGAL_AGGREGATION` khi alias trùng tên cột gốc

**Hiện tượng**

Khi dựng panel Grafana `Creator vs Engager Mix - Top Actors`, ClickHouse báo lỗi:

```text
Aggregate function sum(content_events_created) AS content_events_created
is found inside another aggregate function in query.
ILLEGAL_AGGREGATION
```

Lỗi vẫn xuất hiện ngay cả khi query đã bọc bằng subquery nếu alias aggregate ở
tầng trong hoặc tầng ngoài trùng với tên cột gốc, ví dụ:

```sql
sum(content_events_created) AS content_events_created
```

**Cách phát hiện**

Lỗi xuất hiện trực tiếp trong Grafana panel editor khi chạy query cho bảng:

```text
bluesky.gold_actor_activity_daily
```

**Nguyên nhân**

ClickHouse có cơ chế resolve alias khá sớm trong query. Khi alias của aggregate
trùng với tên cột gốc, biểu thức ở `ORDER BY` hoặc tầng query ngoài có thể bị
substitute theo cách khiến ClickHouse hiểu thành aggregate lồng nhau, ví dụ
`sum(sum(content_events_created))`.

Đây là lỗi query SQL/dashboard, không phải lỗi dữ liệu trong mart.

**Cách xử lý hoặc quyết định**

Tránh đặt alias aggregate trùng tên với cột gốc. Dùng alias khác rõ nghĩa như
`content_created_total`, `engagements_given_total`, `follows_created_total`.

Query chạy được cho panel:

```sql
SELECT
    actor,
    content_events_created,
    engagements_given,
    follows_created
FROM
(
    SELECT
        left(actor_did, 28) AS actor,
        sum(content_events_created) AS content_events_created,
        sum(engagements_given) AS engagements_given,
        sum(follows_created) AS follows_created
    FROM bluesky.gold_actor_activity_daily
    GROUP BY actor_did
)
ORDER BY
    content_events_created + engagements_given + follows_created DESC
LIMIT 15;
```

Nếu lỗi alias tái xuất hiện trong query phức tạp hơn, pattern an toàn là đặt alias
khác hẳn tên cột gốc, ví dụ `content_created_total`.

**Bài học phỏng vấn**

Dashboard SQL cũng là một phần của data product. Khi build serving dashboard,
cần hiểu đặc thù SQL engine của serving database. Với ClickHouse, alias resolution
có thể khác Trino/PostgreSQL, nên nên viết query aggregate theo style rõ ràng:
subquery trước, alias không trùng cột gốc, rồi sort/filter ở tầng ngoài.

**File hoặc bảng liên quan**

- `bluesky.gold_actor_activity_daily`
- `src/bluesky_pipeline/gold_tables.py`
- `docs/gold-analytics-metrics-v1.md`

## Case 10: ClickHouse `NO_COMMON_TYPE` khi `UNION ALL` trộn `UInt64` và `Int64`

**Hiện tượng**

Khi dựng panel `Daily Network Growth Trend`, ClickHouse báo lỗi:

```text
There is no supertype for types Int64, UInt64 because some of them are signed
integers and some are unsigned integers.
NO_COMMON_TYPE
```

**Cách phát hiện**

Panel dùng `UNION ALL` để gom nhiều metric thành dạng long format:

```text
time | metric | value
```

Trong đó:

- `follow_count` và `unfollow_count` là count nên có kiểu `UInt64`.
- `net_follow_count` có thể âm nên có kiểu `Int64`.

**Nguyên nhân**

Trong `UNION ALL`, các cột cùng vị trí phải có kiểu dữ liệu tương thích. ClickHouse
không tự chọn được một supertype an toàn giữa unsigned integer và signed integer
khi unsigned có thể vượt range signed.

**Cách xử lý hoặc quyết định**

Ép kiểu rõ ràng trong dashboard query, ví dụ:

```sql
SELECT
    toDateTime(activity_date) AS time,
    toInt64(sum(follow_count)) AS follow_count,
    toInt64(sum(unfollow_count)) AS unfollow_count,
    toInt64(sum(net_follow_count)) AS net_follow_count
FROM bluesky.gold_network_growth_daily
GROUP BY activity_date
ORDER BY time ASC;
```

Sau đó chuyển query sang wide format để Grafana hiển thị legend sạch hơn và không
cần `UNION ALL`.

**Bài học phỏng vấn**

Serving marts thường có cả count metrics và net/growth metrics. Count tự nhiên là
unsigned, nhưng net metric có thể âm. Khi đưa nhiều metric vào cùng một cột
`value` hoặc `UNION ALL`, cần cast kiểu rõ ràng. Đây là ví dụ thực tế của data
type contract ở tầng serving/dashboard.

**File hoặc bảng liên quan**

- `bluesky.gold_network_growth_daily`
- `src/bluesky_pipeline/gold_tables.py`
- `scripts/gold/build_network_growth_daily_from_gold_modeled.py`

## Case 11: Follow delete không luôn lookup được `target_actor_did`

**Hiện tượng**

Panel `DQ - Unresolved Follow Delete Targets` trong Grafana ghi nhận:

```text
activity_date = 2026-07-08
null_target_rows = 1
null_target_follow_count = 0
null_target_unfollow_count = 644
null_target_net_follow_count = -644
```

**Cách phát hiện**

Query DQ trên ClickHouse:

```sql
SELECT
    activity_date,
    countIf(target_actor_did IS NULL) AS null_target_rows,
    sumIf(follow_count, target_actor_did IS NULL) AS null_target_follow_count,
    sumIf(unfollow_count, target_actor_did IS NULL) AS null_target_unfollow_count,
    sumIf(net_follow_count, target_actor_did IS NULL) AS null_target_net_follow_count
FROM bluesky.gold_network_growth_daily
GROUP BY activity_date
HAVING null_target_rows > 0
ORDER BY activity_date;
```

**Nguyên nhân**

Follow delete event thường chỉ có record URI/rkey, không có full record body. Để
suy ra `target_actor_did`, Gold modeled phải join delete event với follow create
history đã observe trước đó. Nếu pipeline thấy delete nhưng chưa từng observe
follow create tương ứng trong dữ liệu hiện có, `target_actor_did` sẽ null.

Đây là giới hạn dữ liệu hợp lý của observed stream, không phải lỗi dashboard.

**Cách xử lý hoặc quyết định**

Giữ DQ panel để thể hiện giới hạn này thay vì che giấu. Trong dashboard và khi
trình bày project, gọi metric là **observed network growth**, không phải follower
count toàn cục hoặc social graph đầy đủ của Bluesky.

Nếu cần cải thiện sau này, có thể:

- Giữ thêm mapping lịch sử follow create dài hơn.
- Backfill dữ liệu follow state nếu có nguồn phù hợp.
- Tách riêng metric unresolved deletes để không làm người xem hiểu nhầm net
  growth theo target actor.

**Bài học phỏng vấn**

Delete event trong streaming thường thiếu context. Nếu state trước đó không có
trong window dữ liệu hoặc source of truth, pipeline không thể suy ra đầy đủ
business entity bị ảnh hưởng. Cần ghi nhận unresolved state như data quality
metric thay vì âm thầm drop hoặc gán sai target.

**File hoặc bảng liên quan**

- `bluesky.gold_network_growth_daily`
- `lakehouse.gold_v1.gold_fact_network_events`
- `src/bluesky_pipeline/gold_transformations.py`
- `src/bluesky_pipeline/gold_analytics_transformations.py`

## Case 12: Cleanup Iceberg phải xử lý cả Hive Metastore và data files

**Hiện tượng**

Khi muốn xóa toàn bộ dữ liệu ingest để chạy lại E2E từ trạng thái sạch, nếu chỉ
xóa Iceberg warehouse trên MinIO thì vẫn có rủi ro Hive Metastore còn metadata
table/namespace cũ. Lần build hoặc query sau đó có thể gặp trạng thái catalog
nghĩ rằng bảng còn tồn tại nhưng metadata/data files đã bị xóa.

**Cách phát hiện**

Review `scripts/platform/cleanup_ingested_data.py` cho thấy script ban đầu đã xóa:

- Bronze/Gold/checkpoint paths trên MinIO.
- ClickHouse serving tables bằng `TRUNCATE`.
- Kafka raw topic khi truyền `--include-kafka-topic`.

Tuy nhiên Iceberg catalog dùng Hive Metastore, nên cleanup cần xử lý thêm metadata
catalog chứ không chỉ object storage. Sau khi project có Gold incremental, cleanup
cũng cần xóa local state files trong `data/state/*.json`; nếu giữ marker cũ trong
khi dữ liệu lakehouse đã bị xóa, lần chạy mới có thể bỏ qua dữ liệu cần xử lý.

**Nguyên nhân**

Apache Iceberg tách metadata catalog khỏi data files. Trong project này:

- Data files và Iceberg metadata files nằm trên MinIO.
- Table/namespace registration nằm trong Hive Metastore.

Nếu xóa files trên MinIO nhưng không drop table trong metastore, catalog có thể
giữ pointer tới metadata location đã không còn tồn tại.

**Cách xử lý hoặc quyết định**

Update cleanup script để:

1. In rõ các Iceberg tables và namespaces trong dry-run.
2. Drop các bảng Gold/Silver Iceberg trong Hive Metastore.
3. Drop namespace `lakehouse.gold_v1` và `lakehouse.silver_v1`.
4. Sau đó mới xóa warehouse/checkpoint paths trên MinIO.
5. Chuẩn hóa `s3://` thành `s3a://` khi gọi Hadoop FileSystem để xóa MinIO.
6. Xóa local incremental state trong `data/state/*.json` để lần chạy sạch không
   dùng lại watermark/marker cũ.

Cleanup vẫn giữ nguyên cơ chế an toàn: dry-run mặc định, chỉ xóa thật khi có
`--confirm-delete`, và chỉ purge Kafka topic khi có `--include-kafka-topic`.

**Bài học phỏng vấn**

Với lakehouse table format như Iceberg, cleanup/rebuild không chỉ là xóa folder
object storage. Cần hiểu rõ catalog metadata, table metadata và data files đang
nằm ở đâu. Với incremental pipeline, còn phải hiểu cả control-plane state như
watermark hoặc last successful run marker. Đây là khác biệt quan trọng giữa “file
lake” đơn giản và “table format” có catalog cộng với incremental orchestration.

**File hoặc bảng liên quan**

- `scripts/platform/cleanup_ingested_data.py`
- `src/bluesky_pipeline/iceberg_config.py`
- `lakehouse.silver_v1.*`
- `lakehouse.gold_v1.*`
- Hive Metastore
- MinIO Iceberg warehouse
- `data/state/*.json`

## Case 13: `follow_create` có thể trùng `follow_uri` trong fact event

**Hiện tượng**

Sau khi cleanup dữ liệu và chạy lại live E2E, bước
`scripts.lakehouse.run_lakehouse_path` fail ở checkpoint Trino Gold modeled:

```text
gold_fact_network_events key=network_event_id rows=17455 non_null=17455 distinct=17281 MISMATCH
Trino Gold modeled v1 key check failed
```

Các bảng Gold modeled khác đều pass key check.

**Cách phát hiện**

Query chẩn đoán trên Trino cho thấy duplicate chỉ nằm ở `follow_create`:

```text
follow_create rows=15129 distinct_ids=14955
follow_delete rows=2326 distinct_ids=2326
```

Kiểm tra Silver cũng cho thấy `silver_follows` có nhiều dòng hơn số
`follow_uri` distinct:

```text
rows=15129
distinct_follow_uri=14955
```

Một số `follow_uri` xuất hiện nhiều lần với `jetstream_time_us` khác nhau.

**Nguyên nhân**

`gold_fact_network_events` ban đầu dùng `follow_uri` làm `network_event_id` cho
`follow_create`. Cách này coi follow record URI là key duy nhất của fact event.
Tuy nhiên trong observed stream, cùng một follow record có thể xuất hiện nhiều
event create quan sát được. Khi đó fact table có nhiều dòng nhưng dùng chung một
`network_event_id`, làm checkpoint key uniqueness fail.

Đây là lỗi data contract ở tầng Gold modeled: fact key chưa đại diện đủ cho từng
event quan sát được.

**Cách xử lý hoặc quyết định**

Đổi `network_event_id` của `follow_create` sang deterministic event key gồm:

```text
follow_uri + network_event_type + jetstream_time_us
```

Sau đó build lại lakehouse path. Checkpoint Trino Gold modeled pass và Gold
serving có dữ liệu trở lại trong Grafana.

**Bài học phỏng vấn**

Cần phân biệt **business entity key** và **event fact key**. `follow_uri` phù hợp
để nhận diện follow record, nhưng không luôn đủ để nhận diện từng event trong
fact table. Với streaming/observed data, event id thường cần thêm event type và
source/event timestamp để giữ uniqueness, đồng thời vẫn phải nói rõ đây chưa phải
exactly-once guarantee end-to-end.

**File hoặc bảng liên quan**

- `src/bluesky_pipeline/gold_transformations.py`
- `scripts/lakehouse/check_trino_gold_modeled_v1.py`
- `lakehouse.silver_v1.silver_follows`
- `lakehouse.gold_v1.gold_fact_network_events`

## Case 14: Initial refresh marker năm 0001 làm Spark timestamp filter trả về 0 row

**Hiện tượng**

Khi chạy script `refresh_gold_facts_incremental.py --ignore-state`, refresh window
được tính như initial refresh nhưng tất cả Silver incremental row count đều bằng
0:

```text
silver_posts_incremental_rows: 0
silver_engagements_incremental_rows: 0
silver_follows_incremental_rows: 0
silver_deleted_records_incremental_rows: 0
```

Trong khi các bảng Silver thực tế đã có dữ liệu.

**Cách phát hiện**

Script dùng `--ignore-state`, nên `last_successful_run_at` là `None` và
`build_refresh_window()` trả về `refresh_from = datetime.min` dạng:

```text
0001-01-01T00:00:00+00:00
```

Sau đó script đưa mốc này vào filter Spark:

```text
received_at >= refresh_from
```

**Nguyên nhân**

`datetime.min` là marker logic để biểu diễn initial refresh, nhưng Spark không
parse ổn định timestamp năm 0001 trong biểu thức `to_timestamp`. Khi lower bound
thành `NULL`, điều kiện so sánh timestamp trả về null/false và loại toàn bộ rows.

Đây là lỗi chuyển đổi giữa marker control-plane trong Python và filter
data-plane trong Spark.

**Cách xử lý hoặc quyết định**

Với initial refresh, không đưa `datetime.min` vào filter timestamp vật lý. Script
chỉ áp dụng upper bound:

```text
received_at < refresh_to
```

Khi đã có state thật, script mới áp dụng cả lower bound:

```text
received_at >= refresh_from
received_at < refresh_to
```

Sau khi sửa, `--ignore-state` đọc được dữ liệu Silver:

```text
silver_posts_incremental_rows: 30520
silver_engagements_incremental_rows: 194588
silver_follows_incremental_rows: 15129
silver_deleted_records_incremental_rows: 6406
```

**Bài học phỏng vấn**

Watermark và marker control-plane không nên được dùng mù quáng như giá trị dữ
liệu trong engine xử lý. Với Spark, SQL engine hoặc warehouse, cần đảm bảo các
mốc thời gian nằm trong range parse được và có semantics rõ ràng. Initial load
thường nên là một branch logic riêng thay vì ép thành một timestamp cực nhỏ.

**File hoặc bảng liên quan**

- `scripts/gold/refresh_gold_facts_incremental.py`
- `src/bluesky_pipeline/incremental_refresh.py`
- `lakehouse.silver_v1.silver_posts`
- `lakehouse.silver_v1.silver_engagements`
- `lakehouse.silver_v1.silver_follows`
- `lakehouse.silver_v1.silver_deleted_records`

## Case 15: Incremental mart phải phân biệt `received_at` và `event_time`

**Hiện tượng**

Khi incremental hóa `gold_content_quality_hourly`, không thể chỉ lấy giờ hiện tại
hoặc giờ theo thời điểm job chạy để refresh dashboard. Dữ liệu Bluesky có
`received_at` là thời điểm pipeline nhận/ghi dữ liệu, nhưng metric hourly trên
dashboard lại được group theo `event_time`.

Initial run của script incremental replace 276 hourly windows và reconcile pass:

```text
affected_rows_replaced: 276
row_count expected=276 actual=276 OK
total_content_events expected=31906 actual=31906 OK
Gold content quality hourly incremental check passed
```

Sau đó full reconciliation cũ giữa Parquet staging và ClickHouse cũng pass, còn
lần chạy normal không có fact mới:

```text
gold_fact_content_events_incremental_rows: 0
affected_window_count: 0
affected_rows_replaced: 0
state_updated: true
```

**Cách phát hiện**

Khi đọc logic transformation, `gold_content_quality_hourly` dùng:

```text
window_start = date_trunc('hour', event_time)
```

Trong khi incremental refresh state lại cần lọc fact rows mới theo:

```text
received_at >= refresh_from
received_at < refresh_to
```

Điều này tạo ra hai khái niệm thời gian khác nhau trong cùng một job.

**Nguyên nhân**

`received_at` phù hợp để làm watermark xử lý vì nó biểu diễn dữ liệu nào pipeline
mới nhận hoặc mới load vào Gold. Nhưng metric hourly phải phản ánh thời gian sự
kiện xảy ra theo `event_time`. Late data có thể được nhận hôm nay nhưng thuộc về
một event hour cũ, thậm chí khác ngày/năm.

Nếu incremental mart chỉ append giờ mới nhất theo processing/load time, dashboard
sẽ bỏ sót việc điều chỉnh metric của các window cũ.

**Cách xử lý hoặc quyết định**

Script `refresh_gold_content_quality_hourly_incremental.py` dùng hai bước rõ
ràng:

- Lọc fact rows incremental theo `received_at` để biết dữ liệu mới cần xử lý.
- Tính affected `window_start` theo `event_time`, rồi recompute toàn bộ metric
  cho các affected hours từ Gold modeled source of truth.

Khi load vào ClickHouse, script không `TRUNCATE` toàn bảng mà `DELETE` đúng
affected windows, chờ mutation hoàn tất, sau đó insert rows mới và reconcile đúng
phạm vi affected windows.

**Bài học phỏng vấn**

Trong streaming/lakehouse, cần phân biệt event time, ingestion time, processing
time và load time. Incremental serving mart thường dùng ingestion/load time để
xác định dữ liệu mới, nhưng dùng event time hoặc business key để xác định phần
metric cần replace. Đây là cách xử lý late data thực tế hơn append-only theo thời
gian chạy job.

**File hoặc bảng liên quan**

- `scripts/gold/refresh_gold_content_quality_hourly_incremental.py`
- `scripts/gold/check_content_quality_hourly_reconciliation.py`
- `src/bluesky_pipeline/gold_analytics_transformations.py`
- `lakehouse.gold_v1.gold_fact_content_events`
- `bluesky.gold_content_quality_hourly`

## Case 17: Full reconciliation không phù hợp khi Bronze/Silver đang streaming

**Hiện tượng**

Khi thử nghiệm chạy Gold incremental trong lúc Bronze/Silver streaming vẫn đang
ghi dữ liệu, pipeline dừng ở bước `scripts.lakehouse.check_iceberg_silver_v1`
với nhiều mismatch:

```text
silver_posts expected 10071 iceberg 8250 MISMATCH
silver_engagements expected 78726 iceberg 98447 MISMATCH
silver_follows expected 4291 iceberg 5904 MISMATCH
silver_deleted_records expected 2355 iceberg 3211 MISMATCH
```

Sau đó orchestrator nhận lỗi từ Gold refresh và dừng các process live còn lại.

**Cách phát hiện**

Log của `logs/live_pipeline/gold-incremental.log` có một số Spark heartbeat/NPE
message nhìn giống lỗi chính, nhưng Spark vẫn chạy tiếp các stage. Lỗi thật nằm
ở cuối log:

```text
Iceberg Silver v1 reconciliation failed
CalledProcessError: scripts.lakehouse.check_iceberg_silver_v1 returned non-zero
```

**Nguyên nhân**

`check_iceberg_silver_v1` là full reconciliation kiểu snapshot tĩnh: script đọc
Bronze để tính expected metrics, rồi đọc Silver Iceberg để so sánh. Nếu Bronze
writer và Silver streaming job vẫn đang ghi thêm dữ liệu, hai lần đọc này không
đảm bảo cùng một thời điểm snapshot. Vì vậy expected count và Iceberg count có
thể lệch dù pipeline không nhất thiết ghi sai dữ liệu.

Đây là khác biệt giữa validation cho batch/backfill ổn định và validation cho
live stream đang chuyển động.

**Cách xử lý hoặc quyết định**

Giữ `check_iceberg_silver_v1` cho các lần kiểm tra hữu hạn khi dữ liệu đã đứng
yên, ví dụ backfill, rebuild hoặc sau khi dừng ingestion.

Với demo local, chốt lại workflow hai phase: chạy
`scripts.e2e.run_live_pipeline` để ingest tới Silver, dừng live pipeline, rồi
chạy `scripts.lakehouse.run_lakehouse_path_incremental --live-mode` để refresh
Gold. `--live-mode` dùng `scripts.lakehouse.check_silver_iceberg_readiness`, chỉ
kiểm tra Silver tables tồn tại, đọc được và có dữ liệu tối thiểu trước khi chạy
Gold incremental.

**Bài học phỏng vấn**

Không phải mọi data quality check đều phù hợp với mọi chế độ vận hành. Full
reconciliation cần snapshot boundary rõ ràng; nếu source và target đang thay đổi
liên tục, count mismatch có thể là false failure. Trong live pipeline, nên dùng
readiness/freshness checks nhẹ để bảo vệ orchestration, còn reconciliation đầy
đủ nên chạy trên snapshot ổn định hoặc trong workflow backfill/recovery.

**File hoặc bảng liên quan**

- `scripts/e2e/run_live_pipeline.py`
- `scripts/lakehouse/run_lakehouse_path_incremental.py`
- `scripts/lakehouse/check_iceberg_silver_v1.py`
- `scripts/lakehouse/check_silver_iceberg_readiness.py`
- `logs/live_pipeline/gold-incremental.log`
- `lakehouse.silver_v1.*`

## Case 18: Timestamp literal thiếu timezone làm incremental window rỗng

**Hiện tượng**

Khi thử chạy Gold/cold path sau một live run, realtime hot path đã có dữ liệu
trong Grafana nhưng Gold/cold path vẫn không có dữ liệu. Bảng freshness của Gold
serving hiển thị
`row_count = 0` và `last_loaded_at = 1970-01-01`.

Log `gold-incremental.log` cho thấy Silver readiness đã pass và Silver Iceberg có
dữ liệu:

```text
silver_posts 12021 READABLE
silver_engagements 94012 READABLE
silver_follows 5565 READABLE
silver_deleted_records 3920 READABLE
```

Nhưng ngay sau đó Gold facts incremental lại lọc ra 0 row:

```text
silver_posts_incremental_rows: 0
silver_engagements_incremental_rows: 0
silver_follows_incremental_rows: 0
silver_deleted_records_incremental_rows: 0
```

**Cách phát hiện**

So sánh log của `check_silver_iceberg_readiness` với
`refresh_gold_facts_incremental` cho thấy dữ liệu đã có trong Silver, nhưng biến
mất tại bước filter theo `received_at`. `received_at` trong event envelope có
dạng ISO-8601 UTC kết thúc bằng `Z`, ví dụ `2026-07-12T16:00:00Z`, trong khi
`refresh_to` được format thành chuỗi không có timezone như
`2026-07-12 16:02:18.871463`.

**Nguyên nhân**

Spark parse timestamp có timezone và timestamp không timezone theo semantics khác
nhau. `received_at` có `Z` nên được hiểu là UTC rồi hiển thị/tính theo session
timezone. Literal `refresh_to` không có timezone nên được hiểu như giờ local của
Spark session.

Trong môi trường local Asia/Bangkok, dữ liệu nhận lúc `16:00Z` có thể được Spark
diễn giải thành khoảng `23:00` local, còn `refresh_to` không có `Z` bị hiểu là
`16:02` local. Điều kiện:

```text
received_at < refresh_to
```

vì vậy loại sạch dữ liệu vừa ingest.

**Cách xử lý hoặc quyết định**

Sửa các helper format timestamp cho incremental filters để literal refresh window
cũng dùng ISO-8601 UTC kết thúc bằng `Z`, cùng semantics với `received_at`.

Các filter `received_at` ở Gold facts, Gold dimensions và Gold serving mart
incremental đều phải dùng cùng quy ước này.

**Bài học phỏng vấn**

Trong Spark/lakehouse, timezone bug có thể không gây exception mà tạo ra kết quả
rỗng hoặc sai âm thầm. Khi incremental job trả 0 row bất thường, cần kiểm tra
song song:

- Source table có dữ liệu không.
- Refresh window đang dùng mốc nào.
- Cột timestamp và literal timestamp có cùng timezone semantics không.
- Session timezone của engine là gì.

Đây là ví dụ thực tế cho thấy watermark/incremental state là control-plane logic
nhưng vẫn phải tương thích chính xác với data-plane timestamp.

**File hoặc bảng liên quan**

- `scripts/gold/refresh_gold_facts_incremental.py`
- `scripts/gold/refresh_gold_dimensions_incremental.py`
- `scripts/gold/refresh_gold_content_quality_hourly_incremental.py`
- `scripts/gold/incremental_serving_utils.py`
- `src/bluesky_pipeline/event_envelope.py`
- `logs/live_pipeline/gold-incremental.log`
- `lakehouse.silver_v1.*`

## Case 19: Hive Metastore Derby volume mount sai path làm schema init lặp hoặc mất VERSION

**Hiện tượng**

Hive Metastore không start được. Ban đầu log báo schema initialization failed vì
object đã tồn tại:

```text
FUNCTION 'NUCLEUS_ASCII' already exists
Schema initialization failed!
```

Sau khi bật resume mode bằng `IS_RESUME=true`, container bỏ qua init schema nhưng
lại fail với lỗi:

```text
Version information not found in metastore.
Table/View 'VERSION' does not exist.
```

**Cách phát hiện**

Kiểm tra `docker compose logs hive-metastore` cho thấy image
`apache/hive:3.1.3` dùng Derby URL mặc định:

```text
jdbc:derby:;databaseName=metastore_db;create=true
```

Image chạy với working directory `/opt/hive`, nên Derby DB thật nằm ở
`/opt/hive/metastore_db`. Trong khi đó compose ban đầu mount volume vào
`/opt/hive/data`, khiến metastore DB không được persist đúng path.

**Nguyên nhân**

Có hai vấn đề chồng lên nhau:

- Derby embedded metastore DB dùng path relative `metastore_db` dưới
  `/opt/hive`.
- Docker volume lại được mount vào `/opt/hive/data`, không phải nơi Derby tạo DB.

Khi container bị recreate, metadata DB thật không còn. Nếu để Hive tự init lại,
schema có thể bị init dở dang và gặp object đã tồn tại. Nếu bật resume mode khi
DB chưa được init đầy đủ, Hive không tìm thấy bảng `VERSION`.

**Cách xử lý hoặc quyết định**

Cố định Derby connection URL bằng `config/hive/hive-site.xml`:

```text
jdbc:derby:/opt/hive/data/metastore_db;create=true
```

Sau đó mount named volume vào `/opt/hive/data`, chỉnh quyền volume cho user
`hive`, chạy `schematool -initSchema` đúng một lần, rồi start metastore với
`IS_RESUME=true`.

Không mount volume trực tiếp vào `/opt/hive/metastore_db` khi vẫn dùng Derby
database name `metastore_db`, vì Docker sẽ tạo sẵn thư mục đó còn Derby lại muốn
tự tạo thư mục database.

**Bài học phỏng vấn**

Embedded database trong container rất nhạy với working directory và mount path.
Khi dùng Hive Metastore với Derby local, cần phân biệt:

- Path chứa volume persist.
- Connection URL mà Derby thật sự dùng.
- Thời điểm chạy `schematool -initSchema`.
- Resume mode sau khi schema đã được init.

Trong production, Hive Metastore thường dùng database ngoài như PostgreSQL hoặc
MySQL thay vì embedded Derby để tránh các vấn đề về lock, persistence và
khả năng vận hành.

**File hoặc bảng liên quan**

- `docker-compose.yml`
- `config/hive/hive-site.xml`
- `config/hive/core-site.xml`
- `config/trino/catalog/lakehouse.properties`

## Case 20: WSL/Docker restart khi local Spark bị oversubscribe

**Hiện tượng**

Khi thử nghiệm runner gộp hot path và Gold path trên WSL local, VS Code/WSL bị
reload. Sau đó toàn bộ service trong Docker Compose đều dừng cùng lúc với trạng
thái:

```text
Exited (255)
```

Pipeline process không còn chạy. Các log trong `logs/live_pipeline/` bị dừng giữa
Spark stage, có file xuất hiện ký tự null ở cuối log thay vì stacktrace Python
rõ ràng.

Sau khi tăng tần suất Gold incremental, tình trạng này xuất hiện dễ hơn. Điều đó
cho thấy vấn đề chính là oversubscription tài nguyên local khi nhiều Spark app
và Docker services cùng chạy trên một máy cá nhân.

**Cách phát hiện**

`docker compose ps -a` cho thấy tất cả service chính như Kafka, MinIO, Hive
Metastore, Trino, ClickHouse và Grafana cùng exit tại gần như cùng một timestamp.
`docker inspect` cho thấy:

```text
exit=255
oom=false
finished=2026-07-13T10:59:08Z
```

Trong khi `uptime -s` của WSL là khoảng `2026-07-13 17:58:50` giờ local, gần
trùng với thời điểm container dừng. Điều này cho thấy WSL/Docker daemon bị
restart hoặc bị tắt đột ngột, không phải từng service tự fail độc lập.

**Nguyên nhân**

Đây là lỗi vận hành môi trường local: WSL hoặc Docker Desktop bị restart trong
lúc pipeline đang chạy. Các Spark streaming jobs, ingestion gateway và Gold
incremental process chạy trong terminal/WSL nên cũng bị kết thúc theo phiên WSL.

Nguyên nhân kỹ thuật phía project là oversubscription tài nguyên local. Runner
gộp từng thử nghiệm chạy đồng thời nhiều Spark application:

```text
bronze-writer Spark
realtime-metrics Spark
silver-stream Spark
Gold incremental Spark jobs
```

Trước khi tối ưu, helper Spark dùng `local[*]`, nghĩa là mỗi Spark application cố
dùng toàn bộ core WSL. Bronze writer cũng chưa giới hạn `maxOffsetsPerTrigger`,
nên khi Kafka backlog lớn, batch đầu có thể đọc hàng chục nghìn events. Log từng
ghi nhận:

```text
silver_stream_batch_id=0 input_rows=70307
ProcessingTimeExecutor: trigger interval is 60000 ms, but spent 137526 ms
```

Khi nhiều Spark JVM, Docker containers và VS Code cùng chạy trên WSL khoảng 8 GiB
RAM, Docker Desktop/WSL có thể bị restart thay vì chỉ một process Python fail.

**Cách xử lý hoặc quyết định**

Không coi đây là lỗi business logic của pipeline. Khi muốn chạy lại một lần quan
sát sạch, cần start lại Docker services, cleanup dữ liệu ingest dở dang bao gồm
Kafka topic, ClickHouse tables, checkpoints/state files và object storage paths,
rồi chạy lại demo theo hai phase độc lập.

Để giảm tải local, cấu hình Spark được chuyển sang các default bảo thủ hơn:

```text
SPARK_MASTER=local[2]
SPARK_SQL_SHUFFLE_PARTITIONS=8
SPARK_DEFAULT_PARALLELISM=8
SPARK_KAFKA_MAX_OFFSETS_PER_TRIGGER=5000
SPARK_LOCAL_IP=127.0.0.1
```

Bronze writer cũng được giới hạn `maxOffsetsPerTrigger` và đặt trigger 60 giây
để tránh chạy batch liên tục khi Kafka backlog lớn. Sau thay đổi này, baseline
live path không Gold đã chạy ổn trong môi trường local.

Quyết định cuối cùng cho demo cá nhân là loại bỏ runner gộp hot/cold path. Demo
chuẩn của repo là:

```text
scripts.e2e.run_live_pipeline
-> dừng live pipeline
-> scripts.lakehouse.run_lakehouse_path_incremental --live-mode
```

**Bài học phỏng vấn**

Trong local learning environment, failure mode của platform có thể khác production.
Exit code 255 đồng loạt trên nhiều container thường là dấu hiệu runtime bị dừng
đột ngột, không phải lỗi business logic. Khi debug pipeline, cần phân biệt:

- Lỗi ứng dụng: một job có stacktrace rõ và các service nền vẫn chạy.
- Lỗi platform/runtime: nhiều service cùng chết một thời điểm.
- Partial run: dữ liệu/checkpoint có thể đã ghi một phần, nên rerun sạch cần
  cleanup trước.
- Local Spark `local[*]` tiện cho học API nhưng nguy hiểm khi nhiều Spark app
  chạy đồng thời; production cần resource manager/cluster scheduler rõ ràng.
- Với project cá nhân trên WSL, workflow demo hai phase độc lập trung thực hơn
  việc cố chạy hot path và Gold path song song trên cùng máy.

**File hoặc bảng liên quan**

- `logs/live_pipeline/*.log`
- `docker-compose.yml`
- `scripts/e2e/run_live_pipeline.py`
- `scripts/lakehouse/run_lakehouse_path_incremental.py`
- `scripts/ingestion/spark_read_kafka_raw.py`
- `src/bluesky_pipeline/spark_session.py`
- `src/bluesky_pipeline/kafka_config.py`
- `scripts/platform/cleanup_ingested_data.py`
