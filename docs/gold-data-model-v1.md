# Gold data model v1

Tài liệu này mô tả thiết kế Gold modeled layer v1 cho lakehouse path của project.

Mục tiêu của Gold modeled layer là chuyển dữ liệu Silver đã sạch/chuẩn hóa thành
các bảng có ý nghĩa nghiệp vụ rõ ràng hơn, để phục vụ phân tích chuyên sâu và làm
nguồn ổn định cho Trino ad-hoc analytics và các bảng aggregate/serving trong
ClickHouse.

Gold modeled v1 chỉ áp dụng cho **lakehouse path**. Realtime fast path vẫn giữ
luồng riêng:

```text
Kafka -> Spark Structured Streaming -> ClickHouse realtime marts -> Grafana
```

## 1. Vị trí trong kiến trúc

Lakehouse path mục tiêu:

```text
Kafka
  -> Bronze Parquet
  -> Silver Iceberg
  -> Gold modeled Iceberg
      ├-> Trino ad-hoc SQL
      └-> Gold aggregate/serving marts
          -> ClickHouse
          -> Grafana
```

Trong đó:

- Bronze giữ raw event để audit, replay và reprocess.
- Silver giữ event-level datasets đã parse, type hóa và chuẩn hóa semantics.
- Gold modeled biến dữ liệu sạch thành mô hình phân tích như dimension, fact hoặc
  semantic marts và được query trực tiếp bằng Trino.
- Gold aggregate/serving tính trước các metric hay được dashboard query.
- ClickHouse là serving/metric store, không phải toàn bộ tầng Gold.

## 2. Nguyên tắc thiết kế Gold v1

- Gold modeled v1 đọc từ Silver Iceberg, không đọc trực tiếp từ Bronze.
- Gold modeled v1 lưu trên Iceberg để giữ lakehouse source có thể query, rebuild
  bằng Trino và dùng lại cho nhiều aggregate khác nhau.
- Gold aggregate/serving có thể lưu trung gian trên MinIO và load vào ClickHouse,
  nhưng phải rebuild được từ Gold modeled.
- Trino query Gold modeled Iceberg cho SQL/ad-hoc analytics; ClickHouse query
  Gold aggregate/serving marts cho dashboard latency thấp.
- Không đưa enrichment chưa có dữ liệu nguồn vào model, ví dụ handle, display
  name, follower count hoặc language detection.
- Không tuyên bố exactly-once; reconciliation vẫn là cơ chế kiểm chứng chính.
- Grain của mỗi bảng phải rõ ràng trước khi viết Spark transformation.

## 3. Namespace và naming

Namespace Iceberg đề xuất:

```text
lakehouse.gold_v1
```

Bảng Gold modeled v1:

- `gold_dim_actors`
- `gold_dim_posts`
- `gold_fact_content_events`
- `gold_fact_engagement_events`
- `gold_fact_network_events`

Bảng Gold aggregate/serving sẽ được thiết kế chi tiết trong:

- `docs/gold-analytics-metrics-v1.md`

Các bảng serving cũ dưới đây là phiên bản tối thiểu đã dùng để kiểm chứng
ClickHouse path, không phải bộ metric phân tích cuối cùng:

- `gold_event_volume_by_type`
- `gold_post_engagement_summary`

## 4. Bảng `gold_dim_actors`

**Grain**

Một dòng cho mỗi actor DID đã xuất hiện trong dữ liệu.

**Business key**

```text
actor_did
```

**Nguồn Silver**

- `silver_posts.author_did`
- `silver_engagements.actor_did`
- `silver_follows.actor_did`
- `silver_follows.target_actor_did`
- `silver_deleted_records.repository_did`

**Cột đề xuất**

```text
actor_did
first_seen_at
last_seen_at
first_ingest_date
last_ingest_date
source_event_count
```

**Ghi chú thiết kế**

- Bảng này chưa có handle/display name vì nguồn hiện tại chỉ có DID.
- `first_seen_at` và `last_seen_at` dùng để biết actor xuất hiện trong pipeline
  từ khi nào đến khi nào.
- Nếu sau này ingest identity/account events, bảng này có thể được enrich thêm
  handle, display name hoặc trạng thái account.

## 5. Bảng `gold_dim_posts`

**Grain**

Một dòng cho mỗi `post_uri`, đại diện cho trạng thái mới nhất mà lakehouse biết về
bài viết đó.

**Business key**

```text
post_uri
```

**Nguồn Silver**

- `silver_posts`
- `silver_deleted_records` với `collection = 'app.bsky.feed.post'`

**Cột đề xuất**

```text
post_uri
author_did
post_cid
post_text
post_created_at
text_length
is_reply
reply_root_uri
reply_parent_uri
is_deleted
deleted_at
first_seen_at
last_seen_at
ingest_date
ingest_hour
```

**Ghi chú thiết kế**

- Nếu một post có cả create và update, Gold v1 chọn record mới nhất theo
  `jetstream_time_us` hoặc `received_at`.
- `is_deleted` được suy ra từ `silver_deleted_records`.
- Bảng này là dimension vì nó mô tả entity bài viết, không phải từng action riêng
  lẻ.

## 6. Bảng `gold_fact_content_events`

**Grain**

Một dòng cho mỗi content event liên quan tới post lifecycle.

**Business key đề xuất**

```text
content_event_id
```

Trong v1, `content_event_id` có thể dựng deterministic từ:

```text
content_uri + content_event_type + jetstream_time_us
```

**Nguồn Silver**

- `silver_posts`
- `silver_deleted_records` với `collection = 'app.bsky.feed.post'`

**Cột đề xuất**

```text
content_event_id
content_uri
author_did
content_type
content_event_type
post_uri
is_reply
reply_root_uri
reply_parent_uri
event_time
received_at
jetstream_time_us
ingest_date
ingest_hour
```

**Giá trị chuẩn hóa**

`content_type`:

```text
post
```

`content_event_type`:

```text
post_create
post_update
post_delete
reply_create
reply_update
reply_delete
```

**Ghi chú thiết kế**

- Reply vẫn là một post trong Bluesky, nhưng được tách `content_event_type` để
  dashboard và analytics phân biệt được post gốc với reply.
- Delete event có thể thiếu text/cid, nên fact event không nên phụ thuộc vào các
  field đó.

## 7. Bảng `gold_fact_engagement_events`

**Grain**

Một dòng cho mỗi engagement event.

**Business key đề xuất**

```text
engagement_event_id
```

Với create event, `engagement_event_id` có thể dùng `engagement_uri`. Với delete
event trong tương lai, có thể dựng từ `record_uri + event_type + jetstream_time_us`.

**Nguồn Silver**

- `silver_engagements`
- `silver_deleted_records` với collection like/repost nếu cần model delete event

**Cột đề xuất**

```text
engagement_event_id
engagement_uri
actor_did
target_post_uri
target_cid
engagement_type
engagement_event_type
event_time
received_at
jetstream_time_us
ingest_date
ingest_hour
```

**Giá trị chuẩn hóa**

`engagement_type`:

```text
like
repost
```

`engagement_event_type`:

```text
like_create
like_delete
repost_create
repost_delete
```

**Ghi chú thiết kế**

- V1 ưu tiên create events vì `silver_engagements` hiện đã có đầy đủ
  `subject_uri` để join về `gold_dim_posts`.
- Delete like/repost từ `silver_deleted_records` có thể thiếu `subject_uri`, nên
  nếu cần model delete chính xác hơn thì phải lookup từ engagement history hoặc
  giữ thêm mapping ở Silver.

## 8. Bảng `gold_fact_network_events`

**Grain**

Một dòng cho mỗi network event như follow hoặc unfollow.

**Business key đề xuất**

```text
network_event_id
```

Với follow create, `network_event_id` có thể dùng `follow_uri`. Với unfollow,
id có thể dựng từ `record_uri + event_type + jetstream_time_us`.

**Nguồn Silver**

- `silver_follows`
- `silver_deleted_records` với `collection = 'app.bsky.graph.follow'`

**Cột đề xuất**

```text
network_event_id
follow_uri
actor_did
target_actor_did
network_event_type
event_time
received_at
jetstream_time_us
ingest_date
ingest_hour
```

**Giá trị chuẩn hóa**

`network_event_type`:

```text
follow_create
follow_delete
```

**Ghi chú thiết kế**

- Follow create có `target_actor_did` từ `silver_follows`.
- Follow delete có thể cần join ngược theo `follow_uri` để tìm lại
  `target_actor_did`.

## 9. Mapping sang analytics aggregate/serving

Gold modeled là nguồn để build các bảng analytics/serving giàu insight hơn, ví dụ:

- `gold_post_performance`
- `gold_content_quality_hourly`
- `gold_thread_conversation_summary`
- `gold_actor_activity_daily`
- `gold_network_growth_daily`

Chi tiết câu hỏi phân tích, grain, cột và checkpoint của các bảng này được mô tả
trong `docs/gold-analytics-metrics-v1.md`.

### Mapping legacy `gold_event_volume_by_type`

Nếu vẫn giữ bảng event volume tối thiểu để so sánh với path cũ, bảng này nên đọc
từ:

- `gold_fact_content_events`
- `gold_fact_engagement_events`
- `gold_fact_network_events`

Mapping v1:

```text
post_create / reply_create -> post hoặc reply
post_update / reply_update -> post_update hoặc reply_update
post_delete / reply_delete -> post_delete hoặc reply_delete
like_create -> like
repost_create -> repost
follow_create -> follow
follow_delete -> unfollow
```

### Mapping legacy `gold_post_engagement_summary`

Bảng này nên được thay bằng `gold_post_performance`. Nếu vẫn cần giữ checkpoint
tối thiểu, nó nên đọc từ:

- `gold_dim_posts`
- `gold_fact_engagement_events`

Grain của serving table:

```text
Một dòng cho mỗi post_uri
```

Metric:

```text
like_count
repost_count
engagement_count
```

## 10. Thứ tự triển khai đề xuất

1. Tạo contract metadata cho Gold Iceberg namespace và table names.
2. Viết transformation dùng chung để build Gold modeled từ Silver Iceberg.
3. Tạo script build `gold_dim_actors`, `gold_dim_posts`,
   `gold_fact_content_events`, `gold_fact_engagement_events` và
   `gold_fact_network_events`.
4. Cấu hình Trino query được namespace Gold Iceberg và chạy một vài SQL kiểm tra
   schema/row count.
5. Viết checkpoint kiểm tra row count, null key và một vài metric reconciliation
   giữa Silver và Gold modeled.
6. Refactor Gold aggregate scripts để đọc từ Gold modeled thay vì đọc trực tiếp
   từ Silver.
7. Giữ ClickHouse load/reconciliation như serving checkpoint cuối cùng.

## 11. Ngoài scope của Gold modeled v1

- Handle/display name enrichment.
- Identity/account lifecycle modeling.
- Hashtag, language, URL/domain extraction.
- Sentiment analysis hoặc topic modeling.
- Slowly changing dimension cho actor profile.
- Exactly-once end-to-end guarantee.
- ClickHouse materialized views.
