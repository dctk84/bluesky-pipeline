# Thiết kế Gold incremental refresh

Tài liệu này mô tả hướng tiến hóa lớp Gold từ full rebuild sang incremental
batch/micro-batch trong project `bluesky-pipeline`.

Mục tiêu của tài liệu là chốt contract thiết kế trước khi implement, để tránh
nhầm lẫn giữa streaming ingestion, incremental lakehouse refresh và realtime
serving.

Trạng thái hiện tại: contract này đã được triển khai cho Gold facts, Gold
dimensions và các Gold analytics serving marts v1. Entrypoint vận hành hiện tại
là `scripts/lakehouse/run_lakehouse_path_incremental.py`, gọi orchestrator
`scripts/gold/refresh/refresh_gold_incremental.py`. Tài liệu này vẫn giữ vai trò
giải thích thiết kế, trade-off và cách vận hành incremental Gold.

## 1. Bối cảnh

Bối cảnh ban đầu khi thiết kế incremental: lakehouse path đã chạy được theo hướng
full rebuild:

```text
Bronze -> Silver Iceberg
Silver Iceberg -> Gold modeled Iceberg
Gold modeled Iceberg -> Gold analytics staging
Gold analytics staging -> ClickHouse
Grafana -> ClickHouse
```

Cách này phù hợp với local MVP vì đơn giản, dễ kiểm chứng và dễ rebuild khi schema
còn thay đổi. Tuy nhiên khi dữ liệu lớn hơn, mỗi lần chạy lại toàn bộ Gold modeled
và load lại toàn bộ ClickHouse serving marts sẽ không tối ưu.

Hướng đã chốt là chuyển Gold sang incremental batch/micro-batch: xử lý thường
xuyên phần dữ liệu mới từ Silver và chỉ refresh các partition/entity bị ảnh
hưởng.

## 2. Mục tiêu

Chuyển Gold modeled và Gold analytics marts từ full rebuild sang incremental
refresh để:

- Giảm lượng dữ liệu phải scan và ghi lại mỗi lần chạy.
- Tránh để dữ liệu mới từ Silver tồn đọng quá lâu trước khi lên Gold.
- Giữ khả năng rebuild và reconciliation của lakehouse path.
- Phù hợp hơn với cách vận hành production-like nhưng vẫn đủ đơn giản cho môi
  trường học/local.

## 3. Nguyên tắc thiết kế

- Silver Iceberg vẫn là source of truth của lakehouse path.
- Gold modeled không nhất thiết phải continuous streaming hoàn toàn, nhưng không
  nên full rebuild toàn bộ trong vận hành thường xuyên.
- Gold incremental refresh chạy theo batch nhỏ dựa trên dữ liệu Silver mới và
  một lookback window.
- Fact tables ưu tiên append/merge theo deterministic event id.
- Dimension tables cần merge/update theo business key.
- Analytics marts chỉ refresh affected partitions hoặc affected entities.
- ClickHouse serving marts không nên `TRUNCATE` toàn bảng trong incremental path;
  cần replace phần dữ liệu bị ảnh hưởng.
- Mỗi bước incremental vẫn phải có checkpoint/reconciliation rõ ràng.

## 4. Luồng mục tiêu

```text
Bluesky Jetstream
        |
        v
Kafka raw topic
        |
        +--> Bronze Parquet streaming
        |         |
        |         v
        |   Silver Iceberg streaming
        |         |
        |         v
        |   Gold modeled incremental batch
        |         |
        |         v
        |   Gold analytics marts incremental batch
        |         |
        |         v
        |   ClickHouse incremental serving
        |         |
        |         v
        |   Grafana Gold dashboard
        |
        +--> Realtime fast path -> ClickHouse realtime marts -> Grafana realtime dashboard
```

Project vẫn là streaming lakehouse vì ingestion, Kafka buffering, Bronze/Silver
streaming và realtime fast path đều xử lý dữ liệu liên tục. Gold trong lakehouse
path được thiết kế là incremental batch để ưu tiên correctness, late data
handling, rebuildability và reconciliation.

## 5. Trigger và cadence

Gold incremental refresh có thể được trigger theo một trong các cách:

- Chạy định kỳ ngắn, ví dụ mỗi 5 phút, 15 phút hoặc 1 giờ.
- Chạy sau khi Silver streaming đã ghi xong một số micro-batch.
- Sau này orchestrate bằng Airflow cho các workflow có điểm bắt đầu và kết thúc
  rõ ràng.

Trong giai đoạn hiện tại, chưa cần dựng Airflow ngay. Có thể bắt đầu bằng một
script chạy tay để kiểm chứng logic incremental, sau đó mới đưa vào orchestration.

## 6. Watermark và lookback

Incremental job cần biết lần chạy thành công gần nhất để xác định vùng dữ liệu
cần xử lý.

Khái niệm chính:

- `last_successful_run_at`: thời điểm lần refresh trước hoàn thành thành công.
- `refresh_from`: mốc bắt đầu đọc lại dữ liệu.
- `refresh_to`: mốc kết thúc của lần refresh hiện tại.
- `lookback_window`: khoảng lùi lại để bắt late data hoặc dữ liệu đến muộn.

Ví dụ:

```text
refresh_from = last_successful_run_at - 2 hours
refresh_to = current_run_started_at
```

Lookback giúp tránh bỏ sót late data, nhưng cũng khiến một phần dữ liệu cũ được
xử lý lại. Vì vậy downstream phải idempotent hoặc có cơ chế replace/merge rõ ràng.

## 7. Chiến lược theo nhóm bảng

### 7.1. Gold fact tables

Bảng liên quan:

- `gold_fact_content_events`
- `gold_fact_engagement_events`
- `gold_fact_network_events`

Chiến lược:

- Đọc các dòng Silver mới trong khoảng `refresh_from` đến `refresh_to`.
- Build fact events bằng transformation dùng chung.
- Ghi vào Gold fact table bằng append/merge theo deterministic event id.
- Check key không null và không duplicate sau khi merge.

Các key chính:

- `gold_fact_content_events.content_event_id`
- `gold_fact_engagement_events.engagement_event_id`
- `gold_fact_network_events.network_event_id`

Fact event id phải đại diện cho từng event quan sát được, không chỉ business
entity. Ví dụ `network_event_id` của `follow_create` cần phân biệt được nhiều
event có cùng `follow_uri` nhưng khác `jetstream_time_us`.

### 7.2. Gold dimension tables

Bảng liên quan:

- `gold_dim_posts`
- `gold_dim_actors`

Chiến lược:

- Xác định các business key bị ảnh hưởng từ Silver mới và lookback.
- Recompute state cho các key đó từ Silver source of truth.
- Merge/update vào dimension table.

Các key chính:

- `gold_dim_posts.post_uri`
- `gold_dim_actors.actor_did`

Dimension table không chỉ append. Ví dụ post có thể update/delete, actor có thể
xuất hiện thêm event mới làm thay đổi `last_seen_at` hoặc `source_event_count`.

### 7.3. Gold analytics serving marts

Bảng liên quan:

- `gold_post_performance`
- `gold_content_quality_hourly`
- `gold_thread_conversation_summary`
- `gold_actor_activity_daily`
- `gold_network_growth_daily`

Chiến lược:

- Xác định affected windows hoặc affected entities.
- Rebuild lại phần mart bị ảnh hưởng từ Gold modeled.
- Ghi staging incremental.
- Replace phần tương ứng trong ClickHouse thay vì truncate/load toàn bảng.
- Reconcile chỉ phần bị ảnh hưởng.

Ví dụ:

- `gold_content_quality_hourly`: affected window theo `window_start`.
- `gold_actor_activity_daily`: affected partition theo `activity_date` và
  `actor_did`.
- `gold_network_growth_daily`: affected partition theo `activity_date` và
  `target_actor_did`.
- `gold_thread_conversation_summary`: affected entity theo `reply_root_uri`.
- `gold_post_performance`: affected entity theo `post_uri`.

## 8. ClickHouse incremental serving

Hiện tại các load script thường truncate toàn bảng rồi load lại dữ liệu mới. Đây
là cách đơn giản cho local MVP, nhưng không tối ưu khi dữ liệu lớn.

Hướng incremental:

- Với mart theo ngày/giờ: xóa hoặc replace affected partition/range rồi insert
  lại phần đã rebuild.
- Với mart theo entity: xóa hoặc replace affected keys rồi insert lại version mới.
- Giữ schema/table contract ổn định để Grafana không bị mất dashboard query.

Một số lựa chọn kỹ thuật có thể cân nhắc sau:

- `ALTER TABLE ... DELETE WHERE ...` cho affected range/key trong môi trường
  local nhỏ.
- Bảng engine hỗ trợ replace/version như `ReplacingMergeTree`.
- Partition theo ngày/giờ để replace partition hiệu quả hơn.
- Materialized view nếu logic aggregation phù hợp và không cần correction phức
  tạp.

Trong project hiện tại, bước đầu nên chọn phương án đơn giản, dễ kiểm chứng:
delete affected range/key rồi insert lại dữ liệu tương ứng.

## 9. Reconciliation incremental

Reconciliation không nên chỉ check toàn bảng sau khi dữ liệu lớn dần. Cần bổ sung
checkpoint theo phạm vi incremental:

- Check row count trong affected window/entity.
- Check tổng các metric quan trọng trong affected window/entity.
- Check key uniqueness của Gold facts.
- Check null key ở dimension và fact.
- Check ClickHouse serving khớp với Gold staging hoặc Gold modeled trong phạm vi
  vừa refresh.

Full reconciliation vẫn hữu ích nhưng nên dùng như checkpoint định kỳ hoặc trước
demo lớn, không nhất thiết chạy sau mọi micro-batch.

## 10. Lộ trình triển khai đề xuất

### Giai đoạn 1: Chốt incremental contract

- Tạo tài liệu thiết kế này.
- Xác định key, grain và affected scope cho từng nhóm bảng.
- Giữ full rebuild path hiện tại làm baseline an toàn.

### Giai đoạn 2: Implement incremental cho Gold fact tables

Làm trước nhóm fact vì dễ hơn dimension và mart:

```text
Silver new rows -> Gold fact events -> merge/append by event id -> key check
```

Bảng mục tiêu:

- `gold_fact_content_events`
- `gold_fact_engagement_events`
- `gold_fact_network_events`

### Giai đoạn 3: Implement incremental cho dimensions

Recompute và merge state cho affected business keys:

- `post_uri`
- `actor_did`

### Giai đoạn 4: Implement incremental cho analytics marts

Bắt đầu với mart có grain thời gian rõ nhất:

- `gold_content_quality_hourly`

Sau đó nhân pattern sang:

- `gold_actor_activity_daily`
- `gold_network_growth_daily`
- `gold_post_performance`
- `gold_thread_conversation_summary`

### Giai đoạn 5: Incremental ClickHouse load

Thay truncate/load full bằng replace affected scope:

```text
DELETE affected range/key -> INSERT rebuilt rows
```

Sau khi pattern ổn định mới cân nhắc table engine hoặc partition strategy phức
tạp hơn.

## 11. Cách tóm tắt thiết kế

Cách mô tả ngắn gọn:

```text
Project là streaming lakehouse với hybrid serving architecture. Dữ liệu được
ingest liên tục qua Kafka và Spark Structured Streaming, Bronze/Silver cập nhật
liên tục, realtime fast path phục vụ dashboard latency thấp. Gold lakehouse path
không full streaming toàn bộ mà dùng incremental batch/micro-batch để xử lý
dedup, late data, update/delete, join và reconciliation một cách kiểm soát hơn.
```

Điểm cần nhấn mạnh:

- Streaming project không có nghĩa mọi layer đều phải streaming.
- Gold analytical layer thường cần consistency, correction và rebuildability.
- Incremental batch là hướng production-like hơn full rebuild khi dữ liệu lớn.
- Realtime fast path và lakehouse Gold path phục vụ hai mục tiêu khác nhau:
  latency thấp và correctness/rebuildability.
